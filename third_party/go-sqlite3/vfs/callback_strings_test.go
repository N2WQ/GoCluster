package vfs

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ncruces/go-sqlite3/internal/errutil"
	"github.com/ncruces/go-sqlite3/internal/sqlite3_wrap"
	"github.com/ncruces/go-sqlite3/internal/util"
)

func TestV15SQLiteCallbackBooleanParity(t *testing.T) {
	values := []string{"", "0", "0suffix", "9suffix", "true", "TRUE", "false", "FALSE", "on", "off", "yes", "no", " true", "ſ", "\xff", "\xed\xa0\x80", strings.Repeat("x", 20), strings.Repeat("x", 21), "1" + strings.Repeat("x", 600000), strings.Repeat("x", 600000)}
	for _, value := range values {
		want, wantOK := util.ParseBool(value)
		got, gotOK := util.ParseBool(callbackBoolString([]byte(value)))
		if got != want || gotOK != wantOK {
			t.Fatalf("length%d: got%v/%v want%v/%v", len(value), got, gotOK, want, wantOK)
		}
		for _, name := range []string{"checksum_verification", "page_size", "CHECKSUM_VERIFICATION", "checKsum_verification", "unknown"} {
			for _, active := range []bool{false, true} {
				left, right := &cksmFile{computeCksm: active, verifyCksm: active}, &cksmFile{computeCksm: active, verifyCksm: active}
				want, wantErr := left.Pragma(strings.ToLower(name), value)
				memory := &sqlite3_wrap.Memory{Buf: make([]byte, len(name)+len(value)+64)}
				memory.WriteString(32, name)
				valuePtr := ptr_t(33 + len(name))
				memory.WriteString(valuePtr, value)
				memory.Write32(8+ptrlen, 32)
				memory.Write32(8+2*ptrlen, uint32(valuePtr))
				// Test the real callback dispatch on NOTFOUND cases. Successful
				// result allocation requires a real engine, covered below/root.
				if wantErr == _NOTFOUND {
					wrapper := &sqlite3_wrap.Wrapper{Memory: memory}
					if got := vfsFileControlImpl(wrapper, right, _FCNTL_PRAGMA, 8); got != _NOTFOUND {
						t.Fatal("unknown checksum pragma changed", got)
					}
				} else {
					mapped := callbackBoolString([]byte(value))
					if strings.ToLower(name) == "page_size" && value != "" {
						mapped = "1"
					}
					got, gotErr := right.Pragma(strings.ToLower(name), mapped)
					if got != want || gotErr != wantErr || !reflect.DeepEqual(left, right) {
						t.Fatal("checksum value observation changed", name, len(value), got, want)
					}
				}
			}
		}
	}
}

func TestV15SQLiteCallbackParameterBorrowing(t *testing.T) {
	large := strings.Repeat("x", 600000)
	encoded := "\x00" + large + "\x00ignored\x00other\x00" + large + "\x00modeof\x00kept\x00psow\x001suffix\x00modeof\x00second\x00\x00"
	mem := &sqlite3_wrap.Memory{Buf: []byte(encoded)}
	for key, want := range map[string]string{"modeof": "kept", "psow": "1suffix", "absent": ""} {
		if got := string(callbackParameter(mem, 1, key)); got != want {
			t.Fatal("parameter order/value changed", key, got)
		}
		if n := testing.AllocsPerRun(5, func() { _ = callbackParameter(mem, 1, key) }); n != 0 {
			t.Fatal("parameter scan copied engine input", key, n)
		}
	}
	span := callbackParameter(mem, 1, "modeof")
	span[0] = 'K' // fixture only: demonstrate exact backing, then restore it.
	if got := string(callbackParameter(mem, 1, "modeof")); got != "Kept" {
		t.Fatal("result was not borrowed")
	}
	span[0] = 'k'
}

func TestV15SQLiteCallbackReadStringChecks(t *testing.T) {
	for _, tc := range []struct {
		ptr   ptr_t
		limit int64
		want  any
	}{{0, 10, errutil.NilErr}, {1, 2, errutil.NoNulErr}} {
		func() {
			defer func() {
				if got := recover(); got != tc.want {
					t.Fatal("pointer/NUL semantics changed", got, tc.want)
				}
			}()
			_ = callbackString(&sqlite3_wrap.Memory{Buf: []byte("\x00long\x00")}, tc.ptr, tc.limit)
		}()
	}
}
