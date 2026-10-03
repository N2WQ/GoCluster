package peerdiag

import (
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Source-derived buckets deliberately exceed individual allocator size
// classes. Reflection checks the pinned concrete wrapper types, not sampled
// heap/RSS, and fails on a toolchain layout change that invalidates the table.
func TestV15DiagnosticStdlibObjectSizes(t *testing.T) {
	netFD, ok := reflect.TypeFor[net.TCPConn]().Field(0).Type.FieldByName("fd")
	if !ok {
		t.Fatal("pinned net.conn layout changed")
	}
	file := reflect.TypeFor[os.File]().Field(0).Type.Elem()
	info, err := os.Stat(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for name, typ := range map[string]reflect.Type{
		"netFD": netFD.Type.Elem(), "file": file, "fileStat": reflect.TypeOf(info).Elem(),
		"TCPListener": reflect.TypeFor[net.TCPListener](), "TCPAddr": reflect.TypeFor[net.TCPAddr](),
		"OpError": reflect.TypeFor[net.OpError](), "PathError": reflect.TypeFor[os.PathError](), "Timer": reflect.TypeFor[time.Timer](),
	} {
		if typ.Size() > 512 {
			t.Fatalf("%s=%d exceeds wrapper bound", name, typ.Size())
		}
		t.Logf("%s=%d <=512", name, typ.Size())
	}
}

func TestV15HelperNeverResolvesAnAddress(t *testing.T) {
	for _, address := range []string{"example.invalid:1", "127.0.0.2:1", "[::1]:1", "127.0.0.1:0"} {
		if err := RunHelper(address, strings.Repeat("a", 64)); err == nil {
			t.Fatal("non-parent endpoint accepted")
		}
	}
}
