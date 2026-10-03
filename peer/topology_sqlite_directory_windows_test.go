package peer

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

func TestV15TopologyDirectoryNativeAdmission(t *testing.T) {
	real := topologyDirectoryFullPath
	defer func() { topologyDirectoryFullPath = real }()
	for _, tc := range []struct {
		name, value  string
		returned     uint32
		err          error
		unterminated bool
		budget       bool
	}{
		{name: "ordinary", value: `C:\kept`, returned: 7},
		{name: "1024-ascii", value: `C:\` + strings.Repeat("a", 1021), returned: 1024},
		{name: "1024-wtf8", value: `C:\` + strings.Repeat("é", 510) + "a", returned: 514},
		{name: "surrogate-pair", value: `C:\😀`, returned: 5},
		{name: "unpaired-surrogate", value: `C:\` + string([]byte{0xed, 0xa0, 0x80}), returned: 4},
		{name: "native-growth", returned: 1025, budget: true},
		{name: "native-overflow", returned: ^uint32(0), budget: true},
		{name: "zero", budget: true},
		{name: "over-budget-wtf8", value: `C:\` + strings.Repeat("é", 511), returned: 514, budget: true},
		{name: "relative-reply", value: "relative", returned: 8, budget: true},
		{name: "interior-null", value: `C:\`, returned: 6, budget: true},
		{name: "unterminated", value: `C:\kept`, returned: 7, unterminated: true, budget: true},
		{name: "native-error", err: syscall.ERROR_ACCESS_DENIED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			topologyDirectoryFullPath = func(_ *uint16, size uint32, output *uint16, _ **uint16) (uint32, error) {
				calls++
				if size != 1025 {
					t.Fatal("native size escaped fixed workspace", size)
				}
				units, err := syscall.UTF16FromString(tc.value)
				if err != nil {
					t.Fatal(err)
				}
				buffer := unsafe.Slice(output, int(size))
				copy(buffer, units)
				if tc.unterminated {
					buffer[tc.returned] = 'x'
				}
				return tc.returned, tc.err
			}
			got, err := topologyDirectoryAbsolute("relative")
			if calls != 1 || errors.Is(err, errTopologyBudget) != tc.budget || (tc.err != nil && !errors.Is(err, tc.err)) {
				t.Fatal("native failure/admission changed", got, err, calls)
			}
			if !tc.budget && tc.err == nil && (err != nil || got != tc.value) {
				t.Fatalf("native spelling changed: got=%q/%v want=%q", got, err, tc.value)
			}
		})
	}
	topologyDirectoryFullPath = func(_ *uint16, _ uint32, _ *uint16, _ **uint16) (uint32, error) {
		t.Fatal("unadmitted input reached native acquisition")
		return 0, nil
	}
	if _, err := topologyDirectoryAbsolute(strings.Repeat("a", 1025)); !errors.Is(err, errTopologyBudget) {
		t.Fatal("input byte budget ignored", err)
	}
	if _, err := topologyDirectoryAbsolute("interior\x00null"); !errors.Is(err, syscall.EINVAL) {
		t.Fatal("NUL input changed", err)
	}
}

func TestV15TopologyDirectorySpelling(t *testing.T) {
	real, longNames := topologyDirectoryFullPath, topologyDirectoryLongPaths
	defer func() { topologyDirectoryFullPath, topologyDirectoryLongPaths = real, longNames }()
	topologyDirectoryLongPaths = true
	root := t.TempDir()
	t.Chdir(root)
	for _, path := range []string{".", "relative", `relative\..\kept. `, `\kept`, filepath.VolumeName(root) + `:invalid`, filepath.VolumeName(root) + `relative`, root, root + `\literal. `, `\\?\` + root + `\literal. `, `\\.\` + root + `\literal. `} {
		want := path
		var wantErr error
		if !filepath.IsAbs(path) {
			want, wantErr = syscall.FullPath(path)
		}
		got, err := topologyDirectoryPath(path)
		if (err == nil) != (wantErr == nil) || err == nil && got != want {
			t.Fatalf("modern directory spelling: input=%q got=%q/%v want=%q/%v", path, got, err, want, wantErr)
		}
	}
	malformed := string([]byte{'r', 0xff, 0xed, 0xa0, 0x80})
	expected, err := syscall.UTF16FromString(malformed)
	if err != nil {
		t.Fatal(err)
	}
	topologyDirectoryFullPath = func(input *uint16, size uint32, output *uint16, _ **uint16) (uint32, error) {
		if actual := unsafe.Slice(input, len(expected)); !reflect.DeepEqual(actual, expected) {
			t.Fatal("input no longer uses pinned syscall UTF-16/WTF-8 conversion", actual, expected)
		}
		copy(unsafe.Slice(output, int(size)), []uint16{'C', ':', '\\', 'x', 0})
		return 4, nil
	}
	if _, err := topologyDirectoryPath(malformed); err != nil {
		t.Fatal(err)
	}
	topologyDirectoryFullPath = real
	topologyDirectoryLongPaths = false
	long := `C:\` + strings.Repeat(`segment\`, 40) + "kept"
	unc := `\\server\share\` + strings.Repeat(`segment\`, 40) + "kept"
	device := `\\.\` + long
	for _, tc := range []struct{ input, want string }{
		{`C:\short`, `C:\short`}, {long, `\\?\` + long}, {unc, `\\?\UNC\` + unc[2:]},
		{device, device}, {`\\?\C:\literal. `, `\\?\C:\literal. `}, {`\??\C:\literal. `, `\??\C:\literal. `},
	} {
		got, err := topologyDirectoryPath(tc.input)
		if err != nil || got != tc.want {
			t.Fatalf("legacy directory spelling: input=%q got=%q/%v want=%q", tc.input, got, err, tc.want)
		}
	}
	for _, length := range []int{1020, 1021} {
		path := `C:\` + strings.Repeat("a", length-3)
		name, err := topologyDirectoryPath(path)
		if errors.Is(err, errTopologyBudget) != (length == 1021) || length == 1020 && (err != nil || len(name) != 1024) {
			t.Fatal("legacy prefix escaped existing filename budget", length, len(name), err)
		}
	}
}

func TestV15TopologyDirectoryRefusalPreservesState(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	store, err := openTopologyStore(filepath.Join(root, "saved.db"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	observer := topologyTestDB(t, store)
	if _, err = observer.ExecContext(t.Context(), "create table kept(v);insert into kept values('committed')"); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	real := topologyDirectoryFullPath
	defer func() { topologyDirectoryFullPath = real }()
	var seen string
	topologyDirectoryFullPath = func(input *uint16, _ uint32, _ *uint16, _ **uint16) (uint32, error) {
		var units [1025]uint16
		for i := range units {
			units[i] = *(*uint16)(unsafe.Add(unsafe.Pointer(input), 2*i))
			if units[i] == 0 {
				seen = syscall.UTF16ToString(units[:i])
				return 1025, nil
			}
		}
		t.Fatal("native input missing admitted terminator")
		return 0, nil
	}
	for _, dsn := range []string{`must-not-create\nested\new.db`, `must-not-create\new.db?ignored=tail\more`} {
		candidate, err := openTopologyStore(dsn, time.Hour)
		if candidate != nil || !errors.Is(err, errTopologyBudget) || seen != filepath.Dir(dsn) {
			t.Fatal("directory interpretation/refusal changed", dsn, candidate, err, seen)
		}
		if _, err := os.Stat("must-not-create"); !errors.Is(err, os.ErrNotExist) || topologyReservation.Load() != nil {
			t.Fatal("directory refusal created data or retained owner", err, topologyReservation.Load())
		}
	}
	// The existing dot-directory skip must not invoke native preparation.
	seen = ""
	store, err = openTopologyStore("saved.db", time.Hour)
	if err != nil || seen != "" {
		t.Fatal("dot directory no longer bypasses mkdir preparation", err, seen)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	topologyDirectoryFullPath = real
	store, err = openTopologyStore(`ordinary\nested\new.db`, time.Hour)
	if err != nil {
		t.Fatal("normal relative directory could not recover", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	var value string
	if err := observer.QueryRowContext(t.Context(), "select v from kept").Scan(&value); err != nil || value != "committed" {
		t.Fatal("constructor refusal changed saved value", value, err)
	}
	if err := observer.QueryRowContext(t.Context(), "pragma integrity_check").Scan(&value); err != nil || value != "ok" {
		t.Fatal("constructor refusal changed database integrity", value, err)
	}
}
