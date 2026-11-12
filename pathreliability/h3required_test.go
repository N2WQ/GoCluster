package pathreliability

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/internal/testsupport"
)

type requiredTableReader struct {
	io.Reader
	read, closes      int
	readErr, closeErr error
}

func (r *requiredTableReader) Read(p []byte) (int, error) {
	if r.readErr != nil {
		return 0, r.readErr
	}
	n, err := r.Reader.Read(p)
	r.read += n
	return n, err
}
func (r *requiredTableReader) Close() error { r.closes++; return r.closeErr }

func TestRequiredH3TableReader(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		extra                      int
		readErr, closeErr, openErr bool
	}{
		{"valid", 0, false, false, false}, {"res1_truncated", -1, false, false, false},
		{"res2_wrong_count", -8, false, false, false}, {"oversized", 1 << 20, false, false, false},
		{"growing", 2 << 20, false, false, false}, {"read_error", 0, true, false, false},
		{"close_error", 0, false, true, false}, {"open_error", 0, false, false, true},
		{"read_and_close_errors", 0, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			readers := make([]*requiredTableReader, 0, 2)
			opens := 0
			_, _, err := loadRequiredH3Tables("configured", func(path string) (io.ReadCloser, error) {
				opens++
				if tc.openErr {
					return nil, errors.New("open fault")
				}
				count := h3Res1Count
				if strings.HasSuffix(path, "res2.bin") {
					count = h3Res2Count
				}
				r := &requiredTableReader{Reader: io.LimitReader(&zeroTableStream{}, int64(count*8+tc.extra))}
				if tc.name == "growing" {
					r.Reader = &zeroTableStream{}
				}
				if tc.readErr {
					r.readErr = errors.New("read fault")
				}
				if tc.closeErr {
					r.closeErr = errors.New("close fault")
				}
				readers = append(readers, r)
				return r, nil
			})
			if (err != nil) != (tc.name != "valid") {
				t.Fatalf("outcome: %v", err)
			}
			if opens != 2 {
				t.Fatalf("opened %d tables, want both", opens)
			}
			if err != nil && (!strings.Contains(err.Error(), "res1.bin") || !strings.Contains(err.Error(), "res2.bin")) {
				t.Fatalf("errors not aggregated: %v", err)
			}
			if (tc.name == "oversized" || tc.name == "growing") && !strings.Contains(err.Error(), "exceeds") {
				t.Fatalf("overflow diagnostic misrepresented captured size: %v", err)
			}
			if tc.name == "read_and_close_errors" && (!strings.Contains(err.Error(), "read fault") || !strings.Contains(err.Error(), "close fault")) {
				t.Fatalf("lost one handle error: %v", err)
			}
			for i, r := range readers {
				count := h3Res1Count
				if i == 1 {
					count = h3Res2Count
				}
				if r.read > count*8+1 || r.closes != 1 {
					t.Fatalf("read=%d closes=%d limit=%d", r.read, r.closes, count*8+1)
				}
				if (tc.name == "growing" || tc.name == "oversized") && r.read != count*8+1 {
					t.Fatalf("did not exercise overflow byte: %d", r.read)
				}
			}
		})
	}
}

// A reader with no finite size models growth after open; only the bounded
// wrapper, not file metadata or the reader itself, limits consumption.
type zeroTableStream struct{}

func (*zeroTableStream) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func TestRequiredH3Files(t *testing.T) {
	for _, mode := range []string{"missing_res1", "missing_res2", "directory", "oversized", "truncated", "wrong_count", "empty_directory"} {
		t.Run(mode, func(t *testing.T) {
			dir := requiredH3PrivateTables(t)
			path := h3TablePath(dir, coarseResolution)
			switch mode {
			case "missing_res1", "directory":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if mode == "directory" {
					if err := os.Mkdir(path, 0700); err != nil {
						t.Fatal(err)
					}
				}
			case "missing_res2":
				if err := os.Remove(h3TablePath(dir, fineResolution)); err != nil {
					t.Fatal(err)
				}
			case "oversized", "truncated", "wrong_count":
				size := int64(1 << 30)
				if mode == "truncated" {
					size = h3Res1Count*8 - 1
				}
				if mode == "wrong_count" {
					size = h3Res1Count*8 - 8
				}
				if err := os.Truncate(path, size); err != nil {
					t.Fatal(err)
				}
			case "empty_directory":
				dir = " "
			}
			_, _, err := loadRequiredH3Tables(dir, func(path string) (io.ReadCloser, error) { return os.Open(path) })
			if err == nil {
				t.Fatal("invalid required file accepted")
			}
			// Windows removal refuses open handles. This also checks failure
			// cleanup for real files rather than only a fake Close counter.
			if mode == "oversized" || mode == "truncated" || mode == "wrong_count" {
				if err := os.Remove(path); err != nil {
					t.Fatalf("failed read retained handle: %v", err)
				}
			}
		})
	}
}

func TestRequiredH3CapturedTablesSurviveRemoval(t *testing.T) {
	const env, marker = "GOCLUSTER_REQUIRED_H3_CAPTURE_CHILD", "REQUIRED_H3_CAPTURE_CHILD_EXECUTED"
	if os.Getenv(env) != "1" {
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestRequiredH3CapturedTablesSurviveRemoval$", "-test.v")
		cmd.Env = append(os.Environ(), env+"=1")
		out, err := cmd.CombinedOutput()
		if err != nil || !bytes.Contains(out, []byte(marker)) {
			t.Fatalf("capture child: %v\n%s", err, out)
		}
		return
	}
	if EncodeCell("FN31") != InvalidCell || EncodeCoarseCell("FN31") != InvalidCell {
		t.Fatal("capture child prewarmed")
	}
	dir := requiredH3PrivateTables(t)
	reads := 0
	err := initRequiredH3Mappings(dir, func(path string) (io.ReadCloser, error) {
		reads++
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	})
	if err != nil || reads != 2 {
		t.Fatalf("captured read: reads=%d err=%v", reads, err)
	}
	if EncodeCell("FN31") == InvalidCell || EncodeCoarseCell("FN31") == InvalidCell {
		t.Fatal("actual initializer did not publish captured cells")
	}
	for _, res := range []int{fineResolution, coarseResolution} {
		if _, err := os.Stat(h3TablePath(dir, res)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("input recreated: %v", err)
		}
	}
	t.Log(marker)
}

func requiredH3PrivateTables(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"res1.bin", "res2.bin"} {
		data, err := os.ReadFile(filepath.Join(testsupport.H3TableDir(t), name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestRequiredH3Initialization(t *testing.T) {
	const env, marker = "GOCLUSTER_REQUIRED_H3_CHILD", "REQUIRED_H3_CHILD_EXECUTED"
	if mode := os.Getenv(env); mode != "" {
		if EncodeCell("FN31") != InvalidCell || EncodeCoarseCell("FN31") != InvalidCell {
			t.Fatal("child prewarmed")
		}
		dir := requiredH3PrivateTables(t)
		missing := t.TempDir()
		switch mode {
		case "invalid_then_valid":
			if err := InitRequiredH3Mappings(missing); err == nil || !strings.Contains(err.Error(), "res1.bin") || !strings.Contains(err.Error(), "res2.bin") {
				t.Fatalf("missing tables: %v", err)
			}
			if EncodeCell("FN31") != InvalidCell || EncodeCoarseCell("FN31") != InvalidCell {
				t.Fatal("partial maps published")
			}
			if err := InitRequiredH3Mappings(dir); err != nil {
				t.Fatal(err)
			}
		case "valid_then_invalid":
			if err := InitRequiredH3Mappings(dir); err != nil {
				t.Fatal(err)
			}
			if err := InitRequiredH3Mappings(missing); err == nil {
				t.Fatal("warm maps hid invalid configured directory")
			}
		case "second_valid_directory":
			if err := InitRequiredH3Mappings(dir); err != nil {
				t.Fatal(err)
			}
			beforeFine, beforeCoarse := EncodeCell("FN31"), EncodeCoarseCell("FN31")
			other := requiredH3PrivateTables(t)
			for _, table := range []struct {
				res  int
				cell CellID
			}{{fineResolution, beforeFine}, {coarseResolution, beforeCoarse}} {
				data, err := os.ReadFile(h3TablePath(other, table.res))
				if err != nil {
					t.Fatal(err)
				}
				offset := (int(table.cell) - 1) * 8
				clear(data[offset : offset+8])
				if err := os.WriteFile(h3TablePath(other, table.res), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := InitRequiredH3Mappings(other); err != nil {
				t.Fatal(err)
			}
			if EncodeCell("FN31") != beforeFine || EncodeCoarseCell("FN31") != beforeCoarse {
				t.Fatal("second valid directory replaced once-owned maps")
			}
		case "legacy_failure_sticky":
			// A malformed present file fails both backends without allowing the
			// legacy cgo generator to manufacture a missing table.
			if err := os.WriteFile(filepath.Join(missing, "res2.bin"), []byte{1}, 0600); err != nil {
				t.Fatal(err)
			}
			first := InitH3MappingsFromDir(missing)
			if first == nil {
				t.Fatal("legacy failure control did not fail")
			}
			if err := InitRequiredH3Mappings(dir); !errors.Is(err, first) {
				t.Fatalf("lost sticky legacy error: %v / %v", first, err)
			}
		default:
			t.Fatal("unknown child")
		}
		if mode != "legacy_failure_sticky" && (EncodeCell("FN31") == InvalidCell || EncodeCoarseCell("FN31") == InvalidCell) {
			t.Fatal("valid tables unusable")
		}
		entries, err := os.ReadDir(missing)
		if err != nil {
			t.Fatal(err)
		}
		want := 0
		if mode == "legacy_failure_sticky" {
			want = 1
		}
		if len(entries) != want {
			t.Fatal("required initializer generated or rewrote tables")
		}
		t.Log(marker + " " + mode)
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"invalid_then_valid", "valid_then_invalid", "second_valid_directory", "legacy_failure_sticky"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestRequiredH3Initialization$", "-test.v")
			cmd.Env = append(os.Environ(), env+"="+mode)
			out, err := cmd.CombinedOutput()
			if err != nil || !bytes.Contains(out, []byte(marker+" "+mode)) {
				t.Fatalf("child: %v\n%s", err, out)
			}
		})
	}
}
