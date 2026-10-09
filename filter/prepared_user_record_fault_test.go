package filter

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestPreparedRecordCommitAtomicFailures(t *testing.T) {
	usePresetTestDir(t)
	path := userRecordPath("N2WQ-1")
	original := []byte("original bytes\n")
	if err := os.WriteFile(path, original, 0o644); err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareUserRecord("N2WQ-1", &UserRecord{NoiseClass: "URBAN"})
	if err != nil {
		t.Fatal(err)
	}
	expected := bytes.Clone(prepared.data)
	for _, stage := range []string{"write", "sync", "close", "replace"} {
		t.Run(stage, func(t *testing.T) {
			failure := errors.New("commit stage failure")
			called := make([]string, 0, 4)
			stages := atomicUserFileStages{
				write: func(file *os.File, data []byte) error {
					called = append(called, "write")
					if !bytes.Equal(data, expected) {
						t.Fatal("writer did not receive exact prepared bytes")
					}
					if stage == "write" {
						if err := writeUserFileBytes(file, []byte("partial")); err != nil {
							return err
						}
						return failure
					}
					return writeUserFileBytes(file, data)
				},
				sync: func(file *os.File) error {
					called = append(called, "sync")
					if stage == "sync" {
						return failure
					}
					return file.Sync()
				},
				close: func(file *os.File) error {
					called = append(called, "close")
					if stage == "close" {
						return failure
					}
					return file.Close()
				},
				replace: func(_, _ string) error {
					called = append(called, "replace")
					return failure
				},
			}
			err := prepared.commitWith(func(target string, data []byte) error {
				if target != path {
					t.Fatal("atomic commit used a different path")
				}
				return writeAtomicUserFileWithStages(target, data, stages)
			})
			if !errors.Is(err, failure) {
				t.Fatalf("underlying %s error lost: %v", stage, err)
			}
			order := map[string]string{"write": "write", "sync": "write,sync", "close": "write,sync,close", "replace": "write,sync,close,replace"}
			if strings.Join(called, ",") != order[stage] {
				t.Fatalf("commit continued after %s failed: %v", stage, called)
			}
			disk, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(disk, original) {
				t.Fatal("failed commit replaced original bytes")
			}
			files, err := os.ReadDir(UserDataDir)
			if err != nil || len(files) != 1 || files[0].Name() != "N2WQ-1.yaml" {
				t.Fatal("failed commit retained a temporary file")
			}
		})
	}
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	disk, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(disk, expected) {
		t.Fatal("failed attempts changed a later successful exact-byte commit")
	}
}

func TestPreparedRecordZeroValueCannotWrite(t *testing.T) {
	for _, prepared := range []*PreparedUserRecord{nil, {}} {
		called := false
		err := prepared.commitWith(func(_ string, _ []byte) error { called = true; return nil })
		if err == nil || called || prepared.ByteLen() != 0 {
			t.Fatal("empty prepared payload reached a writer")
		}
	}
}
