//go:build sqlite3_qualification

package sqlite3

import (
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/ncruces/go-sqlite3/vfs"
)

func TestV15SQLiteModeofRetainsBothOwners(t *testing.T) {
	root := t.TempDir()
	reference := filepath.Join(root, "reference")
	path := filepath.Join(root, "main.db")
	if err := os.WriteFile(reference, []byte("saved"), 0600); err != nil {
		t.Fatal(err)
	}
	restore := vfs.FailMetadataCloseForQualification(reference)
	defer restore()
	c, err := OpenTopologyContext(WithMaxMemory(t.Context(), 8<<20), "file:"+filepath.ToSlash(path)+"?modeof="+url.QueryEscape(reference))
	if c == nil || !errors.Is(err, IOERR_CLOSE) || !c.CleanupFailed() {
		t.Fatal("partial main/metadata failure lost owner", c, err)
	}
	if err = c.Exec("create table wrong(v)"); !errors.Is(err, IOERR_CLOSE) {
		t.Fatal("failed initialization remained usable", err)
	}
	if c.Retire() == nil {
		t.Fatal("terminal metadata failure reported released")
	}
	// The independently owned main file was released despite the retained
	// consumed metadata shell: a normal open/rename is available afterward.
	if err = os.Rename(path, path+".retired"); err != nil {
		t.Fatal("partial main file was orphaned", err)
	}
	if data, err := os.ReadFile(reference); err != nil || string(data) != "saved" {
		t.Fatal(string(data), err)
	}
}
