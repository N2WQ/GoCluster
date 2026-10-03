package sqlite3

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestV15SQLiteLargePragmaCompatibility(t *testing.T) {
	c, err := OpenTopologyContext(WithMaxMemory(t.Context(), 8<<20), filepath.Join(t.TempDir(), "pragma.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = c.Exec("create table kept(v);insert into kept values('original')"); err != nil {
		t.Fatal(err)
	}
	large := strings.Repeat("\xff", 63000)
	for _, sql := range []string{"pragma " + large, "pragma checksum_verification='" + large + "'", "pragma checksum_verification='0" + large + "'", "pragma checksum_verification='9" + large + "'", "pragma CHECKSUM_VERIFICATION='FALSE'"} {
		if err = c.Exec(sql); err != nil {
			t.Fatalf("long unknown pragma/value changed acceptance: %v", err)
		}
	}
	s, _, err := c.Prepare("select v from kept")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !s.Step() || s.ColumnText(0) != "original" {
		t.Fatal("pragma processing changed saved contents", s.Err())
	}
}
