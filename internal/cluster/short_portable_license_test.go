package cluster

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"dxcluster/cty"
	"dxcluster/spot"
	"dxcluster/uls"
)

func shortPortableFCCFixture(t *testing.T, assigned bool) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fcc.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(t.Context(), `CREATE TABLE AM(unique_system_identifier INTEGER,call_sign TEXT,state TEXT NOT NULL);
CREATE INDEX idx_AM_call_sign ON AM(call_sign);
INSERT INTO AM VALUES(1,'K1ABC','TX'); PRAGMA user_version=1;`)
	if err == nil && assigned {
		_, err = db.ExecContext(t.Context(), "INSERT INTO AM VALUES(2,'W1A','CA');")
	}
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("FCC fixture: %v %v", err, closeErr)
	}
	previous := uls.LicenseChecksEnabled()
	uls.SetRefreshInProgress(false)
	uls.SetLicenseDBPath(path)
	uls.SetLicenseChecksEnabled(true)
	t.Cleanup(func() { uls.SetLicenseDBPath(""); uls.SetLicenseChecksEnabled(previous) })
}

func shortPortableValidator(db *cty.CTYDatabase) *ingestValidator {
	return newIngestValidator(func() *cty.CTYDatabase { return db }, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
}

func TestShortPortableRoleChecksUseBaseIdentity(t *testing.T) {
	path := configureCanadianClusterFixture(t, true)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(t.Context(), "INSERT INTO CA VALUES('VE3A','ON');")
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("Canadian fixture: %v %v", err, closeErr)
	}
	shortPortableFCCFixture(t, true)
	ctyDB := canadianClusterCTY(t)
	v := shortPortableValidator(ctyDB)
	uls.SetAllowlistPath("")
	t.Cleanup(func() { uls.SetAllowlistPath("") })
	for _, row := range []struct{ call, state string }{
		{"VE3/W1A", "CA"}, {"W1A/VE3", "CA"},
		{"W1234/VE3A", "ON"}, {"VE3A/W1234", "ON"},
	} {
		t.Run(row.call, func(t *testing.T) {
			de := spot.NewSpot("K1ABC", row.call, 14020, "CW")
			if !v.validateSpot(de) || de.DEMetadata.State != row.state {
				t.Fatalf("DE check: %+v", de.DEMetadata)
			}
			dx := spot.NewSpot(row.call, "K1ABC", 14020, "CW")
			if !v.validateSpot(dx) || applyLicenseGate(dx, ctyDB, nil, nil) || dx.DXMetadata.State != row.state {
				t.Fatalf("DX check: %+v", dx.DXMetadata)
			}
		})
	}
}

func TestShortPortableRoleAllowlistUsesBaseAuthority(t *testing.T) {
	configureCanadianClusterFixture(t, true)
	shortPortableFCCFixture(t, false)
	db := canadianClusterCTY(t)
	v := shortPortableValidator(db)
	t.Cleanup(func() { uls.SetAllowlistPath("") })
	for _, row := range []struct{ call, qualified, wrong string }{
		{"VE3/W1A", "US:^W1A$", "1:^W1A$"},
		{"W1A/VE3", "US:^W1A$", "1:^W1A$"},
		{"W1234/VE3A", "1:^VE3A$", "US:^VE3A$"},
		{"VE3A/W1234", "1:^VE3A$", "US:^VE3A$"},
	} {
		t.Run(row.call, func(t *testing.T) {
			for _, policy := range []struct {
				entry string
				admit bool
			}{{"", false}, {row.wrong, false}, {row.qualified, true}} {
				path := filepath.Join(t.TempDir(), "allowlist.txt")
				if err := os.WriteFile(path, []byte(policy.entry+"\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				uls.SetAllowlistPath(path)
				de := spot.NewSpot("K1ABC", row.call, 14020, "CW")
				if got := v.validateSpot(de); got != policy.admit || de.DEMetadata.State != "" {
					t.Fatalf("DE policy=%q admitted=%t metadata=%+v", policy.entry, got, de.DEMetadata)
				}
				dx := spot.NewSpot(row.call, "K1ABC", 14020, "CW")
				if !v.validateSpot(dx) {
					t.Fatal("DE anchor was rejected")
				}
				if dropped := applyLicenseGate(dx, db, nil, nil); dropped == policy.admit || dx.DXMetadata.State != "" {
					t.Fatalf("DX policy=%q dropped=%t metadata=%+v", policy.entry, dropped, dx.DXMetadata)
				}
			}
		})
	}
}
