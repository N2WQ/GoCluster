package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadISEDPublicSettings(t *testing.T) {
	cfg, err := Load(testConfigDir(t))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := ISEDConfig{
		Enabled: true, URL: "https://apc-cap.ic.gc.ca/datafiles/amateur_delim.zip",
		SpecialURL: "https://apc-cap.ic.gc.ca/datafiles/special_callsign.zip",
		Archive:    "data/ised/amateur_delim.zip", SpecialArchive: "data/ised/special_callsign.zip",
		DBPath: "data/ised/ised.db", TempDir: "data/ised", RefreshUTC: "22:15",
	}
	if cfg.ISED != want {
		t.Fatalf("ISED = %#v, want %#v", cfg.ISED, want)
	}
	if cfg.FCCULS.CacheTTLSeconds != 21600 {
		t.Fatalf("shared cache TTL = %d, want 21600", cfg.FCCULS.CacheTTLSeconds)
	}
}

func TestLoadISEDDisabledPreservesReferenceSettings(t *testing.T) {
	dir := testConfigDir(t)
	writeTestConfigOverlay(t, dir, "data.yaml", `ised:
  enabled: false
  url: " http://example.invalid/assigned.zip "
  special_url: " https://example.invalid/special.zip "
  archive_path: " custom/assigned.zip "
  special_archive_path: " custom/special.zip "
  db_path: " custom/ised.db "
  temp_dir: " shared-build "
  refresh_utc: " 00:00 "
fcc_uls:
  temp_dir: "shared-build"
  cache_ttl_seconds: 17
`)
	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := ISEDConfig{
		Enabled: false, URL: "http://example.invalid/assigned.zip", SpecialURL: "https://example.invalid/special.zip",
		Archive: "custom/assigned.zip", SpecialArchive: "custom/special.zip", DBPath: "custom/ised.db",
		TempDir: "shared-build", RefreshUTC: "00:00",
	}
	if cfg.ISED != want || cfg.FCCULS.CacheTTLSeconds != 17 {
		t.Fatalf("loaded ISED=%#v TTL=%d, want ISED=%#v TTL=17", cfg.ISED, cfg.FCCULS.CacheTTLSeconds, want)
	}
}

func TestLoadRequiresEveryISEDSetting(t *testing.T) {
	keys := []string{"enabled", "url", "special_url", "archive_path", "special_archive_path", "db_path", "temp_dir", "refresh_utc"}
	for _, key := range keys {
		for _, null := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/null=%t", key, null), func(t *testing.T) {
				dir := testConfigDir(t)
				want := fmt.Sprintf("required YAML setting %q is missing", "ised."+key)
				if null {
					writeTestConfigOverlay(t, dir, "data.yaml", "ised:\n  "+key+": null\n")
					want = fmt.Sprintf("required YAML setting %q must not be null", "ised."+key)
				} else {
					removeTestConfigKey(t, dir, "data.yaml", "ised", key)
				}
				if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("Load error = %v, want %q", err, want)
				}
			})
		}
	}
	for _, null := range []bool{false, true} {
		t.Run(fmt.Sprintf("block/null=%t", null), func(t *testing.T) {
			dir := testConfigDir(t)
			if null {
				writeTestConfigOverlay(t, dir, "data.yaml", "ised: null\n")
			} else {
				removeTestConfigKey(t, dir, "data.yaml", "ised")
			}
			if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), `required YAML setting "ised"`) {
				t.Fatalf("Load error = %v, want required ISED block error", err)
			}
		})
	}
}

func TestLoadRejectsInvalidISEDValues(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"enabled", "-1"}, {"url", `""`}, {"url", `"ftp://example.invalid/a.zip"`},
		{"url", `"https:///a.zip"`}, {"url", `"https://bad host/a.zip"`},
		{"special_url", `"/relative/special.zip"`}, {"special_url", `""`},
		{"archive_path", `""`}, {"special_archive_path", `" "`}, {"db_path", `""`}, {"temp_dir", `""`},
		{"refresh_utc", `""`}, {"refresh_utc", `"24:00"`}, {"refresh_utc", `"2:15"`}, {"refresh_utc", `"22:15:00"`},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			dir := testConfigDir(t)
			writeTestConfigOverlay(t, dir, "data.yaml", "ised:\n  "+tc.key+": "+tc.value+"\n")
			if _, err := Load(dir); err == nil {
				t.Fatal("Load accepted invalid ISED value")
			}
		})
	}
}

func TestLoadRejectsLicenseDataPathCollisions(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"archive_path", "data/fcc/l_amat.zip"},
		{"archive_path", "data/fcc/fcc_uls.db"},
		{"archive_path", "data/fcc/allowlist.txt"},
		{"special_archive_path", "data/ised/amateur_delim.zip"},
		{"special_archive_path", "data/ised/amateur_delim.zip.status.json"},
		{"db_path", "data/fcc/l_amat.zip.status.json"},
		{"db_path", "data/fcc/l_amat.zip.meta.json"},
		{"db_path", "data/ised/special_callsign.zip.status.json"},
		{"db_path", "data/ised/../fcc/fcc_uls.db"},
		{"db_path", "DATA/FCC/FCC_ULS.DB"},
		{"archive_path", "data/fcc/fcc_uls.db/assigned.zip"},
		{"archive_path", "data/fcc"},
		{"temp_dir", "data/ised/ised.db"},
		{"temp_dir", "data/fcc/fcc_uls.db/temporary"},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			dir := testConfigDir(t)
			writeTestConfigOverlay(t, dir, "data.yaml", fmt.Sprintf("ised:\n  %s: %q\n", tc.key, tc.value))
			if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "license data path collision") {
				t.Fatalf("Load error = %v, want path collision", err)
			}
		})
	}
}

func TestLoadRejectsLicenseDataSymlinkParentCollision(t *testing.T) {
	dir := testConfigDir(t)
	dataDir := t.TempDir()
	realDir := filepath.Join(dataDir, "real")
	aliasDir := filepath.Join(dataDir, "alias")
	if err := os.Mkdir(realDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realDir, aliasDir); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	writeTestConfigOverlay(t, dir, "data.yaml", fmt.Sprintf("fcc_uls:\n  db_path: %q\nised:\n  db_path: %q\n",
		filepath.Join(realDir, "future.db"), filepath.Join(aliasDir, "future.db")))
	if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "license data path collision") {
		t.Fatalf("Load error = %v, want aliased path collision", err)
	}
}

func TestLoadRejectsLicenseDataDanglingSymlinks(t *testing.T) {
	for _, name := range []string{"metadata leaf", "directory ancestor"} {
		t.Run(name, func(t *testing.T) {
			dir := testConfigDir(t)
			dataDir := t.TempDir()
			archive := filepath.Join(dataDir, "main.zip")
			db := filepath.Join(dataDir, "future.db")
			link, target := archive+".status.json", db
			if name == "directory ancestor" {
				link, target = filepath.Join(dataDir, "alias"), filepath.Join(dataDir, "future-dir")
				archive = filepath.Join(link, "main.zip")
			}
			if err := os.Symlink(target, link); err != nil {
				t.Skipf("symlink creation unavailable: %v", err)
			}
			writeTestConfigOverlay(t, dir, "data.yaml", fmt.Sprintf("ised:\n  archive_path: %q\n  db_path: %q\n", archive, db))
			if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "dangling symlink") {
				t.Fatalf("Load error = %v, want dangling symlink rejection", err)
			}
			if _, err := os.Stat(target); !os.IsNotExist(err) {
				t.Fatalf("symlink target changed during config loading: %v", err)
			}
		})
	}
}

func TestLoadRejectsLicenseDataHardLinkCollision(t *testing.T) {
	dir := testConfigDir(t)
	dataDir := t.TempDir()
	db := filepath.Join(dataDir, "fcc.db")
	archive := filepath.Join(dataDir, "ised.zip")
	if err := os.WriteFile(db, []byte("database snapshot"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(db, archive+".status.json"); err != nil {
		t.Skipf("hard link creation unavailable: %v", err)
	}
	writeTestConfigOverlay(t, dir, "data.yaml", fmt.Sprintf("fcc_uls:\n  db_path: %q\nised:\n  archive_path: %q\n", db, archive))
	if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "license data path collision") {
		t.Fatalf("Load error = %v, want hard-link path collision", err)
	}
}
