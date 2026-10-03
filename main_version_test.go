package main

import "testing"

func TestShortRevision(t *testing.T) {
	if got := shortRevision("1234567890abcdef"); got != "1234567890ab" {
		t.Fatalf("shortRevision truncation mismatch: got %q", got)
	}
	if got := shortRevision("1234"); got != "1234" {
		t.Fatalf("shortRevision short value mismatch: got %q", got)
	}
}

func TestCompileDateVersion(t *testing.T) {
	cases := []struct {
		name      string
		buildTime string
		want      string
	}{
		{name: "selected format", buildTime: "2026-10-03T12:34:56Z", want: "261003"},
		{name: "zero padding", buildTime: "2026-01-02T00:00:00Z", want: "260102"},
		{name: "UTC next year", buildTime: "2026-12-31T23:30:00-05:00", want: "270101"},
		{name: "UTC previous day", buildTime: "2026-10-03T00:30:00+02:00", want: "261002"},
		{name: "leap day", buildTime: "2028-02-29T12:00:00Z", want: "280229"},
		{name: "bad date", buildTime: "unknown", want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := compileDateVersion(tc.buildTime); got != tc.want {
				t.Fatalf("compileDateVersion() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestResolveBinaryVersionDateOnlyPreservesMetadata(t *testing.T) {
	oldVersion, oldCommit, oldBuildTime := Version, Commit, BuildTime
	t.Cleanup(func() { Version, Commit, BuildTime = oldVersion, oldCommit, oldBuildTime })
	Commit, BuildTime = "abcdef123456", "2026-10-03T12:34:56Z"
	for _, version := range []string{"dev", "261003"} {
		Version = version
		info := resolveBinaryVersion()
		if info.version != "261003" || info.commit != Commit || info.buildTime != BuildTime {
			t.Fatalf("resolved identity: %+v", info)
		}
		build := info.clusterBuildInfo()
		if build.Version != info.version || build.Commit != info.commit || build.BuildTime != info.buildTime || build.VCSModified != info.vcsModified || build.GoVersion != info.goVersion {
			t.Fatalf("runtime lost build metadata: %+v", build)
		}
	}
}

func TestShouldPrintVersion(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want bool
	}{
		{name: "long", args: []string{"--version"}, want: true},
		{name: "short", args: []string{"-version"}, want: true},
		{name: "word", args: []string{"version"}, want: true},
		{name: "mixed", args: []string{"run", "--version"}, want: true},
		{name: "none", args: []string{"run"}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldPrintVersion(tc.args); got != tc.want {
				t.Fatalf("shouldPrintVersion(%v) = %v, want %v", tc.args, got, tc.want)
			}
		})
	}
}
