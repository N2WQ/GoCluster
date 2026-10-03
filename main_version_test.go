package main

import (
	"os"
	"testing"
)

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
	oldVersion, oldReleaseTag, oldCommit, oldBuildTime := Version, ReleaseTag, Commit, BuildTime
	t.Cleanup(func() { Version, ReleaseTag, Commit, BuildTime = oldVersion, oldReleaseTag, oldCommit, oldBuildTime })
	ReleaseTag = " 261003r2 "
	Commit, BuildTime = "abcdef123456", "2026-10-03T12:34:56Z"
	for _, version := range []string{"dev", "261003"} {
		Version = version
		info := resolveBinaryVersion()
		if info.version != "261003" || info.releaseTag != "261003r2" || info.commit != Commit || info.buildTime != BuildTime {
			t.Fatalf("resolved identity: %+v", info)
		}
		build := info.clusterBuildInfo()
		if build.Version != info.version || build.ReleaseTag != info.releaseTag || build.Commit != info.commit || build.BuildTime != info.buildTime || build.GoVersion != info.goVersion {
			t.Fatalf("runtime lost build metadata: %+v", build)
		}
	}
}

func TestPrintVersionReleaseMetadata(t *testing.T) {
	for _, releaseTag := range []string{"", "261003r2"} {
		t.Run(releaseTag, func(t *testing.T) {
			output, err := os.CreateTemp(t.TempDir(), "version-output")
			if err != nil {
				t.Fatal(err)
			}
			previousStdout := os.Stdout
			t.Cleanup(func() {
				os.Stdout = previousStdout
				_ = output.Close()
			})
			os.Stdout = output
			printVersion(binaryVersion{version: "261003", releaseTag: releaseTag, commit: "abcdef123456", buildTime: "2026-10-03T12:34:56Z", goVersion: "go1.26.4"})
			os.Stdout = previousStdout
			if err := output.Close(); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(output.Name())
			if err != nil {
				t.Fatal(err)
			}
			want := "Product version: 261003\n"
			if releaseTag != "" {
				want += "Release tag:     261003r2\n"
			}
			want += "Commit:          abcdef123456\nBuilt:           2026-10-03T12:34:56Z\nGo:              go1.26.4\n"
			if string(got) != want {
				t.Fatalf("version output = %q, want %q", got, want)
			}
		})
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
