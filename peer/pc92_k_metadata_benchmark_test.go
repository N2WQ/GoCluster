package peer

import "testing"

var effectiveSubjectBenchmarkSink PC92Entry

func BenchmarkPC92EffectiveNodeSubject(b *testing.B) {
	for _, tc := range []struct {
		name, action, version, build, wantVersion, wantBuild string
	}{
		{"K_omitted", "K", "", "", "0", "0"},
		{"K_partial", "K", "5457", "", "5457", "0"},
		{"K_explicit", "K", "5457", "633", "5457", "633"},
		{"C_omitted", "C", "", "", "", ""},
	} {
		b.Run(tc.name, func(b *testing.B) {
			record := PC92Record{Action: tc.action, Subject: PC92Entry{Call: "N2AAA", Flags: 5, Version: tc.version, Build: tc.build}}
			b.ReportAllocs()
			for b.Loop() {
				effectiveSubjectBenchmarkSink = effectiveNodeSubject(&record)
			}
			if effectiveSubjectBenchmarkSink.Version != tc.wantVersion || effectiveSubjectBenchmarkSink.Build != tc.wantBuild || record.Subject.Version != tc.version || record.Subject.Build != tc.build {
				b.Fatal("effective view or immutable record invariant failed")
			}
		})
	}
}
