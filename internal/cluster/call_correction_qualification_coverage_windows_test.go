//go:build windows && !qualification

package cluster

import (
	"reflect"
	"testing"
)

func TestCCV2PacedRecoveryCoverage(t *testing.T) {
	healthy := ccQualResult{Name: "dense_canonical", Status: "recorded_comparison_required",
		BeforeRecovery: ccQualSnapshot{TemporalPending: 5976}, NaturalRecovery: true,
		OutputJoined: true, ResolverJoined: true, ShutdownJoined: true, TCPBarriersJoined: true,
		ArchiveRows: 60001, ArchiveExpected: 60001}
	for _, tc := range []struct {
		name string
		edit func(*ccQualResult)
		want bool
	}{
		{"actual_dense_recovery", func(*ccQualResult) {}, true},
		{"sampled_high_water_only", func(r *ccQualResult) { r.BeforeRecovery = ccQualSnapshot{}; r.Sampled.TemporalPending = 7000 }, false},
		{"forced_retirement", func(r *ccQualResult) { r.NaturalRecovery = false }, false},
		{"output_not_joined", func(r *ccQualResult) { r.OutputJoined = false }, false},
		{"resolver_not_joined", func(r *ccQualResult) { r.ResolverJoined = false }, false},
		{"storage_not_joined", func(r *ccQualResult) { r.ShutdownJoined = false }, false},
		{"tcp_not_completed", func(r *ccQualResult) { r.TCPBarriersJoined = false }, false},
		{"still_pending", func(r *ccQualResult) { r.AfterRecovery.StabilizerPending = 1 }, false},
		{"archive_missing", func(r *ccQualResult) { r.ArchiveMissing = 1; r.ArchiveRows-- }, false},
		{"archive_mismatch", func(r *ccQualResult) { r.ArchiveExpected++ }, false},
		{"case_failed", func(r *ccQualResult) { r.Status = "failed" }, false},
		{"case_error", func(r *ccQualResult) { r.Errors = []string{"capacity loss"} }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dense := healthy
			tc.edit(&dense)
			// The named recovery case and a nil entry contribute no positive
			// evidence; they cannot hide failed/missing dense-case coverage.
			idle := healthy
			idle.Name, idle.BeforeRecovery = "drain_recovery", ccQualSnapshot{}
			got := ccQualObservedRecoveryCases([]*ccQualResult{nil, &idle, &dense})
			var want []string
			if tc.want {
				want = []string{"dense_canonical"}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("recovery coverage=%v want=%v", got, want)
			}
		})
	}
}
