package pathreliability

import "testing"

func TestNoiseModelPenaltyAndClassLookup(t *testing.T) {
	cfg := DefaultConfig()
	model := cfg.NoiseModel()

	if got := model.Penalty("URBAN", "160m"); got != 0 {
		t.Fatalf("expected urban 160m penalty 0, got %v", got)
	}
	if got := model.Penalty("urban", "6M"); got != 0 {
		t.Fatalf("expected urban 6m penalty 0, got %v", got)
	}
	if !model.HasClass("QUIET") {
		t.Fatalf("expected quiet class to be valid")
	}
	if got := model.Penalty("QUIET", "160m"); got != 0 {
		t.Fatalf("expected quiet class to have zero penalty, got %v", got)
	}
	if model.HasClass("MOBILE") {
		t.Fatalf("expected unknown class to be invalid")
	}
	if got := model.Penalty("MOBILE", "20m"); got != 0 {
		t.Fatalf("expected unknown class penalty 0, got %v", got)
	}
	if got := model.Penalty("URBAN", "2m"); got != 0 {
		t.Fatalf("expected unknown band penalty 0, got %v", got)
	}
}

func BenchmarkNoiseModelPenalty(b *testing.B) {
	model := DefaultConfig().NoiseModel()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = model.Penalty("URBAN", "20m")
	}
}
