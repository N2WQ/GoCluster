package pathreliability

import "testing"

func TestNoiseModelPenaltyAndClassLookup(t *testing.T) {
	cfg := DefaultConfig()
	model := cfg.NoiseModel()

	if got := model.Penalty("URBAN"); got != 17 {
		t.Fatalf("expected urban penalty 17, got %v", got)
	}
	if got := model.Penalty("urban"); got != 17 {
		t.Fatalf("expected normalized urban penalty 17, got %v", got)
	}
	if !model.HasClass("QUIET") {
		t.Fatalf("expected quiet class to be valid")
	}
	if got := model.Penalty("QUIET"); got != 0 {
		t.Fatalf("expected quiet class to have zero penalty, got %v", got)
	}
	if model.HasClass("MOBILE") {
		t.Fatalf("expected unknown class to be invalid")
	}
	if got := model.Penalty("MOBILE"); got != 0 {
		t.Fatalf("expected unknown class penalty 0, got %v", got)
	}
}

func BenchmarkNoiseModelPenalty(b *testing.B) {
	model := DefaultConfig().NoiseModel()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = model.Penalty("URBAN")
	}
}
