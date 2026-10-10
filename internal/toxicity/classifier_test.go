package toxicity

import (
	"context"
	"errors"
	"testing"
	"time"

	"dxcluster/spot"
)

type fakeClient struct {
	decision Decision
	seen     []string
}

func (f *fakeClient) Classify(_ context.Context, comment string) (Decision, error) {
	f.seen = append(f.seen, comment)
	return f.decision, nil
}

type timeoutClient struct{}

func (timeoutClient) Classify(ctx context.Context, _ string) (Decision, error) {
	<-ctx.Done()
	return Decision{}, ctx.Err()
}

type errorClient struct{}

func (errorClient) Classify(context.Context, string) (Decision, error) {
	return Decision{}, errors.New("bad response")
}

type sequenceClient struct {
	results []sequenceResult
	calls   int
}

type sequenceResult struct {
	decision Decision
	err      error
}

func (s *sequenceClient) Classify(context.Context, string) (Decision, error) {
	idx := s.calls
	s.calls++
	if idx >= len(s.results) {
		return Decision{}, errors.New("unexpected call")
	}
	result := s.results[idx]
	return result.decision, result.err
}

func TestClassifierBypassesRoutineAndRoutesMultilingualToAI(t *testing.T) {
	gate := testGate(t)
	client := &fakeClient{decision: Decision{Status: spot.ToxicitySafe, Model: "mock"}}
	classifier := NewClassifier(Config{
		Enabled:         true,
		Workers:         1,
		QueueSize:       2,
		CacheMaxEntries: 8,
		CacheTTLSeconds: 60,
		MaxCommentBytes: 512,
		TimeoutMS:       100,
	}, gate, client)
	classifier.Start()
	defer classifier.Stop()

	routine := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
	routine.Comment = "POTA 73"
	if !classifier.ClassifyOrEnqueue(routine, time.Now().UTC()) {
		t.Fatalf("routine comment should not enqueue")
	}
	if routine.ToxicityStatus != spot.ToxicitySafeLocal {
		t.Fatalf("expected SAFE_LOCAL, got %q", routine.ToxicityStatus)
	}

	comments := []string{
		"thanks for the contact",
		"gracias por el contacto",
		"merci pour le contact",
		"danke für den kontakt",
		"grazie per il collegamento",
		"obrigado pelo contato",
	}
	for _, comment := range comments {
		multilingual := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
		multilingual.Comment = comment
		if classifier.ClassifyOrEnqueue(multilingual, time.Now().UTC()) {
			t.Fatalf("non-routine multilingual comment %q should enqueue", comment)
		}
		select {
		case result := <-classifier.Results():
			ApplyResult(result)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for classifier result")
		}
		if multilingual.ToxicityStatus != spot.ToxicitySafe {
			t.Fatalf("expected AI safe decision, got %q", multilingual.ToxicityStatus)
		}
	}
	if len(client.seen) != len(comments) {
		t.Fatalf("expected all multilingual comments sent to AI, got %#v", client.seen)
	}
	for i, comment := range comments {
		if client.seen[i] != comment {
			t.Fatalf("expected complete cleaned multilingual comment %q, got %q", comment, client.seen[i])
		}
	}
}

func TestClassifierZeroWorkersDisables(t *testing.T) {
	classifier := NewClassifier(Config{
		Enabled:         true,
		Workers:         0,
		QueueSize:       1,
		CacheMaxEntries: 1,
		CacheTTLSeconds: 60,
		MaxCommentBytes: 512,
		TimeoutMS:       100,
	}, testGate(t), &fakeClient{})
	if classifier != nil {
		t.Fatalf("zero workers should disable classifier")
	}
}

func TestClassifierQueueFullFailsOpen(t *testing.T) {
	classifier := NewClassifier(Config{
		Enabled:         true,
		Workers:         1,
		QueueSize:       1,
		CacheMaxEntries: 1,
		CacheTTLSeconds: 60,
		MaxCommentBytes: 512,
		TimeoutMS:       100,
	}, testGate(t), &fakeClient{})
	if classifier == nil {
		t.Fatal("expected classifier")
	}

	first := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
	first.IsHuman = true
	first.Comment = "please classify this"
	if classifier.ClassifyOrEnqueue(first, time.Now().UTC()) {
		t.Fatalf("first non-routine comment should enqueue")
	}

	second := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
	second.IsHuman = true
	second.Comment = "this cannot fit in the queue"
	if !classifier.ClassifyOrEnqueue(second, time.Now().UTC()) {
		t.Fatalf("queue-full comment should fail open")
	}
	if second.ToxicityStatus != spot.ToxicityUnavailable {
		t.Fatalf("expected UNAVAILABLE, got %q", second.ToxicityStatus)
	}
	stats := classifier.Snapshot()
	if stats.QueueFull != 1 || stats.Unavailable != 1 {
		t.Fatalf("expected queue full unavailable counters, got %#v", stats)
	}
}

func TestClassifierTimeoutAndErrorFailOpen(t *testing.T) {
	for name, client := range map[string]classifierClient{
		"timeout": timeoutClient{},
		"error":   errorClient{},
	} {
		t.Run(name, func(t *testing.T) {
			timeoutMS := 1000
			if name == "timeout" {
				timeoutMS = 1
			}
			classifier := NewClassifier(Config{
				Enabled:         true,
				Workers:         1,
				QueueSize:       1,
				CacheMaxEntries: 1,
				CacheTTLSeconds: 60,
				MaxCommentBytes: 512,
				TimeoutMS:       timeoutMS,
			}, testGate(t), client)
			classifier.Start()
			defer classifier.Stop()

			s := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
			s.IsHuman = true
			s.Comment = "non routine"
			if classifier.ClassifyOrEnqueue(s, time.Now().UTC()) {
				t.Fatalf("expected comment to enqueue")
			}
			select {
			case result := <-classifier.Results():
				ApplyResult(result)
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for classifier result")
			}
			if s.ToxicityStatus != spot.ToxicityUnavailable {
				t.Fatalf("expected UNAVAILABLE, got %q", s.ToxicityStatus)
			}
			classifier.Stop()
			want := Stats{AICalls: 1, Unavailable: 1}
			if name == "timeout" {
				want.Timeouts = 1
			} else {
				want.Malformed = 1
			}
			if stats := classifier.Snapshot(); stats != want {
				t.Fatalf("ordinary %s outcome counters: got %#v want %#v", name, stats, want)
			}
		})
	}
}

func TestClassifierDoesNotCacheUnavailable(t *testing.T) {
	client := &sequenceClient{results: []sequenceResult{
		{err: errors.New("temporary worker failure")},
		{decision: Decision{Status: spot.ToxicitySafe, Model: "mock"}},
	}}
	classifier := NewClassifier(Config{
		Enabled:         true,
		Workers:         1,
		QueueSize:       1,
		CacheMaxEntries: 8,
		CacheTTLSeconds: 60,
		MaxCommentBytes: 512,
		TimeoutMS:       100,
	}, testGate(t), client)
	classifier.Start()
	defer classifier.Stop()

	first := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
	first.IsHuman = true
	first.Comment = "same conversational comment"
	if classifier.ClassifyOrEnqueue(first, time.Now().UTC()) {
		t.Fatalf("expected first comment to enqueue")
	}
	select {
	case result := <-classifier.Results():
		ApplyResult(result)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first classifier result")
	}
	if first.ToxicityStatus != spot.ToxicityUnavailable {
		t.Fatalf("expected first result UNAVAILABLE, got %q", first.ToxicityStatus)
	}

	second := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
	second.IsHuman = true
	second.Comment = "same conversational comment"
	if classifier.ClassifyOrEnqueue(second, time.Now().UTC()) {
		t.Fatalf("expected second comment to enqueue instead of hitting unavailable cache")
	}
	select {
	case result := <-classifier.Results():
		ApplyResult(result)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second classifier result")
	}
	if second.ToxicityStatus != spot.ToxicitySafe {
		t.Fatalf("expected second result SAFE, got %q", second.ToxicityStatus)
	}
	if client.calls != 2 {
		t.Fatalf("expected two AI calls, got %d", client.calls)
	}
}
