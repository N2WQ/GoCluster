package propreport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

type testReportSnapshot struct {
	reader io.Reader
	close  func() error
}

func (s testReportSnapshot) Reader() io.Reader { return s.reader }
func (s testReportSnapshot) Close() error      { return s.close() }

type blockingReportReader struct {
	entered chan struct{}
	resume  chan struct{}
}

type errorReportReader struct{ err error }

func (r errorReportReader) Read([]byte) (int, error) { return 0, r.err }

func (r blockingReportReader) Read([]byte) (int, error) {
	close(r.entered)
	<-r.resume
	return 0, io.EOF
}

func TestReportWorkerRetainsReservationDuringStalledOperations(t *testing.T) {
	for _, stall := range []string{"open", "read", "close"} {
		t.Run(stall, func(t *testing.T) {
			var gate reportWorkerGate
			entered, resume := make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			completed := make(chan error, 1)
			go func() {
				_, err := gate.scan(ctx, func() (reportSnapshot, error) {
					if stall == "open" {
						close(entered)
						<-resume
					}
					var reader io.Reader = strings.NewReader("2026/11/02 00:00:00 record\n")
					if stall == "read" {
						reader = blockingReportReader{entered, resume}
					}
					return testReportSnapshot{reader: reader, close: func() error {
						if stall == "close" {
							close(entered)
							<-resume
						}
						return nil
					}}, nil
				})
				completed <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("worker did not enter operation")
			}
			cancel()
			select {
			case err := <-completed:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("cancellation did not return")
			}
			_, err := gate.scan(context.Background(), func() (reportSnapshot, error) {
				t.Fatal("replacement opened while old operation was stalled")
				return nil, errors.New("unreachable after fatal")
			})
			if !errors.Is(err, errReportWorkerUnavailable) {
				t.Fatalf("replacement: %v", err)
			}
			close(resume)
			deadline := time.After(5 * time.Second)
			for {
				gate.mu.Lock()
				occupied := gate.occupied
				gate.mu.Unlock()
				if !occupied {
					break
				}
				select {
				case <-deadline:
					t.Fatal("finished worker retained reservation")
				default:
					time.Sleep(time.Millisecond)
				}
			}
			_, err = gate.scan(context.Background(), func() (reportSnapshot, error) {
				return testReportSnapshot{strings.NewReader(""), func() error { return nil }}, nil
			})
			if err != nil {
				t.Fatalf("replacement after cleanup: %v", err)
			}
		})
	}
}

func TestReportWorkerRetainsUncertainCleanup(t *testing.T) {
	var gate reportWorkerGate
	closeErr := errors.New("uncertain close")
	_, err := gate.scan(context.Background(), func() (reportSnapshot, error) {
		return testReportSnapshot{strings.NewReader(""), func() error { return closeErr }}, nil
	})
	if !errors.Is(err, closeErr) {
		t.Fatalf("close: %v", err)
	}
	gate.mu.Lock()
	retained := gate.uncertain != nil
	gate.mu.Unlock()
	if !retained {
		t.Fatal("uncertain snapshot is not strongly retained")
	}
	_, err = gate.scan(context.Background(), func() (reportSnapshot, error) {
		t.Fatal("opened after uncertain close")
		return nil, errors.New("unreachable after fatal")
	})
	if !errors.Is(err, errReportWorkerUnavailable) {
		t.Fatalf("replacement: %v", err)
	}
}

func TestReportWorkerRetainsUncertainOpenCleanup(t *testing.T) {
	var gate reportWorkerGate
	openErr := fmt.Errorf("owned failed acquisition: %w", errReportCleanupUncertain)
	_, err := gate.scan(context.Background(), func() (reportSnapshot, error) { return nil, openErr })
	if !errors.Is(err, errReportCleanupUncertain) {
		t.Fatalf("open: %v", err)
	}
	gate.mu.Lock()
	retained := errors.Is(gate.uncertainOpen, openErr)
	gate.mu.Unlock()
	if !retained {
		t.Fatal("failed acquisition owner was not retained")
	}
	_, err = gate.scan(context.Background(), func() (reportSnapshot, error) {
		t.Fatal("opened after uncertain acquisition cleanup")
		return nil, errors.New("unreachable after fatal")
	})
	if !errors.Is(err, errReportWorkerUnavailable) {
		t.Fatalf("replacement: %v", err)
	}
}

func TestReportWorkerReleasesAfterDefinitiveOpenFailure(t *testing.T) {
	var gate reportWorkerGate
	failure := errors.New("open failed and cleaned up")
	_, err := gate.scan(context.Background(), func() (reportSnapshot, error) { return nil, failure })
	if !errors.Is(err, failure) {
		t.Fatalf("open: %v", err)
	}
	_, err = gate.scan(context.Background(), func() (reportSnapshot, error) {
		return testReportSnapshot{strings.NewReader(""), func() error { return nil }}, nil
	})
	if err != nil {
		t.Fatalf("replacement after cleaned open failure: %v", err)
	}
}

func TestReportWorkerPoisonedByInvalidOpenResult(t *testing.T) {
	var gate reportWorkerGate
	_, err := gate.scan(context.Background(), func() (reportSnapshot, error) {
		return nil, nil //nolint:nilnil // The fixture deliberately tests an invalid acquisition result.
	})
	if !errors.Is(err, errReportCleanupUncertain) {
		t.Fatalf("invalid open: %v", err)
	}
	_, err = gate.scan(context.Background(), func() (reportSnapshot, error) {
		t.Fatal("opened after invalid acquisition")
		return nil, errors.New("unreachable after fatal")
	})
	if !errors.Is(err, errReportWorkerUnavailable) {
		t.Fatalf("replacement: %v", err)
	}
}

func TestReportWorkerClosesAfterReadFailure(t *testing.T) {
	var gate reportWorkerGate
	readErr := errors.New("read failed")
	closed := false
	_, err := gate.scan(context.Background(), func() (reportSnapshot, error) {
		return testReportSnapshot{errorReportReader{readErr}, func() error { closed = true; return nil }}, nil
	})
	if !errors.Is(err, readErr) || !closed {
		t.Fatalf("read error = %v, closed = %v", err, closed)
	}
	_, err = gate.scan(context.Background(), func() (reportSnapshot, error) {
		return testReportSnapshot{strings.NewReader(""), func() error { return nil }}, nil
	})
	if err != nil {
		t.Fatalf("replacement after cleaned read failure: %v", err)
	}
}
