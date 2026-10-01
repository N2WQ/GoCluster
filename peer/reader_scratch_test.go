package peer

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

func TestReaderScratchWaitHonorsCancellationAndPhaseDeadline(t *testing.T) {
	for _, cancelWait := range []bool{false, true} {
		budget := newFrameParseBudget()
		full, err := budget.acquireCharge(context.Background(), time.Time{}, peerParseScratchBytes)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		local, remote := net.Pipe()
		read := make(chan struct{})
		r := newLineReaderWithTransport(local, 65536, 65536, func(dst []byte) (int, error) {
			close(read)
			return copy(dst, "PC51^W1AAA^W2AAA^0^~"), nil
		}, &telnetParser{}, nil)
		r.acquireScratch = func(deadline time.Time) (frameParseLease, error) {
			return budget.acquireCharge(ctx, deadline, readerScratchBytes)
		}
		done := make(chan error, 1)
		go func() { _, err := r.ReadLine(time.Now().Add(30 * time.Millisecond)); done <- err }()
		<-read
		if cancelWait {
			cancel()
		}
		select {
		case err := <-done:
			want := context.DeadlineExceeded
			if cancelWait {
				want = context.Canceled
			}
			if !errors.Is(err, want) {
				t.Errorf("wait returned %v, want %v", err, want)
			}
		case <-time.After(time.Second):
			t.Error("reader scratch wait extended the fixed phase deadline")
		}
		cancel()
		_ = local.Close()
		_ = remote.Close()
		if used, _ := budget.usage(); used != peerParseScratchBytes {
			t.Errorf("reader waiter leaked scratch: %d", used)
		}
		full.release()
	}
}

type scratchCheckingParser struct {
	t      *testing.T
	budget *frameParseBudget
	parser telnetParser
}

func (p *scratchCheckingParser) Feed(data []byte) ([]byte, [][]byte) {
	if used, _ := p.budget.usage(); used != readerScratchBytes {
		p.t.Errorf("native parser ran outside its scratch reservation: %d", used)
	}
	return p.parser.Feed(data)
}

func TestReaderScratchReleasedBeforeBlockingReadAndOnReturn(t *testing.T) {
	budget := newFrameParseBudget()
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()
	chunks := []string{"PC51^W1AAA", "^W2AAA^0^~"}
	r := newLineReaderWithTransport(local, 65536, 65536, func(dst []byte) (int, error) {
		if used, _ := budget.usage(); used != 0 {
			t.Errorf("socket read retained scratch: %d", used)
		}
		if len(chunks) == 0 {
			return 0, io.EOF
		}
		n := copy(dst, chunks[0])
		chunks = chunks[1:]
		return n, nil
	}, &scratchCheckingParser{t: t, budget: budget}, nil)
	r.acquireScratch = func(deadline time.Time) (frameParseLease, error) {
		return budget.acquireCharge(context.Background(), deadline, readerScratchBytes)
	}
	line, err := r.ReadLine(time.Now().Add(time.Second))
	if err != nil || line != "PC51^W1AAA^W2AAA^0^" {
		t.Fatalf("read=%q err=%v", line, err)
	}
	if used, peak := budget.usage(); used != 0 || peak != readerScratchBytes {
		t.Fatalf("return reservation used=%d peak=%d", used, peak)
	}
}

func TestNativeTelnetReplyScratchAllocationBound(t *testing.T) {
	input := make([]byte, 4096)
	for i := 0; i+2 < len(input); i += 3 {
		input[i], input[i+1], input[i+2] = telnetIAC, telnetDO, 1
	}
	previous := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previous)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	parser := telnetParser{}
	out, replies := parser.Feed(input)
	runtime.ReadMemStats(&after)
	// Total allocation also includes all obsolete slice-growth generations,
	// making this stronger than a single live-capacity sample for this batch.
	const parserEnvelope = 128 << 10
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > parserEnvelope {
		t.Fatalf("native parser allocated %d bytes, envelope=%d", allocated, parserEnvelope)
	}
	if parserEnvelope+73728+65536+4096 > readerScratchBytes {
		t.Fatal("parser, aggregate growth and raw/remainder copy exceed shared lease")
	}
	if len(replies) != 1365 {
		t.Fatalf("reply fixture emitted%d replies", len(replies))
	}
	runtime.KeepAlive(out)
	runtime.KeepAlive(replies)
}

func BenchmarkReaderScratchLease(b *testing.B) {
	for _, limited := range []bool{false, true} {
		name := "unbudgeted"
		if limited {
			name = "shared_budget"
		}
		b.Run(name, func(b *testing.B) {
			local, remote := net.Pipe()
			defer local.Close()
			defer remote.Close()
			r := newLineReaderWithTransport(local, 65536, 65536, func(dst []byte) (int, error) {
				return copy(dst, "PC51^W1AAA^W2AAA^0^~"), nil
			}, nil, nil)
			if limited {
				budget := newFrameParseBudget()
				r.acquireScratch = func(deadline time.Time) (frameParseLease, error) {
					return budget.acquireCharge(context.Background(), deadline, readerScratchBytes)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := r.ReadLine(time.Time{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
