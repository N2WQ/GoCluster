// File role: Falsifiable refusal checks for concurrent qualification evidence.
// Synthetic raw intervals exercise the same observer used by real campaigns.
package pathreliability

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"dxcluster/internal/testsupport"
)

func TestVOACAPLiveCacheQualificationArtifact(t *testing.T) {
	report := liveQualificationSyntheticReport()
	matrix := liveQualificationMatrixReport{Test: "qualification", Order: "forward", Valid: true, Reports: []liveQualificationReport{report, report}}
	for _, suffix := range []string{".json", ".json.gz"} {
		for _, kind := range []string{"report", "matrix"} {
			t.Run(kind+suffix, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "raw"+suffix)
				var value any = report
				if kind == "matrix" {
					value = matrix
				}
				if err := liveQualificationWriteJSON(path, value); err != nil {
					t.Fatal(err)
				}
				original, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := liveQualificationWriteJSON(path, value); err == nil {
					t.Fatal("existing raw evidence was overwritten")
				}
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(original, after) {
					t.Fatal("refused overwrite changed raw evidence")
				}
				data := original
				if suffix == ".json.gz" {
					reader, err := gzip.NewReader(bytes.NewReader(original))
					if err != nil {
						t.Fatal(err)
					}
					data, err = io.ReadAll(reader)
					closeErr := reader.Close()
					if err != nil || closeErr != nil {
						t.Fatalf("decode compressed evidence: %v / %v", err, closeErr)
					}
				}
				decoder := json.NewDecoder(bytes.NewReader(data))
				if err := liveQualificationUniqueJSON(decoder); err != nil {
					t.Fatal(err)
				}
				if _, err := decoder.Token(); err != io.EOF {
					t.Fatalf("trailing JSON: %v", err)
				}
				if kind == "report" {
					var got liveQualificationReport
					if err := json.Unmarshal(data, &got); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(got, report) {
						t.Fatal("report metadata or raw arrays changed in transit")
					}
				} else {
					var got liveQualificationMatrixReport
					if err := json.Unmarshal(data, &got); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(got, matrix) {
						t.Fatal("matrix metadata or raw arrays changed in transit")
					}
				}
			})
		}
	}
	if err := liveQualificationUniqueJSON(json.NewDecoder(bytes.NewBufferString(`{"key":1,"key":2}`))); err == nil {
		t.Fatal("duplicate-key checker accepted its negative witness")
	}
}

func liveQualificationUniqueJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]bool)
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok || seen[key] {
				return fmt.Errorf("duplicate or invalid JSON key %v", token)
			}
			seen[key] = true
			if err := liveQualificationUniqueJSON(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := liveQualificationUniqueJSON(decoder); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %v", delimiter)
	}
	_, err = decoder.Token()
	return err
}

func TestVOACAPLiveCacheQualificationReaderLifecycle(t *testing.T) {
	for _, failure := range []string{"panic", "goexit", "cancel_before_release"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ownerDone, entered, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
			go func() {
				defer close(ownerDone)
				defer func() { _ = recover() }()
				readers := newLiveQualificationReaders(ctx, cancel, 1, func(int) {
					close(entered)
					defer close(exited)
					<-ctx.Done()
				})
				defer readers.Close()
				if failure == "cancel_before_release" {
					cancel()
					return
				}
				readers.Release()
				<-entered
				if failure == "panic" {
					panic("qualification lifecycle witness")
				}
				runtime.Goexit()
			}()
			select {
			case <-ownerDone:
			case <-t.Context().Done():
				t.Fatal("reader owner did not cancel, release and join")
			}
			if ctx.Err() == nil {
				t.Fatal("abnormal owner exit did not cancel readers")
			}
			if failure != "cancel_before_release" {
				select {
				case <-exited:
				default:
					t.Fatal("owner returned before reader joined")
				}
			}
		})
	}
}

func liveQualificationSyntheticReport() liveQualificationReport {
	r := liveQualificationReport{
		Case: "overlap_half", N: 64, Passes: 1, Readers: 8, ReadsPerReader: 256,
		Operations: 2048, MaintenanceOperations: 1, Completed: true, Valid: true,
		ClockSource: "testing.B.Elapsed(shared live benchmark)", ClockFrequencyHz: 10000000,
		ClockProtocol: testsupport.FixedBenchmarkProtocol,
		WorkIntervals: []liveQualificationWorkInterval{{Pass: 0, StartNS: 1, CycleEndNS: 1004, EndNS: 5000}},
		Maintenance:   []liveQualificationMaintenance{{Pass: 0, Sequence: 0, RequestStartNS: 949, StartNS: 950, EndNS: 1000, RequestEndNS: 1003}},
		Witnesses: []liveQualificationWitness{{Pass: 0, CacheBefore: 65, DelayBefore: 65, CacheAfter: 33, DelayAfter: 33,
			Operations: 2048, SuccessfulOperations: 2048, ExpectedExpired: 32, PruneAfterNS: liveQualificationNow().UnixNano(),
			StateKeysChecked: 130, StateKeysMatched: 130, TriggerCompleted: true}},
	}
	for reader := 0; reader < 8; reader++ {
		for seq := 0; seq < 256; seq++ {
			i := liveQualificationInterval{Pass: 0, Reader: reader, Sequence: seq, Ready: true}
			if seq == 0 {
				i.StartNS, i.AcquiredNS, i.EndNS = 900, 1001, 1002
			} else {
				i.StartNS = 1100 + int64(seq)*10
				i.AcquiredNS, i.EndNS = i.StartNS+1, i.StartNS+2
			}
			r.Intervals = append(r.Intervals, i)
		}
	}
	liveQualificationScoreIntervals(&r)
	return r
}

func TestVOACAPLiveCacheQualificationObserver(t *testing.T) {
	positive := liveQualificationSyntheticReport()
	if err := liveQualificationValidateConcurrent(&positive); err != nil {
		t.Fatalf("positive raw evidence rejected: %v", err)
	}
	for _, tc := range []struct {
		name  string
		fault func(*liveQualificationReport)
	}{
		{"incomplete", func(r *liveQualificationReport) { r.Completed = false }},
		{"canceled", func(r *liveQualificationReport) { r.Canceled = true }},
		{"missing_samples", func(r *liveQualificationReport) { r.Intervals = r.Intervals[1:] }},
		{"missing_hold", func(r *liveQualificationReport) { r.Maintenance = nil }},
		{"nonmonotonic", func(r *liveQualificationReport) { r.Intervals[1].StartNS = 1000 }},
		{"zero_duration", func(r *liveQualificationReport) {
			r.Intervals[0].StartNS = r.Intervals[0].EndNS
			r.Intervals[0].AcquiredNS = r.Intervals[0].EndNS
		}},
		{"acquired_before_start", func(r *liveQualificationReport) { r.Intervals[0].AcquiredNS = 899 }},
		{"wrong_sequence", func(r *liveQualificationReport) { r.Intervals[0].Sequence = 99 }},
		{"wrong_reader", func(r *liveQualificationReport) { r.Intervals[0].Reader = 2 }},
		{"reader_missed", func(r *liveQualificationReport) { r.Intervals[0].Ready = false }},
		{"short_overlap", func(r *liveQualificationReport) { r.MinimumOverlapSamples = 1000 }},
		{"missing_overlap", func(r *liveQualificationReport) {
			r.MinimumOverlapSamples = 1
			for i := 0; i < len(r.Intervals); i += 256 {
				r.Intervals[i].StartNS = 1000
			}
			liveQualificationScoreIntervals(r)
		}},
		{"clock_boundary_overlap", func(r *liveQualificationReport) {
			r.MinimumOverlapSamples = 1
			for i := 0; i < len(r.Intervals); i += 256 {
				r.Intervals[i].AcquiredNS = 1000
			}
			liveQualificationScoreIntervals(r)
		}},
		{"wrong_cardinality", func(r *liveQualificationReport) { r.Witnesses[0].DelayAfter++ }},
		{"unexecuted_sweep", func(r *liveQualificationReport) { r.Witnesses[0].PruneAfterNS = 0 }},
		{"wrong_keys", func(r *liveQualificationReport) { r.Witnesses[0].StateKeysMatched-- }},
		{"wrong_trigger", func(r *liveQualificationReport) { r.Witnesses[0].TriggerCompleted = false }},
		{"fabricated_p99", func(r *liveQualificationReport) { r.LookupP99NS++ }},
		{"fabricated_overlap", func(r *liveQualificationReport) { r.OverlapSamples++ }},
		{"setup_in_elapsed", func(r *liveQualificationReport) { r.ElapsedNS++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := liveQualificationSyntheticReport()
			tc.fault(&r)
			if err := liveQualificationValidateConcurrent(&r); err == nil {
				t.Fatal("faulty evidence was accepted")
			}
		})
	}
}

func TestVOACAPLiveCacheQualificationBlockingWitness(t *testing.T) {
	for _, name := range []string{"block_insert_cache", "block_insert_delay", "block_insert_cache_tied", "block_insert_delay_tied"} {
		r := liveQualificationSyntheticReport()
		r.Case, r.N = name, 50000
		old := 0
		if name == "block_insert_cache_tied" || name == "block_insert_delay_tied" {
			old = 49998
		}
		r.Witnesses[0] = liveQualificationWitness{CacheBefore: 50000, DelayBefore: 50000, CacheAfter: 50000, DelayAfter: 50000, Operations: 2048, SuccessfulOperations: 2048, StateKeysChecked: 100002, StateKeysMatched: 100002, TriggerCompleted: true, OldRemaining: old}
		liveQualificationScoreIntervals(&r)
		if err := liveQualificationValidateConcurrent(&r); err != nil {
			t.Fatalf("%s positive: %v", name, err)
		}
		r.Witnesses[0].OldRemaining++
		if err := liveQualificationValidateConcurrent(&r); err == nil {
			t.Fatal("wrong tied-victim population accepted")
		}
	}
}

func liveQualificationSyntheticSerial(name string, n int) liveQualificationReport {
	passes, batch, ops, expired := liveQualificationSerialShape(name, n)
	count := batch * ops
	if name == "replace_oldest" {
		count *= 2
	}
	r := liveQualificationReport{Case: name, N: n, Scale: 1, Passes: passes, Batch: batch, WorkOperations: ops, Operations: passes * count, Completed: true, Valid: true, ClockSource: "testing.B.Elapsed(shared live benchmark)", ClockFrequencyHz: 10000000, ClockProtocol: testsupport.FixedBenchmarkProtocol}
	for p := 0; p < passes; p++ {
		checked := 2 * n
		if name == "replace_oldest" || name == "delay_touch" {
			checked += 2 * min(n, ops)
		} else if name == "delay_repeat" {
			checked += 2
		} else if strings.HasPrefix(name, "insert_") {
			checked += ops
		}
		prune := int64(0)
		if name == "sweep_none" || expired > 0 {
			prune = liveQualificationNow().UnixNano()
		}
		old := 0
		if strings.HasSuffix(name, "_tied") {
			old = max(0, n-ops)
		}
		r.Witnesses = append(r.Witnesses, liveQualificationWitness{Pass: p, CacheBefore: batch * n, DelayBefore: batch * n, CacheAfter: batch * (n - expired), DelayAfter: batch * (n - expired), Operations: count, SuccessfulOperations: count, ExpectedExpired: batch * expired, PruneAfterNS: prune, StateKeysChecked: batch * checked, StateKeysMatched: batch * checked, OldRemaining: batch * old})
		at := int64(p) * 2000001
		r.WorkIntervals = append(r.WorkIntervals, liveQualificationWorkInterval{Pass: p, StartNS: at, CycleEndNS: at + 1000000, EndNS: at + 1000000})
		r.ElapsedNS += 1000000
	}
	r.NSPerOperation = float64(r.ElapsedNS) / float64(r.Operations)
	return r
}

func TestVOACAPLiveCacheQualificationSerialObserver(t *testing.T) {
	for _, name := range liveQualificationSerialCases {
		for _, n := range []int{1, 64, 10000, 50000} {
			r := liveQualificationSyntheticSerial(name, n)
			if err := liveQualificationValidateSerial(&r); err != nil {
				t.Fatalf("%s/%d: %v", name, n, err)
			}
		}
	}
	for _, tc := range []struct {
		name  string
		fault func(*liveQualificationReport)
	}{
		{"incomplete", func(r *liveQualificationReport) { r.Completed = false }},
		{"wrong_batch", func(r *liveQualificationReport) { r.Batch++ }},
		{"wrong_scale", func(r *liveQualificationReport) { r.Scale = 4 }},
		{"zero_interval", func(r *liveQualificationReport) { r.WorkIntervals[0].EndNS = r.WorkIntervals[0].StartNS }},
		{"clock_reversal", func(r *liveQualificationReport) { r.WorkIntervals[1].StartNS = 0 }},
		{"wrong_sum", func(r *liveQualificationReport) { r.ElapsedNS++ }},
		{"skipped_sweep", func(r *liveQualificationReport) { r.Witnesses[0].PruneAfterNS = 0 }},
		{"shrunk_owner", func(r *liveQualificationReport) { r.Witnesses[0].CacheBefore-- }},
		{"wrong_keys", func(r *liveQualificationReport) { r.Witnesses[0].StateKeysMatched-- }},
		{"wrong_clock", func(r *liveQualificationReport) { r.ClockSource = "time.Since" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := liveQualificationSyntheticSerial("expiry_one", 1)
			tc.fault(&r)
			if liveQualificationValidateSerial(&r) == nil {
				t.Fatal("faulty serial evidence accepted")
			}
		})
	}
}
