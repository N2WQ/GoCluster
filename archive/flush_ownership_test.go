package archive

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

// The expected values are literal storage expectations, checked before
// decodeSpot/Recent can normalize the decoded values and conceal a writer error.
func TestFlushPreservesOwnedInputAndRecordBytes(t *testing.T) {
	cases := []struct {
		name string
		edit func(*spot.Spot)
		want archiveRecord
	}{
		{name: "raw", want: ownedFlushRecord()},
		{name: "partial", edit: func(s *spot.Spot) {
			s.DXCallNorm = "K2CACHE/P"
			s.DEGridNorm = "IO91ZZ"
		}, want: func() archiveRecord {
			r := ownedFlushRecord()
			r.dxCall, r.deGrid = "K2CACHE/P", "IO91ZZ"
			return r
		}()},
		{name: "normalized", edit: func(s *spot.Spot) { s.EnsureNormalized() }, want: ownedFlushRecord()},
		{name: "populated_caches", edit: func(s *spot.Spot) {
			s.DXCallNorm, s.DECallNorm, s.DECallNormStripped = "K2CACHE/P", "W2CACHE-2", "W2CACHE"
			s.ModeNorm, s.BandNorm = "RTTY", "40m"
			s.DXGridNorm, s.DEGridNorm = "IO91ZZ", "JO32AA"
		}, want: func() archiveRecord {
			r := ownedFlushRecord()
			r.dxCall, r.deCall, r.deCallStripped = "K2CACHE/P", "W2CACHE-2", "W2CACHE"
			r.mode, r.band, r.dxGrid, r.deGrid = "RTTY", "40m", "IO91ZZ", "JO32AA"
			return r
		}()},
		{name: "nonempty_whitespace_cache", edit: func(s *spot.Spot) {
			s.DXCallNorm, s.DECallNorm, s.ModeNorm, s.BandNorm = " ", " ", " ", " "
			s.DXGridNorm, s.DEGridNorm = " ", " "
		}, want: func() archiveRecord {
			r := ownedFlushRecord()
			// EnsureNormalized assigns the nonempty DX cache to DXCall. The
			// other whitespace caches fall back to their raw fields at encoding.
			r.dxCall, r.deCall, r.mode, r.band = "", "w1xyz-1", "PSK31", "20M"
			r.dxGrid, r.deGrid = "fn31aa", "em10bb"
			return r
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newCleanupTestWriter(t, 3600)
			defer w.Stop()
			s := ownedFlushInput()
			if tc.edit != nil {
				tc.edit(s)
			}
			s.SealForAsync()
			before := ownedFlushPublicFields(s)
			w.flush([]*spot.Spot{nil, s})
			if after := ownedFlushPublicFields(s); !reflect.DeepEqual(after, before) {
				t.Errorf("archive flush mutated its immutable handoff: before=%v after=%v", before, after)
			}
			if s.SnapshotForAsync() != s {
				t.Error("flush changed sealed ownership")
			}
			raw, closer, err := w.db.Get(spotKeyBytes(s.Time.UnixNano(), 1))
			if err != nil {
				t.Fatal(err)
			}
			defer closer.Close()
			if want := ownedFlushExpectedBytes(tc.want); !bytes.Equal(raw, want) {
				t.Errorf("persisted version-6 bytes differ: got %x want %x", raw, want)
			}
			rec, err := decodeRecord(raw)
			if err != nil {
				t.Fatal(err)
			}
			if rec != tc.want {
				t.Errorf("persisted record before read normalization: got %+v want %+v", rec, tc.want)
			}
		})
	}
}

func TestFlushNormalizedOwnedInputConcurrentReaders(t *testing.T) {
	w := newCleanupTestWriter(t, 3600)
	defer w.Stop()
	s := ownedFlushInput()
	s.EnsureNormalized()
	s.SealForAsync()
	before := ownedFlushPublicFields(s)
	start := make(chan struct{})
	readDone := make(chan bool, 1)
	go func() {
		<-start
		stable := true
		for i := 0; i < 8192; i++ {
			// Read the same exported identity the peer formatter reads. Even
			// assigning an identical value after handoff violates ownership.
			stable = stable && s.DXCall == "K1ABC"
			if i%32 == 0 {
				runtime.Gosched()
			}
		}
		readDone <- stable
	}()
	batch := make([]*spot.Spot, 128)
	for i := range batch {
		batch[i] = s
	}
	close(start)
	w.flush(batch)
	if !<-readDone {
		t.Error("concurrent reader observed changed identity")
	}
	if !reflect.DeepEqual(before, ownedFlushPublicFields(s)) {
		t.Error("flush mutated normalized input")
	}
}

func TestOwnedQueueDropsFullAndStopDrainsMultipleBatches(t *testing.T) {
	cfg := config.ArchiveConfig{
		Enabled: true, DBPath: filepath.Join(t.TempDir(), "archive.db"),
		QueueSize: 5, BatchSize: 2, BatchIntervalMS: 60000,
		CleanupIntervalSeconds: 3600, RetentionSeconds: 3600, Synchronous: "off",
	}
	w, err := NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	inputs := make([]*spot.Spot, 6)
	before := make([]map[string]any, len(inputs))
	for i := range inputs {
		inputs[i] = ownedFlushInput()
		inputs[i].Comment = fmt.Sprintf("queued-%d", i)
		inputs[i].SealForAsync()
		before[i] = ownedFlushPublicFields(inputs[i])
		w.EnqueueOwned(inputs[i])
	}
	if len(w.queue) != 5 {
		t.Fatalf("full queue changed admission: got %d, want 5", len(w.queue))
	}
	// A paused queue gives an exact pointer-identity check before the worker
	// starts, then returns the accepted rows in their original order.
	queued := make([]*spot.Spot, 5)
	for i := range queued {
		queued[i] = <-w.queue
		if queued[i] != inputs[i] {
			t.Error("owned enqueue copied the accepted snapshot")
		}
	}
	for _, s := range queued {
		w.queue <- s
	}
	w.Start()
	w.Stop()
	for i, s := range inputs {
		if !reflect.DeepEqual(before[i], ownedFlushPublicFields(s)) {
			t.Errorf("accepted or dropped handoff %d changed", i)
		}
	}
	w.EnqueueOwned(inputs[5])
	if len(w.queue) != 0 {
		t.Fatal("stopped writer admitted a row")
	}
	reopened, err := NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Stop()
	iter, err := reopened.db.NewIter(spotIterOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	seen := make(map[string]bool)
	for valid := iter.First(); valid; valid = iter.Next() {
		r, err := decodeRecord(iter.Value())
		if err != nil {
			t.Fatal(err)
		}
		seen[r.comment] = true
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 5 || seen["queued-5"] {
		t.Fatalf("Stop lost accepted rows or stored a dropped row: %v", seen)
	}
	for i := 0; i < 5; i++ {
		if !seen[fmt.Sprintf("queued-%d", i)] {
			t.Errorf("accepted row %d missing after reopening", i)
		}
	}
}

func ownedFlushInput() *spot.Spot {
	return &spot.Spot{
		DXCall: " k1abc-2 ", DECall: " w1xyz-1 ", DECallStripped: " w1xyz ",
		Frequency: 14074.5, ObservedFrequency: 14074.53, Mode: " psk31 ", Band: " 20M ",
		Time:   time.Date(2026, 10, 10, 12, 0, 0, 123, time.UTC),
		Report: -12, HasReport: true, IsHuman: true, TTL: 3,
		Comment: " POTA reference ", Events: spot.EventPOTA,
		SourceType: spot.SourceUpstream, SourceNode: " N0NODE ",
		Confidence: " P ", ConfidencePercent: 64, ConfidencePercentOK: true,
		DXMetadata:     spot.CallMetadata{Grid: " fn31aa ", Continent: " na ", State: "MA", CQZone: 5, ADIF: 291, GridDerived: true},
		DEMetadata:     spot.CallMetadata{Grid: " em10bb ", Continent: " eu ", State: "ON", CQZone: 14, ADIF: 1},
		ToxicityStatus: spot.ToxicitySafe, ToxicityCategories: []string{"category-a", "category-b"}, ToxicityModel: " model-x ",
	}
}

func ownedFlushRecord() archiveRecord {
	return archiveRecord{
		dxCall: "K1ABC", deCall: "W1XYZ-1", deCallStripped: "W1XYZ",
		mode: "PSK", band: "20m", dxGrid: "FN31AA", deGrid: "EM10BB",
		dxCont: "NA", deCont: "EU", dxState: "MA", deState: "ON", dxGridDerived: true,
		comment: "POTA reference", events: spot.EventPOTA, source: "UPSTREAM", sourceNode: "N0NODE", confidence: "P",
		freq: 14074.5, observedFreq: 14074.53, report: -12, hasReport: true, isHuman: true, ttl: 3,
		dxCQZone: 5, deCQZone: 14, dxADIF: 291, deADIF: 1,
		toxicityStatus: "SAFE", toxicityCategories: "category-a,category-b", toxicityModel: "model-x",
	}
}

// Capture every exported field without copying Spot's embedded sync.Once or
// aliasing its category slice. Formatting internals are not writer-owned state.
func ownedFlushPublicFields(s *spot.Spot) map[string]any {
	v := reflect.ValueOf(s).Elem()
	fields := make(map[string]any)
	for i := 0; i < v.NumField(); i++ {
		if !v.Type().Field(i).IsExported() {
			continue
		}
		value := v.Field(i).Interface()
		if categories, ok := value.([]string); ok {
			value = append([]string(nil), categories...)
		}
		fields[v.Type().Field(i).Name] = value
	}
	return fields
}

// Build the expected version-6 layout independently of encodeRecord. Field
// values come from the literal oracle above, including the explicit POTA token.
func ownedFlushExpectedBytes(r archiveRecord) []byte {
	fields := []string{r.dxCall, r.deCall, r.deCallStripped, r.mode, r.comment,
		r.source, r.sourceNode, r.confidence, r.band, r.dxGrid, r.deGrid,
		r.dxCont, r.deCont, "POTA", r.toxicityStatus, r.toxicityCategories, r.toxicityModel, r.dxState, r.deState}
	raw := make([]byte, 74)
	raw[0], raw[1], raw[2] = 6, 7, 3 // report, human and DX-derived-grid flags
	binary.BigEndian.PutUint64(raw[4:12], math.Float64bits(14074.5))
	binary.BigEndian.PutUint32(raw[12:16], 0xfffffff4) // signed -12 dB
	binary.BigEndian.PutUint16(raw[16:18], 5)
	binary.BigEndian.PutUint16(raw[18:20], 14)
	binary.BigEndian.PutUint32(raw[20:24], 291)
	binary.BigEndian.PutUint32(raw[24:28], 1)
	binary.BigEndian.PutUint64(raw[28:36], math.Float64bits(14074.53))
	for i, field := range fields {
		binary.BigEndian.PutUint16(raw[36+i*2:38+i*2], uint16(len(field)))
	}
	for _, field := range fields {
		raw = append(raw, field...)
	}
	return raw
}
