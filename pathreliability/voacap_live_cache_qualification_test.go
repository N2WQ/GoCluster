// File role: Opt-in, common-source baseline/candidate live-cache qualification.
// This fixture deliberately uses only APIs available before live indexing. Each
// invocation writes one raw JSON artifact; the external controller owns paired
// process ordering, executable identities, ratio gates, and same-binary controls.
package pathreliability

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

type liveQualificationWitness struct {
	OldRemaining int `json:"old_remaining"`

	Pass                 int   `json:"pass"`
	CacheBefore          int   `json:"cache_before"`
	DelayBefore          int   `json:"delay_before"`
	CacheAfter           int   `json:"cache_after"`
	DelayAfter           int   `json:"delay_after"`
	Operations           int   `json:"operations"`
	SuccessfulOperations int   `json:"successful_operations"`
	ExpectedExpired      int   `json:"expected_expired_per_map"`
	PruneBeforeNS        int64 `json:"prune_before_ns"`
	PruneAfterNS         int64 `json:"prune_after_ns"`
	StateKeysChecked     int   `json:"state_keys_checked"`
	StateKeysMatched     int   `json:"state_keys_matched"`
	ObservedTimestampNS  int64 `json:"observed_timestamp_ns"`
	ExpectedTimestampNS  int64 `json:"expected_timestamp_ns"`
	ForecastRows         int   `json:"forecast_rows"`
	ForecastCapacity     int   `json:"forecast_capacity"`
	TouchedDelays        int   `json:"touched_delays"`
	ExpectedTouched      int   `json:"expected_touched_delays"`
	UnchangedFirstSeen   int   `json:"unchanged_first_seen"`
	ExpectedFirstSeen    int   `json:"expected_first_seen"`
	TriggerCompleted     bool  `json:"trigger_completed"`
}

type liveQualificationInterval struct {
	Pass       int   `json:"pass"`
	Reader     int   `json:"reader"`
	Sequence   int   `json:"sequence"`
	StartNS    int64 `json:"start_ns"`
	AcquiredNS int64 `json:"acquired_ns"`
	EndNS      int64 `json:"end_ns"`
	Ready      bool  `json:"ready"`
}

type liveQualificationMaintenance struct {
	Pass           int   `json:"pass"`
	Sequence       int   `json:"sequence"`
	StartNS        int64 `json:"start_ns"`
	EndNS          int64 `json:"end_ns"`
	RequestStartNS int64 `json:"request_start_ns"`
	RequestEndNS   int64 `json:"request_end_ns"`
}

type liveQualificationWorkInterval struct {
	Pass       int   `json:"pass"`
	StartNS    int64 `json:"start_ns"`
	CycleEndNS int64 `json:"cycle_end_ns"`
	EndNS      int64 `json:"end_ns"`
}

type liveQualificationReport struct {
	Scale          int `json:"scale"`
	Batch          int `json:"batch"`
	WorkOperations int `json:"work_operations"`

	Test                  string                          `json:"test"`
	Case                  string                          `json:"case"`
	N                     int                             `json:"n"`
	Rows                  int                             `json:"rows"`
	Fault                 string                          `json:"fault,omitempty"`
	Operations            int                             `json:"operations"`
	MaintenanceOperations int                             `json:"maintenance_operations"`
	ElapsedNS             int64                           `json:"elapsed_ns"`
	OriginUnixNS          int64                           `json:"origin_unix_ns"`
	NSPerOperation        float64                         `json:"ns_per_operation"`
	Allocations           int64                           `json:"allocations"`
	AllocatedBytes        int64                           `json:"allocated_bytes"`
	HeapBefore            uint64                          `json:"heap_before"`
	HeapAfter             uint64                          `json:"heap_after"`
	HeapDelta             int64                           `json:"heap_delta"`
	OverlapSamples        int                             `json:"overlap_samples"`
	MinimumOverlapSamples int                             `json:"minimum_overlap_samples"`
	OverlapP99NS          int64                           `json:"overlap_p99_ns"`
	LookupP99NS           int64                           `json:"lookup_p99_ns"`
	Witnesses             []liveQualificationWitness      `json:"witnesses"`
	Maintenance           []liveQualificationMaintenance  `json:"maintenance,omitempty"`
	Intervals             []liveQualificationInterval     `json:"intervals,omitempty"`
	Valid                 bool                            `json:"valid"`
	Failure               string                          `json:"failure,omitempty"`
	Passes                int                             `json:"passes"`
	Readers               int                             `json:"readers"`
	ReadsPerReader        int                             `json:"reads_per_reader"`
	ClockSource           string                          `json:"clock_source"`
	ClockFrequencyHz      int64                           `json:"clock_frequency_hz"`
	ClockProtocol         string                          `json:"clock_protocol"`
	WorkIntervals         []liveQualificationWorkInterval `json:"work_intervals,omitempty"`
	CycleNS               int64                           `json:"cycle_ns"`
	CycleP99NS            int64                           `json:"cycle_p99_ns"`
	TriggerNS             int64                           `json:"trigger_ns"`
	TriggerP99NS          int64                           `json:"trigger_p99_ns"`
	Completed             bool                            `json:"completed"`
	Canceled              bool                            `json:"canceled"`
}

type liveQualificationMatrixReport struct {
	Test       string                    `json:"test"`
	Order      string                    `json:"order"`
	Valid      bool                      `json:"valid"`
	Reports    []liveQualificationReport `json:"reports"`
	SerialOnly bool                      `json:"serial_only"`
}

func liveQualificationNow() time.Time {
	return time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
}

func liveQualificationKeys(id int) (voacapCacheKey, voacapDelayKey) {
	key := voacapCacheKey{
		userCell: CellID(1 + id/60000), dxCell: CellID(1 + id%60000),
		band: "20m", frequencyKHz: 14100, year: 2026, month: 6,
		ssn: 112, direction: voacapDirectionBidirectional,
	}
	return key, voacapDelayKey{
		userCell: key.userCell, dxCell: key.dxCell, band: key.band,
		frequencyKHz: key.frequencyKHz, direction: key.direction,
	}
}

func liveQualificationEntry(at time.Time, rows int) voacapCacheEntry {
	records := make([]VOACAPHourlyForecast, rows)
	for i := range records {
		records[i] = VOACAPHourlyForecast{
			HourUTC: (20 + i) % 24, FrequencyMHz: 14.1, FT8SNRDB: -10,
			VOACAPSNRDBHz: 24, ReqSNRReliability: 0.9, HasReqSNRReliability: true,
		}
	}
	return voacapCacheEntry{storedAt: at, forecast: VOACAPClosedForecast{
		WindowStartUTC: liveQualificationNow(), Records: records,
	}}
}

// Every payload has its own compact backing array, including the 24-row case.
func liveQualificationSeed(t *testing.T, n, expired, rows, extra int) *VOACAPClosedFallback {
	t.Helper()
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.MaxCacheEntries = n + extra
	cfg.VOACAPFallback.MaxDelayEntries = n + extra
	cfg.VOACAPFallback.DelaySeconds = 600
	cfg.VOACAPFallback.CacheTTLSeconds = 28800
	cfg.VOACAPFallback.ForecastHours = 8
	f := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})
	now := liveQualificationNow()
	for i := 0; i < n+extra; i++ {
		at := now.Add(time.Duration(i))
		if i < expired {
			at = now.Add(-time.Duration(28800+600+1) * time.Second)
		}
		key, delay := liveQualificationKeys(i)
		f.addCacheLocked(key, liveQualificationEntry(at, rows))
		f.addDelayLocked(delay, voacapDelayEntry{firstSeen: at, lastSeen: at})
	}
	return f
}

func liveQualificationInput(t *testing.T) (string, int, string) {
	t.Helper()
	name, output := os.Getenv("GOCLUSTER_LIVE_CASE"), os.Getenv("GOCLUSTER_LIVE_OUTPUT")
	if name == "" || output == "" || (name != "matrix" && os.Getenv("GOCLUSTER_LIVE_N") == "") {
		t.Skip("set GOCLUSTER_LIVE_CASE, GOCLUSTER_LIVE_N, and GOCLUSTER_LIVE_OUTPUT")
	}
	if name == "matrix" {
		return name, 0, output
	}
	n, err := strconv.Atoi(os.Getenv("GOCLUSTER_LIVE_N"))
	if err != nil || (n != 1 && n != 64 && n != 10000 && n != 50000) {
		t.Fatal("GOCLUSTER_LIVE_N must be 1, 64, 10000, or 50000")
	}
	return name, n, output
}

func liveQualificationWrite(t *testing.T, output string, report liveQualificationReport) {
	t.Helper()
	if err := liveQualificationWriteJSON(output, report); err != nil {
		t.Fatal(err)
	}
	if !report.Valid {
		t.Fatal(report.Failure)
	}
}

// Streaming avoids a second full-size JSON allocation for high-population raw
// samples. Compression changes storage only; every original sample is retained.
func liveQualificationWriteJSON(output string, value any) (err error) {
	file, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
	}()
	if strings.HasSuffix(output, ".gz") {
		writer := gzip.NewWriter(file)
		if err = liveQualificationEncode(writer, value); err != nil {
			_ = writer.Close()
			return err
		}
		return writer.Close()
	}
	return liveQualificationEncode(file, value)
}

func liveQualificationEncode(writer io.Writer, value any) error {
	switch value := value.(type) {
	case liveQualificationReport:
		metadata := value
		metadata.Intervals, metadata.Maintenance, metadata.WorkIntervals, metadata.Witnesses = nil, nil, nil, nil
		encoded, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &fields); err != nil {
			return err
		}
		delete(fields, "witnesses")
		encoded, err = json.Marshal(fields)
		if err != nil {
			return err
		}
		if _, err := writer.Write(encoded[:len(encoded)-1]); err != nil {
			return err
		}
		if _, err := io.WriteString(writer, ",\"witnesses\":"); err != nil {
			return err
		}
		if err := liveQualificationEncodeArray(writer, value.Witnesses); err != nil {
			return err
		}
		if len(value.Intervals) > 0 {
			if _, err := io.WriteString(writer, ",\"intervals\":"); err != nil {
				return err
			}
			if err := liveQualificationEncodeArray(writer, value.Intervals); err != nil {
				return err
			}
		}
		if len(value.Maintenance) > 0 {
			if _, err := io.WriteString(writer, ",\"maintenance\":"); err != nil {
				return err
			}
			if err := liveQualificationEncodeArray(writer, value.Maintenance); err != nil {
				return err
			}
		}
		if len(value.WorkIntervals) > 0 {
			if _, err := io.WriteString(writer, ",\"work_intervals\":"); err != nil {
				return err
			}
			if err := liveQualificationEncodeArray(writer, value.WorkIntervals); err != nil {
				return err
			}
		}
		_, err = io.WriteString(writer, "}\n")
		return err
	case liveQualificationMatrixReport:
		metadata := struct {
			Test       string `json:"test"`
			Order      string `json:"order"`
			Valid      bool   `json:"valid"`
			SerialOnly bool   `json:"serial_only"`
		}{value.Test, value.Order, value.Valid, value.SerialOnly}
		encoded, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		if _, err := writer.Write(encoded[:len(encoded)-1]); err != nil {
			return err
		}
		if _, err := io.WriteString(writer, ",\"reports\":["); err != nil {
			return err
		}
		for i, report := range value.Reports {
			if i > 0 {
				if _, err := io.WriteString(writer, ","); err != nil {
					return err
				}
			}
			if err := liveQualificationEncode(writer, report); err != nil {
				return err
			}
		}
		_, err = io.WriteString(writer, "]}\n")
		return err
	default:
		return json.NewEncoder(writer).Encode(value)
	}
}

func liveQualificationEncodeArray[T any](writer io.Writer, values []T) error {
	if _, err := io.WriteString(writer, "["); err != nil {
		return err
	}
	encoder := json.NewEncoder(writer)
	for i, value := range values {
		if i > 0 {
			if _, err := io.WriteString(writer, ","); err != nil {
				return err
			}
		}
		if err := encoder.Encode(value); err != nil {
			return err
		}
	}
	_, err := io.WriteString(writer, "]")
	return err
}

// The common fixture deliberately permits either tied oldest victim. Exact
// candidate victim identities belong to the separate policy regression tests.
func TestVOACAPLiveCacheQualification(t *testing.T) {
	name, n, output := liveQualificationInput(t)
	if name == "matrix" {
		liveQualificationMatrix(t, output)
		return
	}
	report := liveQualificationReport{Test: "dxcluster/pathreliability.TestVOACAPLiveCacheQualification", Case: name, N: n, Rows: 8, Valid: true}
	if liveQualificationConcurrent(name) {
		liveQualificationOverlap(t, &report)
	} else {
		liveQualificationPerformance(t, &report)
	}
	liveQualificationWrite(t, output, report)
}

func liveQualificationMatrix(t *testing.T, output string) {
	matrix := liveQualificationMatrixReport{Test: "dxcluster/pathreliability.TestVOACAPLiveCacheQualification", Order: os.Getenv("GOCLUSTER_LIVE_ORDER"), Valid: true, SerialOnly: true}
	if matrix.Order == "" {
		matrix.Order = "forward"
	}
	if matrix.Order != "forward" && matrix.Order != "reverse" {
		t.Fatal("invalid matrix order")
	}
	for _, n := range []int{1, 64, 10000, 50000} {
		for _, name := range liveQualificationSerialCases {
			matrix.Reports = append(matrix.Reports, liveQualificationReport{Test: matrix.Test, Case: name, N: n, Rows: 8, Valid: true})
		}
	}
	if matrix.Order == "reverse" {
		for i, j := 0, len(matrix.Reports)-1; i < j; i, j = i+1, j-1 {
			matrix.Reports[i], matrix.Reports[j] = matrix.Reports[j], matrix.Reports[i]
		}
	}
	for i := range matrix.Reports {
		liveQualificationPerformance(t, &matrix.Reports[i])
		matrix.Valid = matrix.Valid && matrix.Reports[i].Valid
	}
	if err := liveQualificationWriteJSON(output, matrix); err != nil {
		t.Fatal(err)
	}
	if !matrix.Valid {
		t.Fatal("qualification matrix contains invalid evidence")
	}
}
