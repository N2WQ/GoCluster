package pathreliability

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/internal/voacap"
)

func TestClosedForecastFromRecordsBuildsHourlyWindow(t *testing.T) {
	records := []voacapPredictionRecordForTest{
		{HourUTC: 1, FrequencyMHz: 14.1, FT8SNRDB: -35, VOACAPSNRDBHz: 5, Reliability: 0.72, HasReliability: true},
		{HourUTC: 1, FrequencyMHz: 14.2, FT8SNRDB: -32, VOACAPSNRDBHz: 8, Reliability: 0.81, HasReliability: true},
		{HourUTC: 2, FrequencyMHz: 14.1, FT8SNRDB: -30},
		{HourUTC: 1, FrequencyMHz: 7.1, FT8SNRDB: -10},
	}
	forecast, err := closedForecastFromRecords(testPredictionRecords(records), "20m")
	if err != nil {
		t.Fatalf("closedForecastFromRecords() error: %v", err)
	}
	if len(forecast.Records) != 2 {
		t.Fatalf("forecast records = %d, want 2: %+v", len(forecast.Records), forecast)
	}
	if forecast.Records[0].HourUTC != 1 || forecast.Records[0].FT8SNRDB != -32 || forecast.Records[0].VOACAPSNRDBHz != 8 {
		t.Fatalf("hour 1 should keep the strongest same-hour SNR, got %+v", forecast.Records[0])
	}
	if !forecast.Records[0].HasReqSNRReliability || forecast.Records[0].ReqSNRReliability != 0.81 {
		t.Fatalf("hour 1 should keep REL from the strongest same-hour SNR, got %+v", forecast.Records[0])
	}
	if forecast.Records[1].HourUTC != 2 || forecast.Records[1].FT8SNRDB != -30 {
		t.Fatalf("hour 2 should be retained, got %+v", forecast.Records[1])
	}
}

func TestVOACAPRunnerClosedForecasterDeckUsesSafeLabels(t *testing.T) {
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	forecaster := NewVOACAPRunnerClosedForecaster(cfg)
	deck, err := forecaster.buildDeck(VOACAPClosedJob{
		Request: VOACAPClosedRequest{
			UserGrid: "FN05",
			DXGrid:   "DM81XX",
			Band:     "20m",
		},
		SSN:            147,
		WindowStartUTC: time.Date(2026, time.June, 9, 3, 0, 0, 0, time.UTC),
	}, voacapDeckTransmit)
	if err != nil {
		t.Fatalf("buildDeck() error: %v", err)
	}
	text := string(deck)
	if !strings.Contains(text, "LABEL     TRANSMITTER") || !strings.Contains(text, " RECEIVER") {
		t.Fatalf("fallback deck should use safe alphabetic labels:\n%s", text)
	}
	if strings.Contains(text, "LABEL     FN05") || strings.Contains(text, "DM81XX") {
		t.Fatalf("fallback deck label must not contain grid strings:\n%s", text)
	}
	if !strings.Contains(text, "CIRCUIT   45.50N   079.00W    31.98N   102.04W") {
		t.Fatalf("fallback deck should preserve grid-center coordinates:\n%s", text)
	}
	if !strings.Contains(text, "TIME          1   24    1    1") {
		t.Fatalf("fallback deck should cover the canonical clock day:\n%s", text)
	}
}

func TestVOACAPRunnerClosedForecasterDeckSelectsMethodByDistance(t *testing.T) {
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	forecaster := NewVOACAPRunnerClosedForecaster(cfg)
	tests := []struct {
		name     string
		userGrid string
		dxGrid   string
		want     string
	}{
		{name: "short path", userGrid: "FN31", dxGrid: "FN32", want: "METHOD       20    0"},
		{name: "long path", userGrid: "FN31", dxGrid: "QF56", want: "METHOD       30    0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deck, err := forecaster.buildDeck(VOACAPClosedJob{
				Request: VOACAPClosedRequest{
					UserGrid: tt.userGrid,
					DXGrid:   tt.dxGrid,
					Band:     "20m",
				},
				SSN:            147,
				WindowStartUTC: time.Date(2026, time.June, 9, 3, 0, 0, 0, time.UTC),
			}, voacapDeckTransmit)
			if err != nil {
				t.Fatalf("buildDeck() error: %v", err)
			}
			if text := string(deck); !strings.Contains(text, tt.want) {
				t.Fatalf("fallback deck method mismatch, want %q:\n%s", tt.want, text)
			}
		})
	}
}

func TestVOACAPRunnerClosedForecasterDeckUsesCanonicalClockDay(t *testing.T) {
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	forecaster := NewVOACAPRunnerClosedForecaster(cfg)
	deck, err := forecaster.buildDeck(VOACAPClosedJob{
		Request: VOACAPClosedRequest{
			UserGrid: "FN05",
			DXGrid:   "DM81XX",
			Band:     "20m",
		},
		SSN:            147,
		WindowStartUTC: time.Date(2026, time.June, 9, 17, 0, 0, 0, time.UTC),
	}, voacapDeckTransmit)
	if err != nil {
		t.Fatalf("buildDeck() error: %v", err)
	}
	if text := string(deck); !strings.Contains(text, "TIME          1   24    1    1") {
		t.Fatalf("fallback deck should cover the complete clock day:\n%s", text)
	}
}

func TestVOACAPRunnerClosedForecasterDeckSupportsReceiveDirection(t *testing.T) {
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	forecaster := NewVOACAPRunnerClosedForecaster(cfg)
	deck, err := forecaster.buildDeck(VOACAPClosedJob{
		Request: VOACAPClosedRequest{
			UserGrid: "FN05",
			DXGrid:   "DM81XX",
			Band:     "20m",
		},
		SSN:            147,
		WindowStartUTC: time.Date(2026, time.June, 9, 3, 0, 0, 0, time.UTC),
	}, voacapDeckReceive)
	if err != nil {
		t.Fatalf("buildDeck() error: %v", err)
	}
	if text := string(deck); !strings.Contains(text, "CIRCUIT   31.98N   102.04W    45.50N   079.00W") {
		t.Fatalf("receive-direction deck should reverse circuit coordinates:\n%s", text)
	}
}

func TestVOACAPRunnerClosedForecasterRemovesDirectionalOutputAfterRead(t *testing.T) {
	home := buildFakeForecastEngine(t)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.VOACAPHome = home
	cfg.VOACAPTimeoutSeconds = 5
	cfg.ForecastHours = 2
	cfg.OutputNamePrefix = "gocluster_cleanup"
	cfg.CenterFrequenciesMHz = []float64{14.1}
	forecaster := NewVOACAPRunnerClosedForecaster(cfg)
	forecast, result, err := forecaster.runDirectionalForecast(context.Background(), VOACAPClosedJob{
		Request: VOACAPClosedRequest{UserGrid: "FN31", DXGrid: "FN32", Band: "20m"},
		SSN:     112, WindowStartUTC: time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC), FrequencyMHz: 14.1,
	}, voacapDeckTransmit)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Output) == 0 {
		t.Fatal("expected in-memory VOACAP output bytes")
	}
	if len(forecast.Records) != 1 || forecast.Records[0].HourUTC != 20 || forecast.Records[0].FrequencyMHz != 14.1 {
		t.Fatalf("unexpected parsed forecast: %+v", forecast)
	}
	if _, err := os.Stat(result.OutputPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("runtime output should be removed after read: %v", err)
	}
}

func buildFakeForecastEngine(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	binDir := filepath.Join(home, "bin_win")
	runDir := filepath.Join(home, "run")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run: %v", err)
	}
	source := filepath.Join(home, "fake_voacap.go")
	engine := filepath.Join(binDir, "Voacapw.exe")
	if err := os.WriteFile(source, []byte(fakeCleanupVOACAPEngineSource), 0o644); err != nil {
		t.Fatalf("write fake engine source: %v", err)
	}
	buildCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(buildCtx, "go", "build", "-o", engine, source)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fake engine: %v\n%s", err, out)
	}

	return home
}

func TestCombineDirectionalForecastsRequiresCommonHours(t *testing.T) {
	receive := VOACAPClosedForecast{Records: []VOACAPHourlyForecast{
		{HourUTC: 20, FrequencyMHz: 14.1, FT8SNRDB: -10, VOACAPSNRDBHz: 24, ReqSNRReliability: 0.91, HasReqSNRReliability: true},
		{HourUTC: 21, FrequencyMHz: 14.1, FT8SNRDB: -12, VOACAPSNRDBHz: 22},
	}}
	transmit := VOACAPClosedForecast{Records: []VOACAPHourlyForecast{
		{HourUTC: 20, FrequencyMHz: 14.1, FT8SNRDB: -20, VOACAPSNRDBHz: 14, ReqSNRReliability: 0.84, HasReqSNRReliability: true},
	}}
	combined, err := combineDirectionalForecasts(receive, transmit, "20m")
	if err != nil {
		t.Fatalf("combineDirectionalForecasts() error: %v", err)
	}
	if len(combined.Records) != 1 {
		t.Fatalf("expected one common-hour record, got %d", len(combined.Records))
	}
	record := combined.Records[0]
	if !record.HasDirectionalSNR || record.ReceiveFT8SNRDB != -10 || record.TransmitFT8SNRDB != -20 {
		t.Fatalf("unexpected combined record: %+v", record)
	}
	if !record.HasDirectionalReqSNRReliability || record.ReceiveReqSNRReliability != 0.91 || record.TransmitReqSNRReliability != 0.84 {
		t.Fatalf("unexpected directional REL fields: %+v", record)
	}
	if rel, ok := (VOACAPCachedForecast{Record: record}).ReqSNRReliability(); !ok || rel != 0.84 {
		t.Fatalf("effective REL = %v ok=%v, want 0.84 true", rel, ok)
	}

	_, err = combineDirectionalForecasts(receive, VOACAPClosedForecast{}, "20m")
	if err == nil {
		t.Fatalf("expected error with no common bidirectional hours")
	}
}

func TestVOACAPRunnerClosedForecasterLiveSafeLabelDeck(t *testing.T) {
	if os.Getenv("GOCLUSTER_RUN_LIVE_VOACAP") != "1" {
		t.Skip("set GOCLUSTER_RUN_LIVE_VOACAP=1 to run against local VOACAP")
	}
	home := os.Getenv("GOCLUSTER_VOACAP_TEST_HOME")
	if home == "" {
		t.Fatal("GOCLUSTER_VOACAP_TEST_HOME must identify the isolated test installation")
	}
	cfg := defaultVOACAPFallbackConfig()
	cfg.VOACAPHome = home
	cfg.VOACAPTimeoutSeconds = 120
	cfg.OutputNamePrefix = "gocluster_voacap_v9_live"
	forecaster := NewVOACAPRunnerClosedForecaster(cfg)
	forecast, err := forecaster.ForecastClosed(context.Background(), VOACAPClosedJob{
		Request: VOACAPClosedRequest{
			UserCell: CellID(1020),
			DXCell:   CellID(1476),
			UserGrid: "FN05",
			DXGrid:   "DM81XX",
			Band:     "40m",
			Mode:     "FT8",
		},
		SSN:            147,
		WindowStartUTC: time.Date(2026, time.June, 9, 3, 0, 0, 0, time.UTC),
		FrequencyMHz:   7.1,
	})
	if err != nil {
		t.Fatalf("ForecastClosed() live error: %v", err)
	}
	if len(forecast.Records) == 0 {
		t.Fatalf("live forecast missing hourly records: %+v", forecast)
	}
	if forecast.Records[0].FrequencyMHz <= 0 {
		t.Fatalf("live forecast missing frequency: %+v", forecast)
	}
	if forecast.OutputPath == "" {
		t.Fatalf("live forecast missing output path: %+v", forecast)
	}
}

const fakeCleanupVOACAPEngineSource = `package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintf(os.Stderr, "unexpected args: %v", os.Args)
		os.Exit(3)
	}
	home := os.Args[2]
	outputName := os.Args[4]
	output := " 20.0 0.0 14.1 FREQ\n 0.0 10.0 SNR\n 0.0 0.80 REL\n"
	if _, err := os.Stat(filepath.Join(home, "run", "full-clock")); err == nil {
		deck, err := os.ReadFile(filepath.Join(home, "run", os.Args[3]))
		if err != nil { panic(err) }
		var year int
		var month float64
		direction := 0
		for _, line := range strings.Split(string(deck), "\n") {
			if strings.HasPrefix(line, "MONTH") { fmt.Sscanf(line, "MONTH %d %f", &year, &month) }
			if strings.HasPrefix(line, "CIRCUIT") && strings.Fields(line)[1] == "41.50N" { direction = 1 }
		}
		calls, err := os.OpenFile(filepath.Join(home,"run","calls"),os.O_CREATE|os.O_APPEND|os.O_WRONLY,0600)
		if err != nil { panic(err) }
		fmt.Fprintf(calls,"%d-%02d %d\n",year,int(month),direction)
		if err := calls.Close(); err != nil { panic(err) }
		control, _ := os.ReadFile(filepath.Join(home,"run","control"))
		var b strings.Builder
		for hour:=1;hour<=24;hour++ {
			utc:=hour%24
			if string(control)=="drop-january-midnight" && int(month)==1 && utc==0 { continue }
			snr:=(year-2020)*10000+int(month)*100+direction*50+utc
			fmt.Fprintf(&b," %d.0 0.0 14.1 FREQ\n 0 %d SNR\n 0 0.80 REL\n",hour,snr)
		}
		output=b.String()
	}
	if err := os.WriteFile(filepath.Join(home, "run", outputName), []byte(output), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(5)
	}
}
`

type voacapPredictionRecordForTest struct {
	HourUTC        int
	FrequencyMHz   float64
	FT8SNRDB       int
	VOACAPSNRDBHz  int
	Reliability    float64
	HasReliability bool
}

func testPredictionRecords(in []voacapPredictionRecordForTest) []voacap.PredictionRecord {
	out := make([]voacap.PredictionRecord, 0, len(in))
	for _, record := range in {
		out = append(out, voacap.PredictionRecord{
			HourUTC:        record.HourUTC,
			FrequencyMHz:   record.FrequencyMHz,
			FT8SNRDB:       record.FT8SNRDB,
			VOACAPSNRDBHz:  record.VOACAPSNRDBHz,
			Reliability:    record.Reliability,
			HasReliability: record.HasReliability,
		})
	}
	return out
}
