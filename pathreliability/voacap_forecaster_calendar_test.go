package pathreliability

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestVOACAPForecasterOwnsAbsoluteCalendarHours(t *testing.T) {
	home := buildFakeForecastEngine(t)
	if err := os.WriteFile(filepath.Join(home, "run", "full-clock"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.VOACAPHome, cfg.ForecastHours, cfg.VOACAPTimeoutSeconds = home, 24, 5
	cfg.CenterFrequenciesMHz = []float64{14.1}
	f := NewVOACAPRunnerClosedForecaster(cfg)
	cases := []struct {
		name                string
		start               time.Time
		calls               int
		first, second, last int
	}{
		{"exclusive_month_end", time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC), 2, 61200, 61201, 61223},
		{"year_crossing", time.Date(2026, 12, 31, 23, 0, 0, 0, time.UTC), 4, 61223, 70100, 70122},
		{"ordinary_midnight", time.Date(2026, 11, 15, 23, 0, 0, 0, time.UTC), 2, 61123, 61100, 61122},
		{"nonleap", time.Date(2027, 2, 28, 23, 0, 0, 0, time.UTC), 4, 70223, 70300, 70322},
		{"leap_day", time.Date(2028, 2, 28, 23, 0, 0, 0, time.UTC), 2, 80223, 80200, 80222},
		{"leap_month_end", time.Date(2028, 2, 29, 23, 0, 0, 0, time.UTC), 4, 80223, 80300, 80322},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(filepath.Join(home, "run", "calls"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			forecast, err := f.ForecastClosed(context.Background(), VOACAPClosedJob{Request: VOACAPClosedRequest{UserGrid: "FN31", DXGrid: "QF56", Band: "20m"}, SSN: 0, WindowStartUTC: tc.start})
			if err != nil {
				t.Fatal(err)
			}
			if !forecast.WindowStartUTC.Equal(tc.start) || len(forecast.Records) != 24 {
				t.Fatalf("wrong window: %+v", forecast)
			}
			for i, record := range forecast.Records {
				at := tc.start.Add(time.Duration(i) * time.Hour)
				want := (at.Year()-2020)*10000 + int(at.Month())*100 + at.Hour()
				if record.HourUTC != at.Hour() || record.ReceiveVOACAPSNRDBHz != want || record.TransmitVOACAPSNRDBHz != want+50 {
					t.Fatalf("hour %d got %+v; absolute %s want RX%d TX%d", i, record, at, want, want+50)
				}
			}
			if forecast.Records[0].ReceiveVOACAPSNRDBHz != tc.first || forecast.Records[1].ReceiveVOACAPSNRDBHz != tc.second || forecast.Records[23].ReceiveVOACAPSNRDBHz != tc.last {
				t.Fatal("literal calendar witnesses differ")
			}
			calls, err := os.ReadFile(filepath.Join(home, "run", "calls"))
			if err != nil {
				t.Fatal(err)
			}
			if got := len(strings.Fields(string(calls))) / 2; got != tc.calls {
				t.Fatalf("invocations=%d want%d: %s", got, tc.calls, calls)
			}
		})
	}
	if err := os.WriteFile(filepath.Join(home, "run", "control"), []byte("drop-january-midnight"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := f.ForecastClosed(context.Background(), VOACAPClosedJob{Request: VOACAPClosedRequest{UserGrid: "FN31", DXGrid: "QF56", Band: "20m"}, SSN: 73, WindowStartUTC: cases[1].start})
	if err == nil || !strings.Contains(err.Error(), "missing bidirectional") {
		t.Fatalf("incomplete new-month output accepted: %v", err)
	}
}
