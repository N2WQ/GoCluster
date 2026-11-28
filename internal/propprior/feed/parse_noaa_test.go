package feed

import (
	"math"
	"testing"
	"time"
)

func TestParseNOAAF107NoonPreferenceIntradayAndFutureRejection(t *testing.T) {
	now := time.Date(2026, 11, 27, 17, 16, 0, 0, time.UTC)
	samples, err := ParseNOAAF107(readTestdata(t, "noaa_f107.json"), now)
	if err != nil {
		t.Fatalf("ParseNOAAF107: %v", err)
	}
	// 2026-11-25: Noon is -999 (rejected), Morning 85.0 selected.
	// 2026-11-26: Afternoon 88, Noon 87, Morning 86 -> Noon 87.0 must win!
	// 2026-11-27: Morning 91.0 (today allowed until Noon posts).
	// 2026-11-28: future day rejected.
	if len(samples) != 3 {
		t.Fatalf("got %d samples (%+v), want 3", len(samples), samples)
	}
	want := []struct {
		day string
		val float64
	}{
		{"2026-11-25T00:00:00Z", 85.0},
		{"2026-11-26T00:00:00Z", 87.0},
		{"2026-11-27T00:00:00Z", 91.0},
	}
	for i, w := range want {
		if got := samples[i].BinStartUTC.Format(time.RFC3339); got != w.day {
			t.Fatalf("sample[%d] day=%s, want %s", i, got, w.day)
		}
		if math.Abs(samples[i].Value-w.val) > 1e-12 {
			t.Fatalf("sample[%d] val=%v, want %v", i, samples[i].Value, w.val)
		}
	}
}

func TestParseSFIFallbackAutoDetectsGFZAndNOAA10cm(t *testing.T) {
	now := time.Date(2026, 11, 27, 17, 16, 0, 0, time.UTC)

	gfzSamples, gfzSrc, err := ParseSFIFallback(readTestdata(t, "gfz_fobs.json"), now)
	if err != nil {
		t.Fatalf("ParseSFIFallback GFZ: %v", err)
	}
	if gfzSrc != "gfz_fobs_fallback" || len(gfzSamples) != 3 {
		t.Fatalf("GFZ fallback got src=%q len=%d", gfzSrc, len(gfzSamples))
	}

	noaaSamples, noaaSrc, err := ParseSFIFallback(readTestdata(t, "noaa_10cm_30d.json"), now)
	if err != nil {
		t.Fatalf("ParseSFIFallback NOAA 10cm: %v", err)
	}
	if noaaSrc != "noaa_10cm_fallback" || len(noaaSamples) != 2 {
		t.Fatalf("NOAA 10cm fallback got src=%q len=%d (%+v)", noaaSrc, len(noaaSamples), noaaSamples)
	}
	if noaaSamples[0].Value != 87 || noaaSamples[1].Value != 88 {
		t.Fatalf("unexpected NOAA 10cm values: %+v", noaaSamples)
	}
}

func TestParseRTSWMagActiveFilterNullAndSignedHourlyMean(t *testing.T) {
	now := time.Date(2026, 11, 27, 17, 16, 0, 0, time.UTC)
	samples, err := ParseRTSWMag(readTestdata(t, "noaa_rtsw_mag_1m.json"), now)
	if err != nil {
		t.Fatalf("ParseRTSWMag: %v", err)
	}
	// 15:00 hour: +4.0 and -2.0 -> signed mean = +1.0 (NOT rectified before mean!)
	// 16:00 hour: IMAP/ACE inactive dropped; 16:30 null dropped; 16:02=0.0 + deduped 16:01=-6.0 -> mean = -3.0
	// 17:00 hour: in-progress at 17:16Z, dropped!
	if len(samples) != 2 {
		t.Fatalf("got %d hourly mag samples (%+v), want 2", len(samples), samples)
	}
	if samples[0].BinStartUTC.Format(time.RFC3339) != "2026-11-27T15:00:00Z" || math.Abs(samples[0].Value-1.0) > 1e-12 {
		t.Fatalf("hour 15 mismatch: %+v", samples[0])
	}
	if samples[1].BinStartUTC.Format(time.RFC3339) != "2026-11-27T16:00:00Z" || math.Abs(samples[1].Value-(-3.0)) > 1e-12 {
		t.Fatalf("hour 16 mismatch: %+v", samples[1])
	}
}

func TestParseRTSWWindActiveFilterAndNullProtonSpeed(t *testing.T) {
	now := time.Date(2026, 11, 27, 17, 16, 0, 0, time.UTC)
	samples, err := ParseRTSWWind(readTestdata(t, "noaa_rtsw_wind_1m.json"), now)
	if err != nil {
		t.Fatalf("ParseRTSWWind: %v", err)
	}
	// 15:00 -> 500.0
	// 16:00 -> active 420.0 + 380.0 (active null dropped, ACE 900.0 inactive dropped) -> 400.0
	// 17:00 -> in-progress dropped
	if len(samples) != 2 {
		t.Fatalf("got %d hourly wind samples (%+v), want 2", len(samples), samples)
	}
	if math.Abs(samples[0].Value-500.0) > 1e-12 || math.Abs(samples[1].Value-400.0) > 1e-12 {
		t.Fatalf("unexpected wind hourly means: %+v", samples)
	}
}

func TestParseKyotoDstDualSchemaAndSentinelRejection(t *testing.T) {
	now := time.Date(2026, 11, 27, 17, 16, 0, 0, time.UTC)

	objSamples, err := ParseKyotoDst(readTestdata(t, "noaa_kyoto_dst_objects.json"), now)
	if err != nil {
		t.Fatalf("ParseKyotoDst objects: %v", err)
	}
	// 14:00 is 9999 (sentinel rejected), 15:00 (-15), 16:00 (-25), 17:00 (in-progress rejected).
	if len(objSamples) != 2 || objSamples[0].Value != -15 || objSamples[1].Value != -25 {
		t.Fatalf("unexpected Kyoto object samples: %+v", objSamples)
	}

	legacySamples, err := ParseKyotoDst(readTestdata(t, "noaa_kyoto_dst_legacy_2d.json"), now)
	if err != nil {
		t.Fatalf("ParseKyotoDst legacy 2D: %v", err)
	}
	if len(legacySamples) != 2 || legacySamples[0].Value != -15 || legacySamples[1].Value != -25 {
		t.Fatalf("unexpected Kyoto legacy samples: %+v", legacySamples)
	}
}

func TestParseGeospaceDstDownsamplesCompletedHoursAndDropsFutureForecast(t *testing.T) {
	now := time.Date(2026, 11, 27, 17, 16, 0, 0, time.UTC)
	samples, err := ParseGeospaceDst(readTestdata(t, "noaa_geospace_dst_7d.json"), now)
	if err != nil {
		t.Fatalf("ParseGeospaceDst: %v", err)
	}
	// 15:00 mean(-10, -20) = -15
	// 16:00 mean(-30, -50) = -40
	// 17:00 (in-progress) and 18:12 (+1h future forecast) MUST be dropped!
	if len(samples) != 2 {
		t.Fatalf("got %d Geospace hourly samples (%+v), want 2", len(samples), samples)
	}
	if samples[0].BinStartUTC.Format(time.RFC3339) != "2026-11-27T15:00:00Z" || math.Abs(samples[0].Value-(-15.0)) > 1e-12 {
		t.Fatalf("hour 15 mismatch: %+v", samples[0])
	}
	if samples[1].BinStartUTC.Format(time.RFC3339) != "2026-11-27T16:00:00Z" || math.Abs(samples[1].Value-(-40.0)) > 1e-12 {
		t.Fatalf("hour 16 mismatch: %+v", samples[1])
	}
}
