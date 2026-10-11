//go:build !qualification

// File role: Exercises configured numerical-input failure before runtime
// publication owners start, including partial-startup resource ownership.
package cluster

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/config"
)

const correctionNumericValidModel = `{"modes":["CW"],"snr_band_edges":[-50,50],"alphabet":"AB?","unknown_char":"?","sub_counts":[[[[1,1,1],[1,1,1],[1,1,1]]]],"del_counts":[[[1,1,1]]],"ins_counts":[[[1,1,1]]]}`

func correctionNumericFile(t *testing.T, name, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func correctionNumericRuntime(t *testing.T, c config.CallCorrectionConfig) *clusterRuntime {
	t.Helper()
	c.AdaptiveMinReports = config.AdaptiveMinReportsConfig{Enabled: true, Groups: []config.AdaptiveMinReportsGroup{
		{Name: "fixture", Bands: []string{"20m"}, QuietMinReports: 3, NormalMinReports: 3, BusyMinReports: 3},
	}}
	r := newClusterRuntime(BuildInfo{}, &config.Config{CallCorrection: c}, "", config.LoadDiagnostics{})
	r.ctx, r.cancel = context.WithCancel(t.Context())
	t.Cleanup(r.close)
	return r
}

func TestCorrectionNumericRuntimeRejectsConfiguredAssets(t *testing.T) {
	for _, asset := range []struct {
		name, key string
		set       func(*config.CallCorrectionConfig, string)
	}{
		{"global", "spotter_reliability_file", func(c *config.CallCorrectionConfig, p string) { c.SpotterReliabilityFile = p }},
		{"cw", "spotter_reliability_file_cw", func(c *config.CallCorrectionConfig, p string) { c.SpotterReliabilityFileCW = p }},
		{"rtty", "spotter_reliability_file_rtty", func(c *config.CallCorrectionConfig, p string) { c.SpotterReliabilityFileRTTY = p }},
	} {
		for _, value := range []string{"NaN", "+Inf", "-Inf", "1e309", "missing"} {
			t.Run(asset.name+"/"+value, func(t *testing.T) {
				path := correctionNumericFile(t, "weights.txt", "N0AAA "+value+"\n")
				if value == "missing" {
					path = filepath.Join(t.TempDir(), "not-present.txt")
				}
				// Explicit reliability files are loaded even with correction off.
				c := config.CallCorrectionConfig{AdaptiveRefreshByBand: config.AdaptiveRefreshByBandConfig{Enabled: true}}
				asset.set(&c, path)
				r := correctionNumericRuntime(t, c)
				r.initializeCorrectionModels()
				if r.startupErr == nil || !strings.Contains(r.startupErr.Error(), "call_correction."+asset.key) || !strings.Contains(r.startupErr.Error(), strconv.Quote(path)) {
					t.Errorf("explicit invalid asset must identify its YAML key and path: %v", r.startupErr)
				}
				if r.adaptiveMinReports != nil || r.refresher != nil || r.statsTracker != nil || r.signalResolver != nil || r.recentBandStore != nil || r.spotBuffer != nil || r.outputPipeline != nil {
					t.Error("invalid configured asset created downstream runtime owners")
				}
			})
		}
	}
	for _, tc := range []struct{ name, text string }{
		{"negative_count", strings.Replace(correctionNumericValidModel, "[1,1,1]", "[-1,1,1]", 1)},
		{"overflowing_total", strings.Replace(correctionNumericValidModel, "[1,1,1]", "[9223372036854775807,1,0]", 1)},
		{"nonascending_edges", strings.Replace(correctionNumericValidModel, "[-50,50]", "[50,50]", 1)},
		{"malformed", "{"},
		{"missing", ""},
	} {
		t.Run("confusion/"+tc.name, func(t *testing.T) {
			path := correctionNumericFile(t, "model.json", tc.text)
			if tc.name == "missing" {
				path = filepath.Join(t.TempDir(), "not-present.json")
			}
			r := correctionNumericRuntime(t, config.CallCorrectionConfig{
				ConfusionModelEnabled: true, ConfusionModelFile: path, ConfusionModelWeight: 0,
				AdaptiveRefreshByBand: config.AdaptiveRefreshByBandConfig{Enabled: true},
			})
			r.initializeCorrectionModels()
			if r.startupErr == nil || !strings.Contains(r.startupErr.Error(), "call_correction.confusion_model_file") || !strings.Contains(r.startupErr.Error(), strconv.Quote(path)) {
				t.Errorf("enabled invalid model must fail even at zero weight and identify key/path: %v", r.startupErr)
			}
			if r.confusionModel != nil || r.adaptiveMinReports != nil || r.refresher != nil {
				t.Error("invalid model created scoring or refresher owners")
			}
		})
	}
}

func TestCorrectionNumericRuntimeRawParameterActivity(t *testing.T) {
	model := correctionNumericFile(t, "model.json", correctionNumericValidModel)
	for _, value := range []struct {
		name string
		n    float64
	}{
		{"nan", math.NaN()}, {"positive_infinity", math.Inf(1)}, {"negative_infinity", math.Inf(-1)},
	} {
		for _, parameter := range []string{"min_spotter_reliability", "confusion_model_weight"} {
			for _, active := range []bool{false, true} {
				name := value.name + "/" + parameter + "/inactive"
				if active {
					name = value.name + "/" + parameter + "/active"
				}
				t.Run(name, func(t *testing.T) {
					c := config.CallCorrectionConfig{Enabled: active, ConfusionModelEnabled: true, ConfusionModelFile: model}
					if parameter == "min_spotter_reliability" {
						c.MinSpotterReliability = value.n
					} else {
						c.ConfusionModelWeight = value.n
					}
					r := correctionNumericRuntime(t, c)
					r.initializeCorrectionModels()
					if active {
						if r.startupErr == nil || !strings.Contains(r.startupErr.Error(), "call_correction."+parameter) || r.adaptiveMinReports != nil || r.refresher != nil {
							t.Errorf("active nonfinite raw parameter must fail before downstream owners: err=%v adaptive=%t refresher=%t", r.startupErr, r.adaptiveMinReports != nil, r.refresher != nil)
						}
					} else if r.startupErr != nil || r.confusionModel == nil {
						t.Errorf("unused raw parameter must not disable an independently enabled valid asset: %v", r.startupErr)
					}
				})
			}
		}
	}
}

func TestCorrectionNumericRuntimeValidAndDisabledControls(t *testing.T) {
	weights := correctionNumericFile(t, "weights.txt", "N0AAA -0.2\nN0BBB 2\nN0CCC 0.95\nignored text\n")
	model := correctionNumericFile(t, "model.json", correctionNumericValidModel)
	r := correctionNumericRuntime(t, config.CallCorrectionConfig{
		Enabled: true, SpotterReliabilityFile: weights,
		MinSpotterReliability: -0.2, ConfusionModelEnabled: true, ConfusionModelFile: model, ConfusionModelWeight: 0,
	})
	r.initializeCorrectionModels()
	if r.startupErr != nil || r.confusionModel == nil || r.spotterReliability["N0AAA"] != 0 || r.spotterReliability["N0BBB"] != 1 || r.spotterReliability["N0CCC"] != 0.95 || len(r.spotterReliability) != 3 {
		t.Errorf("valid finite clamping, existing ignored text, or zero-weight model loading changed: err=%v weights=%v model=%t", r.startupErr, r.spotterReliability, r.confusionModel != nil)
	}
	disabled := correctionNumericRuntime(t, config.CallCorrectionConfig{
		Enabled: true, ConfusionModelFile: filepath.Join(t.TempDir(), "missing.json"), ConfusionModelWeight: math.NaN(),
	})
	disabled.initializeCorrectionModels()
	if disabled.startupErr != nil || disabled.confusionModel != nil {
		t.Errorf("disabled model must ignore its unused asset and parameter: %v", disabled.startupErr)
	}
}

func TestCorrectionNumericReferenceFailureAndPartialClose(t *testing.T) {
	for _, order := range []struct {
		name  string
		first int
	}{
		{"ULS_then_Canadian", 0}, {"Canadian_then_ULS", 1},
	} {
		t.Run(order.name, func(t *testing.T) {
			correctionNumericCheckPartialClose(t, order.first)
		})
	}
}

func correctionNumericCheckPartialClose(t *testing.T, first int) {
	t.Helper()
	dir := t.TempDir()
	path := correctionNumericFile(t, "weights.txt", "N0AAA +Inf\n")
	gridPath := correctionNumericFile(t, "grid-not-a-directory", "sentinel")
	cfg := &config.Config{GridDBPath: gridPath, CallCorrection: config.CallCorrectionConfig{
		SpotterReliabilityFile: path, AdaptiveRefreshByBand: config.AdaptiveRefreshByBandConfig{Enabled: true},
		AdaptiveMinReports: config.AdaptiveMinReportsConfig{Enabled: true, Groups: []config.AdaptiveMinReportsGroup{
			{Name: "fixture", Bands: []string{"20m"}, QuietMinReports: 3, NormalMinReports: 3, BusyMinReports: 3},
		}},
	}}
	cfg.FCCULS.DBPath, cfg.FCCULS.Archive = filepath.Join(dir, "fcc.db"), filepath.Join(dir, "fcc.zip")
	cfg.ISED.DBPath, cfg.ISED.Archive, cfg.ISED.SpecialArchive = filepath.Join(dir, "ised.db"), filepath.Join(dir, "ised.zip"), filepath.Join(dir, "ised-special.zip")
	r := newClusterRuntime(BuildInfo{}, cfg, "", config.LoadDiagnostics{})
	r.ctx, r.cancel = context.WithCancel(t.Context())
	mux, err := setupLogging(config.LoggingConfig{Enabled: true, Dir: filepath.Join(dir, "logs")}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	r.logMux = mux
	sink := mux.file.(*dailyFileSink)
	sink.WriteLine("partial startup", time.Now().UTC())
	logHandle := sink.file
	if logHandle == nil {
		t.Fatal("fixture did not open a real runtime-owned log handle")
	}
	initialized := r.initializeReferenceData()
	if initialized || r.startupErr == nil || !strings.Contains(r.startupErr.Error(), "call_correction.spotter_reliability_file") {
		t.Errorf("actual reference-data branch must propagate asset failure: initialized=%t err=%v", initialized, r.startupErr)
	}
	if r.refresher != nil || r.statsTracker != nil || r.spotBuffer != nil || r.signalResolver != nil || r.recentBandStore != nil || r.whoSpotsMeStore != nil || r.stopGridWriter != nil || r.outputPipeline != nil {
		t.Error("reference-data startup continued creating owners after invalid asset")
	}
	// Retain the actual completion channels. Controlled tails make either
	// omitted join observable without a network request or source timing race.
	ready := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	release := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	var releaseOnce [2]sync.Once
	actual := [2]<-chan struct{}{r.ulsDone, r.canadianDone}
	controlled := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	for i := range controlled {
		go func(i int) {
			<-r.ctx.Done()
			<-actual[i]
			close(ready[i])
			<-release[i]
			close(controlled[i])
		}(i)
	}
	r.ulsDone, r.canadianDone = controlled[0], controlled[1]
	closed := make(chan struct{})
	go func() { r.close(); close(closed) }()
	t.Cleanup(func() {
		for i := range release {
			releaseOnce[i].Do(func() { close(release[i]) })
		}
		select {
		case <-closed:
		case <-time.After(3 * time.Second):
			t.Error("partial runtime close did not finish")
		}
	})
	for i := range ready {
		select {
		case <-ready[i]:
		case <-time.After(3 * time.Second):
			t.Fatalf("source owner %d did not observe cancellation and complete its actual work", i)
		}
	}
	releaseOnce[first].Do(func() { close(release[first]) })
	select {
	case <-controlled[first]:
	case <-time.After(3 * time.Second):
		t.Fatal("released source completion tail did not finish")
	}
	select {
	case <-closed:
		t.Errorf("partial close returned while source owner %d remained held", 1-first)
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := logHandle.WriteString("owners still held\n"); err != nil {
		t.Errorf("log handle closed before both source owners completed: %v", err)
	}
	releaseOnce[1-first].Do(func() { close(release[1-first]) })
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("partial runtime close failed to join completed owners")
	}
	if _, err := logHandle.WriteString("after close\n"); !errors.Is(err, os.ErrClosed) {
		t.Errorf("runtime-owned log handle remains open after close: %v", err)
	}
	// Join both fixture tails even if an omitted production join let close
	// return early; no test-owned goroutine may outlive its temporary resources.
	for _, done := range controlled {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("released fixture source tail did not finish")
		}
	}
}
