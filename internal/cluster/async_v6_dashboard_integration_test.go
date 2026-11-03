//go:build dashboard_integration

package cluster

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/asynclog"
	"dxcluster/ui"

	"github.com/gdamore/tcell/v2"
)

func TestV6RealDashboardKeepsConfiguredMessageLimit(t *testing.T) {
	root := t.TempDir()
	var console bytes.Buffer
	l, err := newV6LogRuntime(&console)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.LoggingConfig{Enabled: true, Dir: filepath.Join(root, "system"), RetentionDays: 7}
	l.startFiles(cfg, nil, true)
	uiCfg := config.UIConfig{V2: config.UIV2Config{Pages: []string{"events"}, TargetFPS: 60,
		DebugBuffer: config.UIV2BufferConfig{MaxEvents: 32, MaxBytesMB: 1, MaxMessageBytes: 4096, EvictOnByteLimit: true}}}
	screen := tcell.NewSimulationScreen("UTF-8")
	var dashboard *ui.DashboardV2
	surface, err := newV6SurfaceWithFactory(func() *ui.DashboardV2 {
		dashboard = ui.IntegrationDashboard(uiCfg, screen)
		screen.SetSize(120, 48)
		return dashboard
	}, uiCfg)
	if err != nil {
		l.close(time.Now().Add(time.Second))
		t.Fatal(err)
	}
	l.fanout.setConsole(v6DashboardAdmission{surface: surface})
	line3000 := strings.Repeat("a", 3000)
	line4097 := strings.Repeat("b", 4097)
	lineHuge := strings.Repeat("h", asynclog.MaxMessageBytes+1)
	multiRow := strings.Repeat("m", 1000)
	largeMultiline := strings.Repeat(multiRow+"\n", 8) + strings.Repeat("z", asynclog.MaxMessageBytes)
	_, _ = l.fanout.Write([]byte(line3000 + "\n"))
	_, _ = l.fanout.Write([]byte(line4097 + "\n"))
	surface.AppendSystem(lineHuge)
	_, _ = l.fanout.Write([]byte(lineHuge + "\n"))
	_, _ = l.fanout.Write([]byte(largeMultiline))
	edgeRows := strings.Repeat("e", asynclog.MaxMessageBytes-1025) + "\n" + strings.Repeat("q", 10000) + "\n"
	_, _ = l.fanout.Write([]byte(edgeRows))
	_, _ = l.fanout.Write([]byte("edge-control\n"))
	_, _ = l.fanout.Write([]byte("dashboard-control\n"))
	_, _ = l.fanout.WriteStream([]byte("partial"))
	_, _ = l.fanout.WriteStream([]byte(" line\r\nnext line\n"))
	deadline := time.Now().Add(5 * time.Second)
	for {
		rows, truncated := ui.IntegrationDashboardEvents(dashboard)
		if slices.Contains(rows, line3000) && slices.Contains(rows, strings.Repeat("b", 4081)+"... [truncated]") && slices.Contains(rows, strings.Repeat("h", 4081)+"... [truncated]") && slices.Contains(rows, multiRow) && slices.Contains(rows, strings.Repeat("z", 4081)+"... [truncated]") && slices.Contains(rows, "edge-control") && slices.Contains(rows, "dashboard-control") && slices.Contains(rows, "partial line") && slices.Contains(rows, "next line") && truncated == 5 {
			if slices.Contains(rows, strings.Repeat("q", 1024)) {
				t.Fatal("an incomplete row was displayed without truncation")
			}
			break
		}
		if time.Now().After(deadline) {
			surface.beginStop()
			l.close(time.Now().Add(time.Second))
			t.Fatalf("dashboard rows=%d truncated=%d has3000=%t has4097=%t hasHuge=%t hasMulti=%t hasTail=%t hasEdge=%t hasControl=%t hasPartial=%t hasNext=%t", len(rows), truncated,
				slices.Contains(rows, line3000), slices.Contains(rows, strings.Repeat("b", 4081)+"... [truncated]"),
				slices.Contains(rows, strings.Repeat("h", 4081)+"... [truncated]"), slices.Contains(rows, multiRow),
				slices.Contains(rows, strings.Repeat("z", 4081)+"... [truncated]"), slices.Contains(rows, "edge-control"),
				slices.Contains(rows, "dashboard-control"), slices.Contains(rows, "partial line"), slices.Contains(rows, "next line"))
		}
		time.Sleep(5 * time.Millisecond)
	}
	runtime := &clusterRuntime{surface: surface, v6Logging: l}
	runtime.closeV6Logging(time.Now().Add(2 * time.Second))
	select {
	case <-surface.Done():
	default:
		t.Fatal("runtime returned before the healthy dashboard stopped")
	}
	data, err := os.ReadFile(filepath.Join(root, "system", "system.log"))
	if err != nil || !strings.Contains(string(data), "dashboard-control") || strings.Contains(string(data), strings.Repeat("a", 40)) {
		t.Fatalf("file output %q: %v", data, err)
	}
}

func TestV6RealDashboardDropsAboveQueueCapacity(t *testing.T) {
	var console bytes.Buffer
	l, err := newV6LogRuntime(&console)
	if err != nil {
		t.Fatal(err)
	}
	uiCfg := config.UIConfig{V2: config.UIV2Config{Pages: []string{"events"}, TargetFPS: 60,
		DebugBuffer: config.UIV2BufferConfig{MaxEvents: 8, MaxBytesMB: 2, MaxMessageBytes: 1 << 20, EvictOnByteLimit: true}}}
	screen := tcell.NewSimulationScreen("UTF-8")
	var dashboard *ui.DashboardV2
	surface, err := newV6SurfaceWithFactory(func() *ui.DashboardV2 {
		dashboard = ui.IntegrationDashboard(uiCfg, screen)
		screen.SetSize(120, 48)
		return dashboard
	}, uiCfg)
	if err != nil {
		l.close(time.Now().Add(time.Second))
		t.Fatal(err)
	}
	defer func() {
		(&clusterRuntime{surface: surface, v6Logging: l}).closeV6Logging(time.Now().Add(5 * time.Second))
	}()
	l.fanout.setConsole(v6DashboardAdmission{surface: surface})
	waitRow := func(want string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			rows, _ := ui.IntegrationDashboardEvents(dashboard)
			if slices.Contains(rows, want) {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("dashboard did not retain expected %d-byte row", len(want))
	}
	atCapacity := strings.Repeat("a", asynclog.MaxMessageBytes)
	surface.AppendSystem(atCapacity)
	waitRow(atCapacity)
	tooLarge := strings.Repeat("b", asynclog.MaxMessageBytes+1)
	surface.AppendSystem(tooLarge)
	surface.AppendSystem("capacity-control")
	waitRow("capacity-control")
	rows, _ := ui.IntegrationDashboardEvents(dashboard)
	if slices.Contains(rows, tooLarge) || surface.handles[5].Stats().Dropped == 0 {
		t.Fatal("oversized message was not dropped at the queue boundary")
	}
}
