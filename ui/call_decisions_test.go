// File role: Checks call-decision labels/help and the existing bounded UI stream
// without starting a terminal, scheduler, or dashboard application goroutine.
package ui

import (
	"strings"
	"testing"
	"time"

	"github.com/rivo/tview"
)

func TestB32CallDecisionPlaceholderAndBoundedStream(t *testing.T) {
	d := &DashboardV2{
		pipelineHdr: newBoxedTextView("Overview"), pipelineQuality: newBoxedTextView("Pipeline Quality"),
		pipelineCorrected: newStreamPanelWithOptions("Call decisions", streamPanelOptions{MaxEvents: 3,
			MaxBytes: 2048, MaxMessageBytes: 256, EvictOnByteLimit: true}, true),
		pipelineHarmonics: newStreamPanel("Harmonics", 3, true),
		scheduler:         newFrameScheduler(tview.NewApplication(), 60, 50*time.Millisecond, nil),
	}
	d.seedPipelinePlaceholders()
	if text := d.pipelineCorrected.SnapshotText(); text != "Call decisions: --" {
		t.Fatalf("call-decision placeholder=%q", text)
	}
	for _, line := range []string{"corrected K1ABC -> K1ABD", "rejected K1ABC -> K1ABD", "retained K1ABC resolver_evidence_loss", "corrected K1ABE -> K1ABD"} {
		d.AppendCall(line)
	}
	text := d.pipelineCorrected.SnapshotText()
	if strings.Contains(text, "corrected K1ABC -> K1ABD") || !strings.Contains(text, "rejected K1ABC -> K1ABD") ||
		!strings.Contains(text, "retained K1ABC resolver_evidence_loss") || !strings.Contains(text, "corrected K1ABE -> K1ABD") {
		t.Fatalf("bounded decision pane did not retain its latest three outcomes: %q", text)
	}
	if title := d.pipelineCorrected.view.baseTitle; title != "Call decisions" {
		t.Fatalf("decision pane title=%q", title)
	}
}

func TestB32CallDecisionHelpSeparatesEvaluationsAndAcceptedCorrections(t *testing.T) {
	outer := buildHelpOverlay().(*tview.Flex)
	column := outer.GetItem(1).(*tview.Flex)
	help := column.GetItem(1).(*tview.TextView).GetText(true)
	for _, phrase := range []string{
		"Call decisions: corrected, rejected, or retained evaluations.",
		"Corrections: accepted call changes since process start.",
		"Shares describe current support. S uses separate recognition.",
	} {
		if !strings.Contains(help, phrase) {
			t.Errorf("keyboard help is missing %q: %s", phrase, help)
		}
	}
}
