package main

import (
	"testing"

	"dxcluster/spot"
)

func TestCloneSpotForBroadcastPreservesHasReportAndReport(t *testing.T) {
	src := spot.NewSpot("GM4YXI", "EA1TX", 144360.0, "MSK144")
	src.Report = 0
	src.HasReport = true

	clone := cloneSpotForBroadcast(src)
	if clone == nil {
		t.Fatalf("expected clone spot, got nil")
	}
	if clone == src {
		t.Fatalf("expected clone to be a new instance")
	}
	if clone.Report != src.Report {
		t.Fatalf("expected Report=%d, got %d", src.Report, clone.Report)
	}
	if clone.HasReport != src.HasReport {
		t.Fatalf("expected HasReport=%v, got %v", src.HasReport, clone.HasReport)
	}
}

func TestCloneSpotForBroadcastPreservesMissingReport(t *testing.T) {
	src := spot.NewSpot("GM4YXI", "EA1TX", 144360.0, "MSK144")
	src.Report = 99
	src.HasReport = false

	clone := cloneSpotForBroadcast(src)
	if clone == nil {
		t.Fatalf("expected clone spot, got nil")
	}
	if clone.HasReport {
		t.Fatalf("expected HasReport=false, got true with Report=%d", clone.Report)
	}
	if clone.Report != src.Report {
		t.Fatalf("expected Report=%d, got %d", src.Report, clone.Report)
	}
}
