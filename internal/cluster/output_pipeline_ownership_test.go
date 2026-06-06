package cluster

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/spot"
	"dxcluster/telnet"
)

func TestOutputPipelineEmitSpotReusesFinalOwnedSnapshot(t *testing.T) {
	ring := buffer.NewRingBuffer(4)
	writer, err := archive.NewWriter(config.ArchiveConfig{
		DBPath:    filepath.Join(t.TempDir(), "archive"),
		QueueSize: 1,
	})
	if err != nil {
		t.Fatalf("new archive writer: %v", err)
	}
	t.Cleanup(func() {
		writer.Stop()
	})
	srv := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 1}, nil)
	pipeline := &outputPipeline{
		ctyLookup:     func() *cty.CTYDatabase { return nil },
		buf:           ring,
		archiveWriter: writer,
		telnet:        srv,
		gridLookupSync: func(call string) (string, bool, bool) {
			switch call {
			case "K1ABC":
				return "EN61", false, true
			case "W1XYZ":
				return "FN31", false, true
			default:
				return "", false, false
			}
		},
	}

	src := spot.NewSpot("K1ABC", "W1XYZ", 14074.0, "FT8")
	src.Comment = "CQ TEST"

	ctx, ok := pipeline.prepareSpotContext(src)
	if !ok {
		t.Fatal("expected spot context")
	}
	if !pipeline.finalizeSpotForMetrics(&ctx) {
		t.Fatal("expected metrics stage to pass")
	}
	if !pipeline.prepareFanoutSpot(&ctx) {
		t.Fatal("expected fanout stage to pass")
	}
	if ctx.spot.DXMetadata.Grid != "EN61" || ctx.spot.DEMetadata.Grid != "FN31" {
		t.Fatalf("expected prepareFanoutSpot to backfill grids, got DX=%q DE=%q", ctx.spot.DXMetadata.Grid, ctx.spot.DEMetadata.Grid)
	}

	pipeline.emitSpot(&ctx, outputDeliveryPlan{
		telnetDeliverNow: true,
		allowFast:        true,
		allowMed:         true,
		allowSlow:        true,
	})

	recent := ring.GetRecent(1)
	if len(recent) != 1 {
		t.Fatalf("expected one buffered spot, got %d", len(recent))
	}

	archived := readArchiveQueuedSpot(t, writer)
	broadcasted := readTelnetBroadcastSpot(t, srv)

	shared := ctx.spot
	if shared.ID == 0 {
		t.Fatal("expected ring buffer to assign an ID to the shared snapshot")
	}
	if recent[0] != shared {
		t.Fatalf("expected ring buffer to reuse the final shared pointer")
	}
	if archived != shared {
		t.Fatalf("expected archive queue to reuse the final shared pointer")
	}
	if broadcasted != shared {
		t.Fatalf("expected telnet broadcast to reuse the final shared pointer")
	}
}

func TestArchiveSnapshotForBlankBeaconDoesNotMutatePeerComment(t *testing.T) {
	src := spot.NewSpot("W1ABC/B", "K1XYZ", 14020.0, "CW")
	src.SourceType = spot.SourceRBN
	src.BeaconComment = spot.BeaconCommentNCDXF
	src.Comment = ""
	shared := src.SealForAsync()

	archived := archiveSnapshotForSpot(shared)
	if archived == shared {
		t.Fatalf("expected archive snapshot to clone blank beacon comment")
	}
	if archived.Comment != "NCDXF BEACON" {
		t.Fatalf("expected archive snapshot comment NCDXF BEACON, got %q", archived.Comment)
	}
	if shared.Comment != "" {
		t.Fatalf("expected shared peer/telnet snapshot comment to remain blank, got %q", shared.Comment)
	}
	if comment := peerPublishComment(shared); comment != "" {
		t.Fatalf("expected peer comment to remain blank for non-manual beacon, got %q", comment)
	}
	if got := shared.FormatDXCluster(); !strings.Contains(got, "CW NCDXF BEACON") {
		t.Fatalf("expected telnet formatter fallback on shared snapshot, got %q", got)
	}
	if got := archived.FormatDXCluster(); !strings.Contains(got, "CW NCDXF BEACON") {
		t.Fatalf("expected archived snapshot to format beacon comment, got %q", got)
	}
}

func readArchiveQueuedSpot(t *testing.T, writer *archive.Writer) *spot.Spot {
	t.Helper()
	queue := unsafeValue(reflect.ValueOf(writer).Elem().FieldByName("queue"))
	select {
	case snapshot := <-queue.Interface().(chan *spot.Spot):
		return snapshot
	default:
		t.Fatal("expected archived snapshot")
		return nil
	}
}

func readTelnetBroadcastSpot(t *testing.T, srv *telnet.Server) *spot.Spot {
	t.Helper()
	broadcast := unsafeValue(reflect.ValueOf(srv).Elem().FieldByName("broadcast"))
	recv, ok := broadcast.TryRecv()
	if !ok {
		t.Fatal("expected broadcast snapshot")
	}
	if recv.Kind() == reflect.Pointer {
		recv = recv.Elem()
	}
	return unsafeValue(recv.FieldByName("spot")).Interface().(*spot.Spot)
}

func unsafeValue(v reflect.Value) reflect.Value {
	return reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem()
}
