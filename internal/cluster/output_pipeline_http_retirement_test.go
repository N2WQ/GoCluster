// File role: Qualifies the real runtime grace boundary against HTTP stalls and
// transferred-result processing. Cleanup has independent releases, so it cannot
// manufacture the success cancellation or downstream-completion observations.
package cluster

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/dedup"
	"dxcluster/internal/toxicity"
	"dxcluster/spot"
	"dxcluster/telnet"
)

const runtimeHTTPWait = 8 * time.Second

type runtimeHTTPStallBody struct {
	io.ReadCloser
	transport *runtimeHTTPRetirementTransport
}

func (b *runtimeHTTPStallBody) Read(p []byte) (int, error) {
	b.transport.readOnce.Do(func() { close(b.transport.readEntered) })
	n, err := b.ReadCloser.Read(p)
	b.transport.returnOnce.Do(func() { b.transport.readReturned <- err })
	return n, err
}

type runtimeHTTPRetirementTransport struct {
	base         *http.Transport
	request      chan context.Context
	tripDone     chan struct{}
	readEntered  chan struct{}
	readReturned chan error
	readOnce     sync.Once
	returnOnce   sync.Once
}

func (tr *runtimeHTTPRetirementTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	copyBody, err := req.GetBody()
	if err != nil {
		return nil, err
	}
	var input struct {
		Comment string `json:"comment"`
	}
	err = json.NewDecoder(copyBody).Decode(&input)
	_ = copyBody.Close()
	if err != nil {
		return nil, err
	}
	if input.Comment != "unfinished runtime conversation" {
		return tr.base.RoundTrip(req)
	}
	tr.request <- req.Context()
	defer close(tr.tripDone)
	response, err := tr.base.RoundTrip(req)
	if err == nil {
		response.Body = &runtimeHTTPStallBody{ReadCloser: response.Body, transport: tr}
	}
	return response, err
}

type runtimeHTTPRetirementFixture struct {
	runtime        *clusterRuntime
	server         *httptest.Server
	transport      *runtimeHTTPRetirementTransport
	phase          string
	entered        map[string]chan struct{}
	release        map[string]chan struct{}
	releaseOnce    sync.Once
	callback       chan struct{}
	callbackFree   chan struct{}
	callbackOnce   sync.Once
	callbackClose  sync.Once
	gridStopped    chan struct{}
	closeDone      chan struct{}
	repeatDone     chan struct{}
	handlerStopped chan struct{}
	requestCount   atomic.Int64
	serverCanceled atomic.Bool
	spots          [3]*spot.Spot
}

func runtimeHTTPAwait(t *testing.T, ch <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(runtimeHTTPWait):
		t.Fatalf("runtime HTTP retirement did not reach %s", description)
	}
}

func runtimeHTTPEventually(t *testing.T, description string, ready func() bool) {
	t.Helper()
	deadline, tick := time.NewTimer(runtimeHTTPWait), time.NewTicker(time.Millisecond)
	defer deadline.Stop()
	defer tick.Stop()
	for !ready() {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("runtime HTTP retirement did not establish %s", description)
		}
	}
}

func newRuntimeHTTPRetirementFixture(t *testing.T, phase string) *runtimeHTTPRetirementFixture {
	t.Helper()
	t.Setenv("DXC_TEST_RUNTIME_RETIREMENT_TOKEN", "local-runtime-retirement-token")
	archiveDir := filepath.Join(t.TempDir(), "archive")
	f := &runtimeHTTPRetirementFixture{phase: phase, entered: make(map[string]chan struct{}),
		release: make(map[string]chan struct{}), callback: make(chan struct{}), callbackFree: make(chan struct{}),
		gridStopped: make(chan struct{}), handlerStopped: make(chan struct{})}
	comments := []string{"owned runtime conversation", "buffered runtime conversation", "unfinished runtime conversation"}
	for _, comment := range comments {
		f.entered[comment], f.release[comment] = make(chan struct{}), make(chan struct{})
	}
	f.transport = &runtimeHTTPRetirementTransport{base: &http.Transport{DisableKeepAlives: true},
		request: make(chan context.Context, 1), tripDone: make(chan struct{}), readEntered: make(chan struct{}), readReturned: make(chan error, 1)}
	f.server = httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(func() { f.cleanup(t) })
	gate, err := toxicity.NewSafeGateFromLists([]string{"CQ"}, nil, 8)
	if err != nil {
		t.Fatal(err)
	}
	cfg := toxicity.Config{Enabled: true, Workers: 3, QueueSize: 4, CacheMaxEntries: 4,
		CacheTTLSeconds: 60, MaxCommentBytes: 512, TimeoutMS: 30000,
		Endpoint: f.server.URL, BearerTokenEnv: "DXC_TEST_RUNTIME_RETIREMENT_TOKEN"}
	classifier := toxicity.NewClassifier(cfg, gate, toxicity.NewClient(cfg, &http.Client{Transport: f.transport}))
	r := newClusterRuntime(BuildInfo{}, &config.Config{}, "", config.LoadDiagnostics{})
	f.runtime = r
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.deduplicator = dedup.NewDeduplicator(time.Minute, false, 4)
	r.spotBuffer = buffer.NewRingBuffer(4)
	r.toxicityClassifier = classifier
	r.archiveWriter, err = archive.NewWriter(config.ArchiveConfig{DBPath: archiveDir, QueueSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	r.telnetServer = telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil)
	r.stopGridWriter = func() { close(f.gridStopped) }
	r.gridUpdater = func(call, _ string) {
		if call == "K1OWN" {
			f.callbackOnce.Do(func() { close(f.callback) })
			<-f.callbackFree
		}
	}
	// The runtime's default nil CTY snapshot starts no reference refresh.
	classifier.Start()
	for i, call := range []string{"K1OWN", "K1BUF", "K1PEND"} {
		s := spot.NewSpot(call, "W1XYZ", 14030, "CW")
		s.ToxicityStatus = spot.ToxicityUnknown
		s.Comment, s.DXMetadata.Grid, s.DEMetadata.Grid = comments[i], "FN31", "EM12"
		f.spots[i] = s
		if classifier.ClassifyOrEnqueue(s, time.Now().UTC()) {
			t.Fatal("real runtime HTTP control did not enqueue")
		}
	}
	r.startOutputPipeline()
	return f
}

func (f *runtimeHTTPRetirementFixture) serveHTTP(w http.ResponseWriter, req *http.Request) {
	f.requestCount.Add(1)
	var input struct {
		Comment string `json:"comment"`
	}
	err := json.NewDecoder(req.Body).Decode(&input)
	_ = req.Body.Close()
	if err != nil || f.entered[input.Comment] == nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	close(f.entered[input.Comment])
	if input.Comment == "unfinished runtime conversation" {
		defer close(f.handlerStopped)
		if f.phase == "body" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := http.NewResponseController(w).Flush(); err != nil {
				return
			}
		}
	}
	select {
	case <-req.Context().Done():
		if input.Comment == "unfinished runtime conversation" {
			f.serverCanceled.Store(true)
		}
		return
	case <-f.release[input.Comment]:
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"status":"toxic","categories":["S10"],"model":"runtime-retirement-fixture"}`)
}

func (f *runtimeHTTPRetirementFixture) cleanup(t *testing.T) {
	t.Helper()
	f.releaseOnce.Do(func() {
		for _, ch := range f.release {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
	})
	f.callbackClose.Do(func() { close(f.callbackFree) })
	f.server.CloseClientConnections()
	f.transport.base.CloseIdleConnections()
	f.server.Close()
	if f.runtime != nil {
		if f.closeDone == nil {
			f.closeDone = make(chan struct{})
			go func() { f.runtime.close(); close(f.closeDone) }()
		}
		for _, done := range []<-chan struct{}{f.closeDone, f.repeatDone} {
			if done == nil {
				continue
			}
			select {
			case <-done:
			case <-time.After(runtimeHTTPWait):
				t.Error("runtime HTTP fixture did not join after independent cleanup releases")
			}
		}
	}
}

func TestRuntimeRetirementHTTPExpiryPreservesTransferredResult(t *testing.T) {
	for _, phase := range []string{"headers", "body"} {
		t.Run(phase, func(t *testing.T) {
			f := newRuntimeHTTPRetirementFixture(t, phase)
			r, classifier := f.runtime, f.runtime.toxicityClassifier
			for _, entered := range f.entered {
				runtimeHTTPAwait(t, entered, "three actual request entries")
			}
			request := <-f.transport.request
			ordinaryDeadline, ok := request.Deadline()
			if !ok || request.Err() != nil || time.Until(ordinaryDeadline) < 20*time.Second {
				t.Fatal("stalled production request lacks a live long ordinary deadline")
			}
			if phase == "body" {
				runtimeHTTPAwait(t, f.transport.readEntered, "actual first body Read")
				select {
				case <-f.transport.readReturned:
					t.Fatal("body Read completed before the retirement stimulus")
				default:
				}
			}
			f.closeDone = make(chan struct{})
			requestBefore := time.Now()
			go func() { r.close(); close(f.closeDone) }()
			p := r.outputPipeline
			runtimeHTTPAwait(t, p.stopCh, "first runtime stop request")
			requestAfter := time.Now()
			firstDeadline := p.drainDeadline
			if classifier.DrainTimeout() != 5*time.Second || firstDeadline.Before(requestBefore.Add(5*time.Second)) || firstDeadline.After(requestAfter.Add(5*time.Second)) {
				t.Fatal("first retirement request did not start the existing five-second grace budget")
			}
			close(f.release["owned runtime conversation"])
			runtimeHTTPAwait(t, f.callback, "transferred A blocked before handoff")
			close(f.release["buffered runtime conversation"])
			runtimeHTTPEventually(t, "completed unreceived B plus unfinished HTTP", func() bool {
				return classifier.Pending() == 1 && classifier.ResultBacklog() == 1
			})
			f.repeatDone = make(chan struct{})
			go func() { r.retireOutputPipeline(); close(f.repeatDone) }()
			p.beginRetirement() // Exercise the pipeline owner's repeated request too.
			if !time.Now().Before(firstDeadline) || request.Err() != nil {
				t.Fatal("fixture failed to establish its blocked processing while grace was still live")
			}
			expiry := time.NewTimer(time.Until(firstDeadline))
			select {
			case <-request.Done():
				if time.Now().Before(firstDeadline) {
					expiry.Stop()
					t.Fatal("unfinished request canceled before the first grace opportunity ended")
				}
			case <-expiry.C:
			}
			expiry.Stop()
			runtimeHTTPAwait(t, p.classifierStopped, "terminal classifier cancellation and join")
			if request.Err() != context.Canceled || !time.Now().Before(ordinaryDeadline) || !p.drainDeadline.Equal(firstDeadline) {
				t.Fatal("retirement used an ordinary timeout or extended the first grace budget")
			}
			runtimeHTTPAwait(t, f.handlerStopped, "real canceled HTTP handler exit")
			if !f.serverCanceled.Load() || f.requestCount.Load() != 3 || classifier.Snapshot() != (toxicity.Stats{AICalls: 3, Toxic: 2, CacheEntries: 2}) {
				t.Fatalf("retirement invented outcomes or retained work: requests=%d stats=%+v", f.requestCount.Load(), classifier.Snapshot())
			}
			if phase == "body" {
				select {
				case err := <-f.transport.readReturned:
					if err == nil {
						t.Fatal("unfinished body Read succeeded without a response body")
					}
				default:
					t.Fatal("terminal Stop returned before the body Read joined")
				}
			} else {
				runtimeHTTPAwait(t, f.transport.tripDone, "canceled header RoundTrip completion")
			}
			for _, done := range []<-chan struct{}{p.done, f.closeDone, f.repeatDone, f.gridStopped} {
				select {
				case <-done:
					t.Fatal("processing or downstream closure completed before transferred A returned")
				default:
				}
			}
			archiveStopped, serverStopped := runtimeRetirementResourceSignals(r)
			for _, stopped := range []<-chan struct{}{archiveStopped, serverStopped, r.ctx.Done()} {
				select {
				case <-stopped:
					t.Fatal("downstream resource stopped before admitted A completed processing")
				default:
				}
			}
			if r.spotBuffer.GetCount() != 0 || f.spots[1].ToxicityStatus != spot.ToxicityUnknown || f.spots[2].ToxicityStatus != spot.ToxicityUnknown {
				t.Fatal("expiry owner performed handoff or stamped unreceived/unfinished spots")
			}
			f.callbackClose.Do(func() { close(f.callbackFree) })
			runtimeHTTPAwait(t, f.closeDone, "processing followed by downstream closure")
			runtimeHTTPAwait(t, f.repeatDone, "repeated shared retirement join")
			runtimeHTTPAwait(t, f.gridStopped, "post-pipeline grid writer closure")
			recent := r.spotBuffer.GetRecent(1)
			if len(recent) != 1 {
				t.Fatalf("transferred A missing from normal ring handoff: count=%d", len(recent))
			}
			for _, snapshot := range []*spot.Spot{recent[0], mustReadRuntimeHTTPArchive(t, r), mustReadRuntimeHTTPTelnet(t, r)} {
				if snapshot.DXCallNorm != "K1OWN" || snapshot.ToxicityStatus != spot.ToxicityToxic || snapshot.ToxicityModel != "runtime-retirement-fixture" || len(snapshot.ToxicityCategories) != 1 || snapshot.ToxicityCategories[0] != "S10" {
					t.Fatalf("transferred A lost its normal classified handoff: %+v", snapshot)
				}
			}
			if r.spotBuffer.GetCount() != 1 || classifier.Pending() != 0 || classifier.ResultBacklog() != 0 {
				t.Fatal("runtime completion retained work or handed off buffered B")
			}
		})
	}
}

func mustReadRuntimeHTTPArchive(t *testing.T, r *clusterRuntime) *spot.Spot {
	t.Helper()
	s, ok := tryReadArchiveQueuedSpot(r.archiveWriter)
	if !ok {
		t.Fatal("transferred A did not attempt normal archive handoff")
	}
	return s
}

func mustReadRuntimeHTTPTelnet(t *testing.T, r *clusterRuntime) *spot.Spot {
	t.Helper()
	s, ok := tryReadTelnetBroadcastSpot(r.telnetServer)
	if !ok {
		t.Fatal("transferred A did not attempt normal telnet handoff")
	}
	return s
}
