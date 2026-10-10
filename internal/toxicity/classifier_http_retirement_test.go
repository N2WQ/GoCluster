package toxicity

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/spot"
)

// This transport observes the real request context and body. It never replaces
// or cancels that context: only Classifier.Stop may satisfy the cancellation
// assertion. Cleanup independently releases the server and closes connections.
type classifierHTTPRetirementTransport struct {
	base          *http.Transport
	request       chan context.Context
	roundTripDone chan struct{}
	readEntered   chan struct{}
	readReturned  chan error
	roundTripOnce sync.Once
	readOnce      sync.Once
}

func (tr *classifierHTTPRetirementTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	select {
	case tr.request <- r.Context():
	default:
	}
	defer tr.roundTripOnce.Do(func() { close(tr.roundTripDone) })
	response, err := tr.base.RoundTrip(r)
	if err == nil {
		response.Body = &classifierHTTPRetirementBody{ReadCloser: response.Body, transport: tr}
	}
	return response, err
}

type classifierHTTPRetirementBody struct {
	io.ReadCloser
	transport *classifierHTTPRetirementTransport
}

func (body *classifierHTTPRetirementBody) Read(p []byte) (int, error) {
	first := false
	body.transport.readOnce.Do(func() {
		first = true
		close(body.transport.readEntered)
	})
	n, err := body.ReadCloser.Read(p)
	if first {
		body.transport.readReturned <- err
	}
	return n, err
}

type classifierHTTPRetirementRequest struct {
	method, path, auth, contentType string
	payload                         map[string]any
	err                             error
}

type classifierHTTPRetirementFixture struct {
	phase          string
	server         *httptest.Server
	transport      *classifierHTTPRetirementTransport
	classifier     *Classifier
	entered        chan struct{}
	handlerDone    chan struct{}
	release        chan struct{}
	requests       chan classifierHTTPRetirementRequest
	stopDone       []<-chan struct{}
	enterOnce      sync.Once
	handlerOnce    sync.Once
	releaseOnce    sync.Once
	requestCount   atomic.Int64
	serverCanceled atomic.Bool
}

func newClassifierHTTPRetirementFixture(t *testing.T, phase string) *classifierHTTPRetirementFixture {
	t.Helper()
	t.Setenv("DXC_TEST_HTTP_RETIREMENT_TOKEN", "local-retirement-token")
	f := &classifierHTTPRetirementFixture{phase: phase, entered: make(chan struct{}), handlerDone: make(chan struct{}),
		release: make(chan struct{}), requests: make(chan classifierHTTPRetirementRequest, 4)}
	f.transport = &classifierHTTPRetirementTransport{
		base:    &http.Transport{DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext, DisableKeepAlives: true},
		request: make(chan context.Context, 4), roundTripDone: make(chan struct{}),
		readEntered: make(chan struct{}), readReturned: make(chan error, 1),
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	// Register before any setup that can fail. This runs before Setenv restores
	// credentials, and never depends on the implementation canceling its HTTP I/O.
	t.Cleanup(func() { f.cleanup(t) })
	gate, err := NewSafeGateFromLists([]string{"CQ", "73"}, nil, 8)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Enabled: true, Workers: 1, QueueSize: 2, CacheMaxEntries: 4, CacheTTLSeconds: 60,
		MaxCommentBytes: 512, TimeoutMS: 30000, Endpoint: f.server.URL + "/classify", BearerTokenEnv: "DXC_TEST_HTTP_RETIREMENT_TOKEN"}
	f.classifier = NewClassifier(cfg, gate, NewClient(cfg, &http.Client{Transport: f.transport}))
	if f.classifier == nil {
		t.Fatal("enabled real HTTP classifier was not constructed")
	}
	f.classifier.Start()
	return f
}

func (f *classifierHTTPRetirementFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	f.requestCount.Add(1)
	defer f.handlerOnce.Do(func() { close(f.handlerDone) })
	var payload map[string]any
	err := json.NewDecoder(r.Body).Decode(&payload)
	_ = r.Body.Close()
	select {
	case f.requests <- classifierHTTPRetirementRequest{r.Method, r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("Content-Type"), payload, err}:
	default:
	}
	f.enterOnce.Do(func() { close(f.entered) })
	if err != nil {
		return
	}
	if f.phase == "body" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := http.NewResponseController(w).Flush(); err != nil {
			return
		}
	}
	select {
	case <-r.Context().Done():
		f.serverCanceled.Store(true)
	case <-f.release:
		// Failure cleanup alone may complete a response. The success assertions
		// run before release, so this cannot manufacture Stop cancellation.
		_, _ = io.WriteString(w, `{"status":"safe","model":"cleanup"}`)
	}
}

func classifierHTTPRetirementWait(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("real HTTP retirement did not reach %s", label)
	}
}

func (f *classifierHTTPRetirementFixture) startStop() <-chan struct{} {
	done := make(chan struct{})
	f.stopDone = append(f.stopDone, done)
	go func() {
		f.classifier.Stop()
		close(done)
	}()
	return done
}

func (f *classifierHTTPRetirementFixture) cleanup(t *testing.T) {
	t.Helper()
	f.releaseOnce.Do(func() { close(f.release) })
	f.server.CloseClientConnections()
	f.transport.base.CloseIdleConnections()
	f.server.Close() // Joins the independently released test HTTP handlers.
	if f.classifier != nil {
		f.startStop()
	}
	for _, done := range f.stopDone {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("test-owned Stop did not join after independent HTTP cleanup")
		}
	}
	f.transport.base.CloseIdleConnections()
}

func TestClassifierStopRetiresRealHTTPHeaderAndBodyStalls(t *testing.T) {
	for _, phase := range []string{"headers", "body"} {
		t.Run(phase, func(t *testing.T) {
			f := newClassifierHTTPRetirementFixture(t, phase)
			c := f.classifier
			active := spot.NewSpot("K1ABC", "W1XYZ", 14030, "CW")
			active.Comment = "retirement " + phase + " stall"
			active.ToxicityStatus, active.ToxicityModel = spot.ToxicityUnknown, "caller-model"
			active.ToxicityCategories = []string{"caller-category"}
			spots, before := []*spot.Spot{active}, []*spot.Spot{active.Clone()}
			if c.ClassifyOrEnqueue(active, time.Now().UTC()) {
				t.Fatal("unsafe comment bypassed the real HTTP job")
			}
			classifierHTTPRetirementWait(t, f.entered, "server request entry")
			request := <-f.requests
			if request.err != nil || request.method != "POST" || request.path != "/classify" || request.auth != "Bearer local-retirement-token" ||
				request.contentType != "application/json" || !reflect.DeepEqual(request.payload, map[string]any{"comment": active.Comment}) {
				t.Fatalf("production client did not send the intended local comment-only request: %#v", request)
			}
			parent := <-f.transport.request
			deadline, ok := parent.Deadline()
			if !ok || parent.Err() != nil || time.Until(deadline) < 20*time.Second || time.Until(deadline) > 30*time.Second {
				t.Fatalf("request lacks the live long ordinary timeout: deadline=%v error=%v", deadline, parent.Err())
			}
			if phase == "body" {
				classifierHTTPRetirementWait(t, f.transport.readEntered, "client first body Read")
				select {
				case err := <-f.transport.readReturned:
					t.Fatalf("body Read returned before Stop released its stall: %v", err)
				default:
				}
			} else {
				select {
				case <-f.transport.roundTripDone:
					t.Fatal("header RoundTrip completed before terminal cancellation")
				default:
				}
			}
			for _, comment := range []string{"retirement queued one", "retirement queued two"} {
				s := spot.NewSpot("K2ABC", "W2XYZ", 7030, "CW")
				s.Comment, s.ToxicityStatus = comment, spot.ToxicityUnknown
				spots, before = append(spots, s), append(before, s.Clone())
				if c.ClassifyOrEnqueue(s, time.Now().UTC()) {
					t.Fatal("bounded queued control was not admitted behind the stalled request")
				}
			}
			if c.Pending() != 3 || len(c.jobs) != 2 || c.ResultBacklog() != 0 {
				t.Fatal("retirement must exercise one in-flight and two queued jobs")
			}
			stoppedAt := time.Now()
			for range 4 {
				f.startStop()
			}
			for _, done := range f.stopDone {
				classifierHTTPRetirementWait(t, done, "concurrent terminal Stop completion")
			}
			if time.Since(stoppedAt) >= 5*time.Second || parent.Err() != context.Canceled || !time.Now().Before(deadline) {
				t.Fatal("Stop did not cancel the production request well before its ordinary timeout")
			}
			if phase == "body" {
				select {
				case err := <-f.transport.readReturned:
					if err == nil {
						t.Fatal("stalled body Read succeeded without a response body")
					}
				default:
					t.Fatal("Stop returned before the actual body Read completed")
				}
			} else {
				select {
				case <-f.transport.roundTripDone:
				default:
					t.Fatal("Stop returned before the stalled RoundTrip completed")
				}
			}
			classifierHTTPRetirementWait(t, f.handlerDone, "canceled local HTTP handler exit")
			if !f.serverCanceled.Load() || f.requestCount.Load() != 1 || len(c.jobs) != 0 || len(c.results) != 0 || c.Snapshot() != (Stats{AICalls: 1}) {
				t.Fatalf("terminal drops retained work or became ordinary outcomes: requests=%d jobs=%d results=%d stats=%+v",
					f.requestCount.Load(), len(c.jobs), len(c.results), c.Snapshot())
			}
			for i, s := range spots {
				// Clone normalizes status and an empty category slice. Check the
				// raw caller fields too so that normalization cannot hide mutation.
				if s.ToxicityStatus != spot.ToxicityUnknown || (s.ToxicityCategories == nil) != (before[i].ToxicityCategories == nil) ||
					!reflect.DeepEqual(s.Clone(), before[i]) {
					t.Fatalf("terminal retirement mutated discarded spot %d", i)
				}
			}
			classifierHTTPRetirementWait(t, f.startStop(), "repeated Stop completion")
			if c.Snapshot() != (Stats{AICalls: 1}) || c.Pending() != 0 || c.ResultBacklog() != 0 {
				t.Fatal("idempotent Stop changed completed terminal accounting")
			}
		})
	}
}
