// File role: Bounded HTTP client with conditional GET caching, body-size caps,
// redirect safety, jittered exponential backoff, and context-cancellable waits.
// Crawler notes: Owns the dedicated http.Transport closed on Poller.Stop().
package feed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	maxConditionalHeaderBytes = 256
	maxRedirects              = 5
	maxErrorDrainBytes        = 4096
	jitterFraction            = 0.15
	maxBackoffDelay           = 30 * time.Minute
	minBackoffBase            = 5 * time.Second
)

var (
	// ErrBodyTooLarge is returned when an upstream HTTP payload exceeds the
	// configured byte cap for that endpoint.
	ErrBodyTooLarge = errors.New("propprior/feed: response body exceeds byte limit")
)

type httpStatusError struct {
	statusCode int
	retryAfter time.Duration
}

func (e *httpStatusError) Error() string {
	if e.retryAfter > 0 {
		return fmt.Sprintf("http status %d (retry-after %s)", e.statusCode, e.retryAfter.Round(time.Second))
	}
	return fmt.Sprintf("http status %d", e.statusCode)
}

type conditionalEndpoint struct {
	etag         string
	lastModified string
}

func (e *conditionalEndpoint) updateHeaders(h http.Header) {
	if e == nil || h == nil {
		return
	}
	if etag := strings.TrimSpace(h.Get("ETag")); etag != "" && len(etag) <= maxConditionalHeaderBytes {
		e.etag = etag
	}
	if lm := strings.TrimSpace(h.Get("Last-Modified")); lm != "" && len(lm) <= maxConditionalHeaderBytes {
		e.lastModified = lm
	}
}

type boundedHTTPClient struct {
	client    *http.Client
	transport *http.Transport
	timeout   time.Duration
	userAgent string
}

func newBoundedHTTPClient(timeout time.Duration, userAgent string) *boundedHTTPClient {
	dialTimeout := 10 * time.Second
	if timeout < dialTimeout {
		dialTimeout = timeout
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          16,
		MaxIdleConnsPerHost:   4,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   dialTimeout,
		ResponseHeaderTimeout: timeout,
		ExpectContinueTimeout: 1 * time.Second,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
				return fmt.Errorf("stopped after %d redirects", maxRedirects)
			}
			if len(via) > 0 && via[len(via)-1].URL.Scheme == "https" && req.URL.Scheme != "https" {
				return errors.New("refusing https-to-http redirect downgrade")
			}
			return nil
		},
	}
	return &boundedHTTPClient{
		client:    client,
		transport: transport,
		timeout:   timeout,
		userAgent: strings.TrimSpace(userAgent),
	}
}

func (c *boundedHTTPClient) closeIdle() {
	if c != nil && c.transport != nil {
		c.transport.CloseIdleConnections()
	}
}

type fetchResult struct {
	body       []byte
	statusCode int
	modified   bool
	retryAfter time.Duration
}

func (c *boundedHTTPClient) fetch(ctx context.Context, endpoint *conditionalEndpoint, rawURL string, maxBytes int64, now time.Time) (fetchResult, error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		return fetchResult{}, err
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "application/json")
	if endpoint != nil {
		if endpoint.etag != "" {
			req.Header.Set("If-None-Match", endpoint.etag)
		}
		if endpoint.lastModified != "" {
			req.Header.Set("If-Modified-Since", endpoint.lastModified)
		}
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fetchResult{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		drainResponseBody(resp.Body)
		endpoint.updateHeaders(resp.Header)
		return fetchResult{statusCode: resp.StatusCode, modified: false}, nil
	}

	if resp.StatusCode != http.StatusOK {
		drainResponseBody(resp.Body)
		var retryAfter time.Duration
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
			retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), now)
		}
		return fetchResult{statusCode: resp.StatusCode, retryAfter: retryAfter}, &httpStatusError{
			statusCode: resp.StatusCode,
			retryAfter: retryAfter,
		}
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return fetchResult{statusCode: resp.StatusCode}, err
	}
	if int64(len(body)) > maxBytes {
		return fetchResult{statusCode: resp.StatusCode}, fmt.Errorf("%w (cap=%d)", ErrBodyTooLarge, maxBytes)
	}
	endpoint.updateHeaders(resp.Header)
	return fetchResult{
		body:       body,
		statusCode: resp.StatusCode,
		modified:   true,
	}, nil
}

func parseRetryAfter(raw string, now time.Time) time.Duration {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0
	}
	if secs, err := strconv.Atoi(trimmed); err == nil {
		if secs <= 0 {
			return 0
		}
		d := time.Duration(secs) * time.Second
		if d > maxBackoffDelay {
			return maxBackoffDelay
		}
		return d
	}
	if t, err := http.ParseTime(trimmed); err == nil {
		d := t.Sub(now)
		if d <= 0 {
			return 0
		}
		if d > maxBackoffDelay {
			return maxBackoffDelay
		}
		return d
	}
	return 0
}

// jitterRNG is a small non-cryptographic PRNG used solely to spread HTTP
// polling intervals and backoff delays across hosts without thundering herds.
type jitterRNG struct {
	state uint64
}

func newJitterRNG(seed uint64) *jitterRNG {
	if seed == 0 {
		seed = 0x9e3779b97f4a7c15
	}
	return &jitterRNG{state: seed}
}

func (r *jitterRNG) unitFloat() float64 {
	// SplitMix64 step producing a uniform float64 in [0, 1).
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	z ^= z >> 31
	return float64(z>>11) / (1 << 53)
}

func drainResponseBody(r io.Reader) {
	if _, err := io.CopyN(io.Discard, r, maxErrorDrainBytes); err != nil {
		// Best-effort drain for connection reuse on non-200/304 responses.
		return
	}
}

func jitterDuration(base time.Duration, rng *jitterRNG) time.Duration {
	if base <= 0 || rng == nil {
		return base
	}
	// Uniform in [1-jitterFraction, 1+jitterFraction].
	factor := 1.0 + (rng.unitFloat()*2.0-1.0)*jitterFraction
	d := time.Duration(math.Round(float64(base) * factor))
	if d <= 0 {
		return time.Millisecond
	}
	return d
}

func computeBackoff(pollInterval time.Duration, consecutiveErrors int, retryAfter time.Duration, rng *jitterRNG) time.Duration {
	base := pollInterval / 4
	if base <= 0 {
		base = minBackoffBase
	}
	if base > minBackoffBase && pollInterval >= 4*minBackoffBase {
		// Cap initial backoff base to min(pollInterval/4, 30s) for responsive recovery
		// while respecting short test poll intervals.
		if base > 30*time.Second {
			base = 30 * time.Second
		}
	}
	maxDelay := pollInterval * 2
	if maxDelay > maxBackoffDelay {
		maxDelay = maxBackoffDelay
	}
	if maxDelay < base {
		maxDelay = base
	}
	shift := consecutiveErrors - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 10 {
		shift = 10
	}
	delay := base << shift
	if delay <= 0 || delay > maxDelay {
		delay = maxDelay
	}
	if retryAfter > delay {
		delay = retryAfter
	}
	if delay > maxBackoffDelay {
		delay = maxBackoffDelay
	}
	return jitterDuration(delay, rng)
}

func waitContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(d)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
