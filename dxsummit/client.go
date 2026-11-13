// Package dxsummit polls the DXSummit HTTP spot API and converts rows into
// regular upstream human spots for the shared ingest pipeline.
package dxsummit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/config"
	"dxcluster/internal/ratelimit"
	"dxcluster/spot"
)

const (
	SourceNode            = "DXSUMMIT"
	apiTimeLayout         = "2006-01-02T15:04:05"
	queueDropLogInterval  = time.Minute
	truncationLogInterval = time.Minute
	defaultHTTPClientIdle = 30 * time.Second
)

type rawSpot struct {
	ID          uint64   `json:"id"`
	DECall      string   `json:"de_call"`
	DXCall      string   `json:"dx_call"`
	Info        *string  `json:"info"`
	Frequency   float64  `json:"frequency"`
	Time        string   `json:"time"`
	DXCountry   string   `json:"dx_country"`
	DELatitude  *float64 `json:"de_latitude"`
	DELongitude *float64 `json:"de_longitude"`
	DXLatitude  *float64 `json:"dx_latitude"`
	DXLongitude *float64 `json:"dx_longitude"`
}

// BadCallReporter receives parse-time callsign drops. The callback must be
// quick because it runs on the polling goroutine.
type BadCallReporter func(source, role, reason, call, deCall, dxCall, mode, detail string)

// HealthSnapshot reports DXSummit polling and queue state for diagnostics.
type HealthSnapshot struct {
	Connected          bool
	LastPollAt         time.Time
	LastMessageAt      time.Time
	LastSpotAt         time.Time
	LastParseErrAt     time.Time
	SpotQueueLen       int
	SpotQueueCap       int
	SpotDrops          uint64
	ParseErrors        uint64
	RequestErrors      uint64
	ResponseTooLarge   uint64
	DuplicateRows      uint64
	EmittedSpots       uint64
	TruncationWarnings uint64
	LastStatusCode     int
	LastError          string
	LastSeenID         uint64
}

// Client owns one DXSummit polling loop and a bounded spot output channel.
type Client struct {
	cfg        config.DXSummitConfig
	httpClient *http.Client
	spotChan   chan *spot.Spot
	now        func() time.Time
	logf       func(string, ...any)

	mu              sync.Mutex
	cancel          context.CancelFunc
	connected       bool
	lastPollAt      time.Time
	lastMessageAt   time.Time
	lastSpotAt      time.Time
	lastParseErrAt  time.Time
	lastStatusCode  int
	lastError       string
	lastSeenID      uint64
	badCallReporter BadCallReporter
	wg              sync.WaitGroup

	spotDrops          atomic.Uint64
	parseErrors        atomic.Uint64
	requestErrors      atomic.Uint64
	responseTooLarge   atomic.Uint64
	duplicateRows      atomic.Uint64
	emittedSpots       atomic.Uint64
	truncationWarnings atomic.Uint64

	queueDropLogs  ratelimit.Counter
	truncationLogs ratelimit.Counter
}

// NewClient constructs a DXSummit client with bounded queues and default HTTP transport.
func NewClient(cfg config.DXSummitConfig) *Client {
	return NewClientWithHTTPClient(cfg, &http.Client{Timeout: defaultHTTPClientIdle})
}

// NewClientWithHTTPClient constructs a client with an injected HTTP client for tests.
func NewClientWithHTTPClient(cfg config.DXSummitConfig, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultHTTPClientIdle}
	}
	size := cfg.SpotChannelSize
	if size <= 0 {
		size = 1
	}
	return &Client{
		cfg:            cfg,
		httpClient:     httpClient,
		spotChan:       make(chan *spot.Spot, size),
		now:            func() time.Time { return time.Now().UTC() },
		logf:           log.Printf,
		queueDropLogs:  ratelimit.NewCounter(queueDropLogInterval),
		truncationLogs: ratelimit.NewCounter(truncationLogInterval),
	}
}

// SetNowFunc overrides the clock source. It is intended for deterministic tests.
func (c *Client) SetNowFunc(now func() time.Time) {
	if now == nil {
		return
	}
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

// SetLogger overrides the logger. Passing nil disables client logs.
func (c *Client) SetLogger(logf func(string, ...any)) {
	c.mu.Lock()
	c.logf = logf
	c.mu.Unlock()
}

// SetBadCallReporter installs an optional callback for callsign-validation
// drops. It is safe to call while polling is active.
func (c *Client) SetBadCallReporter(reporter BadCallReporter) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.badCallReporter = reporter
	c.mu.Unlock()
}

// Connect starts the polling loop.
func (c *Client) Connect() error {
	if c == nil {
		return errors.New("dxsummit: nil client")
	}
	c.mu.Lock()
	if c.cancel != nil {
		c.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	c.mu.Unlock()

	go c.run(ctx)
	return nil
}

func (c *Client) Endpoint() string {
	if c == nil {
		return ""
	}
	return strings.TrimSpace(c.cfg.BaseURL)
}

// Stop cancels polling and waits for the output channel to close.
func (c *Client) Stop() {
	if c == nil {
		return
	}
	c.mu.Lock()
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	c.wg.Wait()
}

// GetSpotChannel returns the bounded channel consumed by the shared ingest path.
func (c *Client) GetSpotChannel() <-chan *spot.Spot {
	if c == nil {
		return nil
	}
	return c.spotChan
}

// HealthSnapshot returns recent polling and queue counters.
func (c *Client) HealthSnapshot() HealthSnapshot {
	if c == nil {
		return HealthSnapshot{}
	}
	c.mu.Lock()
	snap := HealthSnapshot{
		Connected:      c.connected,
		LastPollAt:     c.lastPollAt,
		LastMessageAt:  c.lastMessageAt,
		LastSpotAt:     c.lastSpotAt,
		LastParseErrAt: c.lastParseErrAt,
		LastStatusCode: c.lastStatusCode,
		LastError:      c.lastError,
		LastSeenID:     c.lastSeenID,
	}
	c.mu.Unlock()
	snap.SpotQueueLen = len(c.spotChan)
	snap.SpotQueueCap = cap(c.spotChan)
	snap.SpotDrops = c.spotDrops.Load()
	snap.ParseErrors = c.parseErrors.Load()
	snap.RequestErrors = c.requestErrors.Load()
	snap.ResponseTooLarge = c.responseTooLarge.Load()
	snap.DuplicateRows = c.duplicateRows.Load()
	snap.EmittedSpots = c.emittedSpots.Load()
	snap.TruncationWarnings = c.truncationWarnings.Load()
	return snap
}

func (c *Client) run(ctx context.Context) {
	defer c.wg.Done()
	defer close(c.spotChan)

	ticker := time.NewTicker(time.Duration(c.cfg.PollIntervalSeconds) * time.Second)
	defer ticker.Stop()

	seeded := false
	for {
		// The polling goroutine owns request sequencing. Each iteration finishes
		// the current request before waiting on the ticker, so DXSummit polls
		// cannot overlap even when the endpoint is slow.
		if !seeded {
			if c.poll(ctx, true) {
				seeded = true
			}
		} else {
			c.poll(ctx, false)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// poll performs one bounded API request, advances the O(1) high-water cursor,
// and emits accepted rows oldest-to-newest. A successful fetch marks the source
// connected before emission, because seed-only startup can be healthy while
// intentionally producing no spots.
func (c *Client) poll(ctx context.Context, startup bool) bool {
	now := c.nowUTC()
	records, err := c.fetch(ctx, now)
	if err != nil {
		c.requestErrors.Add(1)
		c.recordFailure(now, err)
		return false
	}
	c.recordSuccess(now)
	if len(records) >= c.cfg.MaxRecordsPerPoll {
		total := c.truncationWarnings.Add(1)
		if c.logf != nil {
			if count, ok := c.truncationLogs.Inc(); ok {
				c.logf("DXSummit: response contained max_records_per_poll=%d rows; possible truncation (events=%d total=%d)", c.cfg.MaxRecordsPerPoll, count, total)
			}
		}
	}

	lastSeen := c.readLastSeenID()
	maxID := lastSeen
	spots := make([]parsedSpot, 0, len(records))
	startupCutoff := time.Time{}
	if startup && c.cfg.StartupBackfillSeconds > 0 {
		startupCutoff = now.Add(-time.Duration(c.cfg.StartupBackfillSeconds) * time.Second)
	}
	badCallReporter := c.badCallReporterSnapshot()
	for _, record := range records {
		// Keep only the highest DXSummit row ID. This avoids an unbounded seen-ID
		// map; rows at or below the prior high-water mark are treated as duplicates
		// within the configured lookback window.
		if record.ID > maxID {
			maxID = record.ID
		}
		if record.ID <= lastSeen {
			c.duplicateRows.Add(1)
			continue
		}
		if startup && c.cfg.StartupBackfillSeconds == 0 {
			continue
		}
		sp, err := parseRecordWithReporter(record, badCallReporter)
		if err != nil {
			c.parseErrors.Add(1)
			c.recordParseError(now, err)
			continue
		}
		if startup && !startupCutoff.IsZero() && sp.Time.Before(startupCutoff) {
			continue
		}
		spots = append(spots, parsedSpot{id: record.ID, spot: sp})
	}
	c.writeLastSeenID(maxID)

	sort.Slice(spots, func(i, j int) bool {
		return spots[i].id < spots[j].id
	})
	for _, parsed := range spots {
		c.emit(parsed.spot)
	}
	return true
}

type parsedSpot struct {
	id   uint64
	spot *spot.Spot
}

func (c *Client) fetch(ctx context.Context, now time.Time) ([]rawSpot, error) {
	reqCtx, cancel := context.WithTimeout(ctx, time.Duration(c.cfg.RequestTimeoutMS)*time.Millisecond)
	defer cancel()

	// Always send both endpoints of the time window. DXSummit may return a full
	// limited page when only from_time is supplied, which can hide recent rows.
	end := now.UTC()
	start := end.Add(-time.Duration(c.cfg.LookbackSeconds) * time.Second)
	uri, err := c.requestURL(start, end)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("dxsummit: HTTP status %d", resp.StatusCode)
	}
	// Cap the response body before JSON decoding so a bad or changed upstream
	// response cannot grow process memory beyond max_response_bytes.
	limit := c.cfg.MaxResponseBytes
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		c.responseTooLarge.Add(1)
		return nil, fmt.Errorf("dxsummit: response exceeded max_response_bytes=%d", limit)
	}
	var records []rawSpot
	if err := json.Unmarshal(body, &records); err != nil {
		c.parseErrors.Add(1)
		c.recordParseError(now, err)
		return nil, err
	}
	c.mu.Lock()
	c.lastStatusCode = resp.StatusCode
	c.mu.Unlock()
	return records, nil
}

func (c *Client) requestURL(start, end time.Time) (string, error) {
	parsed, err := url.Parse(c.cfg.BaseURL)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Set("limit", strconv.Itoa(c.cfg.MaxRecordsPerPoll))
	query.Set("from_time", strconv.FormatInt(start.UTC().Unix(), 10))
	query.Set("to_time", strconv.FormatInt(end.UTC().Unix(), 10))
	query.Set("include", strings.Join(c.cfg.IncludeBands, ","))
	query.Set("refresh", strconv.FormatInt(end.UTC().UnixMilli(), 10))
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

// parseRecord maps one DXSummit JSON row into the shared spot model. DXSummit
// coordinates are deliberately ignored here; grids can still be filled later by
// the existing CTY/grid-cache enrichment path from callsign-derived metadata.
func parseRecord(record rawSpot) (*spot.Spot, error) {
	return parseRecordWithReporter(record, nil)
}

func parseRecordWithReporter(record rawSpot, badCallReporter BadCallReporter) (*spot.Spot, error) {
	if record.ID == 0 {
		return nil, errors.New("dxsummit: missing id")
	}
	freq := record.Frequency
	if freq <= 0 {
		return nil, errors.New("dxsummit: invalid frequency")
	}
	if spot.FreqToBand(freq) == "???" {
		return nil, fmt.Errorf("dxsummit: unsupported frequency %.1f", freq)
	}
	mode := parseRecordMode(record.Info, freq)
	dxCall := spot.NormalizeSpotDXCallsign(record.DXCall)
	if !spot.IsValidNormalizedCallsign(dxCall) {
		reportDXSummitBadCall(badCallReporter, "DX", "invalid_callsign", record.DXCall, record.DECall, record.DXCall, mode)
		return nil, fmt.Errorf("dxsummit: invalid DX call %q", record.DXCall)
	}
	deCall, _, ok := normalizeSpotterCall(record.DECall)
	if !ok {
		reportDXSummitBadCall(badCallReporter, "DE", "invalid_callsign", record.DECall, record.DECall, record.DXCall, mode)
		return nil, fmt.Errorf("dxsummit: invalid DE call %q", record.DECall)
	}
	sourceTime, err := parseAPITime(record.Time)
	if err != nil {
		return nil, err
	}
	info := ""
	if record.Info != nil {
		info = *record.Info
	}
	parsedComment := spot.ParseHumanSpotComment(info, freq)
	sp := spot.NewSpotNormalized(dxCall, deCall, freq, parsedComment.Mode)
	sp.Time = sourceTime
	sp.Comment = parsedComment.Comment
	sp.Events = parsedComment.Events
	sp.Report = parsedComment.Report
	sp.HasReport = parsedComment.HasReport
	sp.SourceType = spot.SourceUpstream
	sp.SourceNode = SourceNode
	sp.IsHuman = true
	if parsedComment.Mode != "" {
		sp.ModeProvenance = spot.ModeProvenanceCommentExplicit
	}
	sp.EnsureNormalized()
	sp.RefreshBeaconFlag()
	return sp, nil
}

func (c *Client) badCallReporterSnapshot() BadCallReporter {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.badCallReporter
}

func parseRecordMode(info *string, freq float64) string {
	if info == nil {
		return ""
	}
	return spot.ParseHumanSpotComment(*info, freq).Mode
}

func reportDXSummitBadCall(reporter BadCallReporter, role, reason, call, deCall, dxCall, mode string) {
	if reporter == nil {
		return
	}
	reporter(SourceNode, role, reason, call, deCall, dxCall, mode, "source_parser")
}

func parseAPITime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, errors.New("dxsummit: missing time")
	}
	if ts, err := time.ParseInLocation(apiTimeLayout, value, time.UTC); err == nil {
		return ts.UTC(), nil
	}
	ts, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("dxsummit: invalid time %q: %w", value, err)
	}
	return ts.UTC(), nil
}

// normalizeSpotterCall returns both the display callsign and the base lookup
// callsign. A final "-@" marker is DXSummit source provenance and is preserved
// for display/archive output, but embedded or malformed "@" forms are rejected.
// A final "-#" skimmer marker is stripped when DXSummit relays one.
func normalizeSpotterCall(raw string) (display string, base string, ok bool) {
	trimmed := strings.ToUpper(strings.TrimSpace(raw))
	if trimmed == "" {
		return "", "", false
	}
	if strings.Contains(trimmed, "@") {
		if !strings.HasSuffix(trimmed, "-@") || strings.Count(trimmed, "@") != 1 {
			return "", "", false
		}
		base = spot.NormalizeCallsign(strings.TrimSuffix(trimmed, "-@"))
		if !spot.IsValidNormalizedCallsign(base) {
			return "", "", false
		}
		return base + "-@", base, true
	}
	trimmed = stripTerminalSkimmerMarker(trimmed)
	base = spot.NormalizeCallsign(trimmed)
	if !spot.IsValidNormalizedCallsign(base) {
		return "", "", false
	}
	return base, base, true
}

func stripTerminalSkimmerMarker(call string) string {
	return strings.TrimSuffix(call, "-#")
}

// emit never blocks the polling goroutine. When the bounded output channel is
// full, the newest DXSummit row is dropped and counted so other ingest sources
// and shutdown are not held behind this feed.
func (c *Client) emit(sp *spot.Spot) {
	if sp == nil {
		return
	}
	select {
	case c.spotChan <- sp:
		c.emittedSpots.Add(1)
		now := c.nowUTC()
		c.mu.Lock()
		c.lastSpotAt = now
		c.mu.Unlock()
	default:
		total := c.spotDrops.Add(1)
		if c.logf != nil {
			if count, ok := c.queueDropLogs.Inc(); ok {
				c.logf("DXSummit: spot channel full, dropping spot (events=%d total=%d)", count, total)
			}
		}
	}
}

func (c *Client) nowUTC() time.Time {
	c.mu.Lock()
	now := c.now
	c.mu.Unlock()
	return now().UTC()
}

func (c *Client) readLastSeenID() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastSeenID
}

func (c *Client) writeLastSeenID(id uint64) {
	c.mu.Lock()
	if id > c.lastSeenID {
		c.lastSeenID = id
	}
	c.mu.Unlock()
}

func (c *Client) recordSuccess(now time.Time) {
	c.mu.Lock()
	c.connected = true
	c.lastPollAt = now
	c.lastMessageAt = now
	c.lastError = ""
	c.mu.Unlock()
}

func (c *Client) recordFailure(now time.Time, err error) {
	c.mu.Lock()
	c.connected = false
	c.lastPollAt = now
	c.lastError = err.Error()
	c.mu.Unlock()
}

func (c *Client) recordParseError(now time.Time, err error) {
	c.mu.Lock()
	c.lastParseErrAt = now
	c.lastError = err.Error()
	c.mu.Unlock()
}
