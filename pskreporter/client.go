// Package pskreporter subscribes to PSKReporter MQTT feeds (FT8/FT4/CW/RTTY)
// and converts incoming JSON payloads into canonical *spot.Spot records.
package pskreporter

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/skew"
	"dxcluster/spot"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	jsoniter "github.com/json-iterator/go"
)

// normalizedPSK carries normalized tokens to avoid repeated string operations per message.
// All string fields are uppercased and trimmed.
type normalizedPSK struct {
	modeUpper   string // canonical mode (e.g., PSK)
	displayMode string // original token (e.g., PSK31)
	dxCall      string
	deCall      string
	dxGrid      string
	deGrid      string
	freqKHz     float64
}

// pskModeInfo carries canonical mode data for allowlist checks and conversion.
type pskModeInfo struct {
	canonical string
	variant   string
	isPSK     bool
}

// Client represents a PSKReporter MQTT client
type Client struct {
	broker       string
	port         int
	topics       []string
	name         string
	client       mqtt.Client
	spotChan     chan *spot.Spot
	pathOnlyChan chan *spot.Spot
	shutdown     chan struct{}
	workers      int
	processing   chan []byte
	payloadPool  chan []byte
	// payloadPoolMaxBytes caps pooled buffer sizes to avoid retaining large arrays.
	payloadPoolMaxBytes int
	processingMu        sync.RWMutex
	workerWg            sync.WaitGroup
	skewStore           *skew.Store
	appendSSID          bool
	spotterCache        *spot.CallCache

	maxPayloadBytes         int
	mqttInboundWorkers      int
	mqttInboundQueueDepth   int
	mqttQoS12EnqueueTimeout time.Duration

	lastPayloadAt  atomic.Int64
	lastSpotAt     atomic.Int64
	lastParseErrAt atomic.Int64

	payloadDropCounter     rateCounter
	payloadTooLargeCounter rateCounter
	spotDropCounter        rateCounter
	parseErrorCounter      rateCounter
	pathOnlyDropCounter    rateCounter

	allowAllModes bool
	allowedModes  map[string]struct{}
	pathOnlyModes map[string]struct{}
	hasPathOnly   bool
}

var (
	jsonFast   = jsoniter.ConfigFastest
	jsonCompat = jsoniter.ConfigCompatibleWithStandardLibrary
)

// PSKRMessage represents a PSKReporter MQTT message
type PSKRMessage struct {
	SequenceNumber  uint64 `json:"sq"` // Sequence number
	Frequency       int64  `json:"f"`  // Frequency in Hz
	Mode            string `json:"md"` // Mode (FT8, FT4, etc.)
	Report          *int   `json:"rp"` // SNR report in dB (nil when missing)
	Timestamp       int64  `json:"t"`  // Unix timestamp in seconds (seconds since 1970-01-01)
	SenderCall      string `json:"sc"` // Sender (DX) callsign
	SenderLocator   string `json:"sl"` // Sender grid locator
	ReceiverCall    string `json:"rc"` // Receiver (spotter) callsign
	ReceiverLocator string `json:"rl"` // Receiver grid locator
	SenderCountry   int    `json:"sa"` // Sender ADIF country code
	ReceiverCountry int    `json:"ra"` // Receiver ADIF country code
	Band            string `json:"b"`  // Band (e.g., "20m", "15m")
}

// Purpose: Canonicalize a PSKReporter mode token once for filtering + conversion.
// Key aspects: Returns canonical/variant/isPSK; empty canonical means invalid.
// Upstream: handlePayload allowlist checks.
// Downstream: normalizeMessage conversion path.
func parseModeInfo(raw string) (pskModeInfo, bool) {
	canonical, variant, isPSK := spot.CanonicalPSKMode(raw)
	if canonical == "" {
		return pskModeInfo{}, false
	}
	if variant == "" {
		variant = canonical
	}
	return pskModeInfo{
		canonical: canonical,
		variant:   variant,
		isPSK:     isPSK,
	}, true
}

const (
	pskReporterQueuePerWorker = 256
	mqttInboundQueuePerWorker = 256
	mqttInboundQueueDepthCap  = 4096
	defaultSpotBuffer         = 25000
	defaultMaxPayloadBytes    = 4096
	defaultDropLogInterval    = 10 * time.Second
	envPSKReporterMQTTDebug   = "DXC_PSKR_MQTT_DEBUG"
	payloadPoolMaxCountCap    = 1024      // hard cap on pooled buffers
	payloadPoolMaxBytesCap    = 16 * 1024 // avoid retaining large payload arrays
)

var (
	callCacheSize = 4096
	callCacheTTL  = 10 * time.Minute
	pahoDebugOnce sync.Once
)

// ConfigureCallCache configures the callsign normalization cache for PSKReporter.
// Key aspects: Applies sane defaults when size/ttl are zero.
// Upstream: Config load or tests.
// Downstream: callCacheSize/callCacheTTL globals.
func ConfigureCallCache(size int, ttl time.Duration) {
	if size <= 0 {
		size = 4096
	}
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	callCacheSize = size
	callCacheTTL = ttl
}

// NewClient constructs a PSKReporter MQTT client.
// Key aspects: Initializes channels, caches, and worker settings.
// Upstream: main.go startup.
// Downstream: Client.Connect, worker pool.
func NewClient(broker string, port int, topics []string, allowedModes []string, pathOnlyModes []string, name string, workers int, mqttInboundWorkers int, mqttInboundQueueDepth int, mqttQoS12EnqueueTimeout time.Duration, skewStore *skew.Store, appendSSID bool, spotBuffer int, maxPayloadBytes int) *Client {
	if spotBuffer <= 0 {
		spotBuffer = defaultSpotBuffer
	}
	if maxPayloadBytes <= 0 {
		maxPayloadBytes = defaultMaxPayloadBytes
	}
	modeSet := make(map[string]struct{}, len(allowedModes))
	for _, m := range allowedModes {
		m = strings.ToUpper(strings.TrimSpace(m))
		if m == "" {
			continue
		}
		modeSet[m] = struct{}{}
	}
	pathOnlySet := make(map[string]struct{}, len(pathOnlyModes))
	for _, m := range pathOnlyModes {
		m = strings.ToUpper(strings.TrimSpace(m))
		if m == "" {
			continue
		}
		pathOnlySet[m] = struct{}{}
	}
	allowAll := len(modeSet) == 0
	hasPathOnly := len(pathOnlySet) > 0
	var pathOnlyChan chan *spot.Spot
	if hasPathOnly {
		pathOnlyChan = make(chan *spot.Spot, spotBuffer)
	}
	return &Client{
		broker:                  broker,
		port:                    port,
		topics:                  append([]string{}, topics...),
		allowedModes:            modeSet,
		allowAllModes:           allowAll,
		pathOnlyModes:           pathOnlySet,
		hasPathOnly:             hasPathOnly,
		name:                    name,
		spotChan:                make(chan *spot.Spot, spotBuffer), // Buffered ingest to absorb bursts
		pathOnlyChan:            pathOnlyChan,
		shutdown:                make(chan struct{}),
		workers:                 workers,
		mqttInboundWorkers:      mqttInboundWorkers,
		mqttInboundQueueDepth:   mqttInboundQueueDepth,
		mqttQoS12EnqueueTimeout: mqttQoS12EnqueueTimeout,
		skewStore:               skewStore,
		appendSSID:              appendSSID,
		spotterCache:            spot.NewCallCache(callCacheSize, callCacheTTL),
		maxPayloadBytes:         maxPayloadBytes,

		payloadDropCounter:     newRateCounter(),
		payloadTooLargeCounter: newRateCounter(),
		spotDropCounter:        newRateCounter(),
		parseErrorCounter:      newRateCounter(),
		pathOnlyDropCounter:    newRateCounter(),
	}
}

// Purpose: Enable Paho MQTT debug logging when the env flag is set.
// Key aspects: Validates the env var and installs loggers once.
// Upstream: Connect.
// Downstream: mqtt.* logger globals.
func enablePahoDebugLogging() {
	raw := strings.TrimSpace(os.Getenv(envPSKReporterMQTTDebug))
	if raw == "" {
		return
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		log.Printf("PSKReporter: ignoring invalid %s=%q", envPSKReporterMQTTDebug, raw)
		return
	}
	if !enabled {
		return
	}
	pahoDebugOnce.Do(func() {
		writer := log.Writer()
		mqtt.DEBUG = log.New(writer, "PSKReporter MQTT DEBUG: ", 0)
		mqtt.WARN = log.New(writer, "PSKReporter MQTT WARN: ", 0)
		mqtt.ERROR = log.New(writer, "PSKReporter MQTT ERROR: ", 0)
		mqtt.CRITICAL = log.New(writer, "PSKReporter MQTT CRITICAL: ", 0)
		log.Printf("PSKReporter: Paho MQTT debug logging enabled via %s", envPSKReporterMQTTDebug)
	})
}

// Connect connects to the PSKReporter MQTT broker and starts workers.
// Key aspects: Configures reconnect/keepalive and sets handlers.
// Upstream: main.go startup or reconnect flows.
// Downstream: startWorkerPool, mqtt.Client.Connect.
func (c *Client) Connect() error {
	enablePahoDebugLogging()
	c.startWorkerPool()
	// Create MQTT client options
	opts := mqtt.NewClientOptions()
	brokerURL := fmt.Sprintf("tcp://%s:%d", c.broker, c.port)
	opts.AddBroker(brokerURL)

	// Set client ID with timestamp for uniqueness
	clientID := fmt.Sprintf("gocluster-%d", time.Now().UTC().Unix())
	opts.SetClientID(clientID)

	// Set keep alive and timeouts
	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetCleanSession(true)
	opts.SetOrderMatters(false)

	mqttWorkers := c.mqttInboundWorkers
	if mqttWorkers <= 0 {
		mqttWorkers = c.workers
	}
	if mqttWorkers <= 0 {
		mqttWorkers = defaultPSKReporterWorkers()
	}
	queueDepth := c.mqttInboundQueueDepth
	if queueDepth <= 0 {
		queueDepth = mqttWorkers * mqttInboundQueuePerWorker
		if queueDepth > mqttInboundQueueDepthCap {
			queueDepth = mqttInboundQueueDepthCap
		}
	}
	if c.mqttQoS12EnqueueTimeout < 0 {
		c.mqttQoS12EnqueueTimeout = 0
	}
	c.mqttInboundWorkers = mqttWorkers
	c.mqttInboundQueueDepth = queueDepth
	opts.SetInboundPublishWorkers(mqttWorkers)
	opts.SetInboundPublishQueueDepth(uint(queueDepth))
	opts.SetInboundPublishEnqueueTimeout(c.mqttQoS12EnqueueTimeout)
	log.Printf("PSKReporter: MQTT inbound queue depth=%d workers=%d qos12_timeout=%s", queueDepth, mqttWorkers, c.mqttQoS12EnqueueTimeout)

	// Set auto-reconnect
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(1 * time.Minute)

	// Set connection handlers
	opts.SetOnConnectHandler(c.onConnect)
	opts.SetConnectionLostHandler(c.onConnectionLost)

	// Create MQTT client
	c.client = mqtt.NewClient(opts)

	topicDesc := strings.Join(c.topics, ",")
	if topicDesc == "" {
		topicDesc = "<none>"
	}
	if c.name != "" {
		log.Printf("Connecting to %s MQTT broker at %s (topics=%s)...", c.name, brokerURL, topicDesc)
	} else {
		log.Printf("Connecting to PSKReporter MQTT broker at %s (topics=%s)...", brokerURL, topicDesc)
	}

	// Connect to broker
	token := c.client.Connect()
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to connect to PSKReporter: %w", token.Error())
	}

	log.Println("Connected to PSKReporter MQTT broker")

	return nil
}

// Purpose: Subscribe to configured topics on connect.
// Key aspects: Logs subscription results and aborts on failure.
// Upstream: MQTT on-connect callback.
// Downstream: client.Subscribe, messageHandler.
func (c *Client) onConnect(client mqtt.Client) {
	log.Printf("PSKReporter: Connected, subscribing to %d topic(s)", len(c.topics))
	for _, topic := range c.topics {
		token := client.Subscribe(topic, 0, c.messageHandler)
		if token.Wait() && token.Error() != nil {
			log.Printf("PSKReporter: Failed to subscribe to %s: %v", topic, token.Error())
			return
		}
		log.Printf("PSKReporter: Subscribed to %s", topic)
	}

	log.Println("PSKReporter: Successfully subscribed, receiving spots...")
}

// Purpose: Log connection loss events.
// Key aspects: Relies on MQTT auto-reconnect for recovery.
// Upstream: MQTT connection-lost callback.
// Downstream: None.
func (c *Client) onConnectionLost(client mqtt.Client, err error) {
	snap := c.HealthSnapshot()
	now := time.Now().UTC()
	log.Printf("PSKReporter: Connection lost: %v (last_payload=%s last_spot=%s last_parse_err=%s processing=%d/%d mqtt_q=%d/%d spot_queue=%d/%d path_only_q=%d/%d drops payload=%d oversize=%d spot=%d path_only=%d parse=%d mqtt_drop=%d mqtt_qos12_timeout=%d mqtt_qos12_disconnect=%d)",
		err,
		formatAge(now, snap.LastPayloadAt),
		formatAge(now, snap.LastSpotAt),
		formatAge(now, snap.LastParseErrAt),
		snap.ProcessingQueueLen,
		snap.ProcessingQueueCap,
		snap.MQTTInboundQueueLen,
		snap.MQTTInboundQueueCap,
		snap.SpotQueueLen,
		snap.SpotQueueCap,
		snap.PathOnlyQueueLen,
		snap.PathOnlyQueueCap,
		snap.PayloadDrops,
		snap.PayloadTooLarge,
		snap.SpotDrops,
		snap.PathOnlyDrops,
		snap.ParseErrors,
		snap.MQTTInboundDropsQoS0,
		snap.MQTTInboundQoS12Timeouts,
		snap.MQTTInboundQoS12Disconnects)
	log.Println("PSKReporter: Will attempt to reconnect...")
}

// Purpose: Initialize a bounded payload buffer pool to reduce per-message allocs.
// Key aspects: Bounded by size and count; non-blocking get/put.
// Upstream: startWorkerPool.
// Downstream: messageHandler/workerLoop.
func (c *Client) initPayloadPool(queueCapacity int) {
	if c == nil || c.payloadPool != nil {
		return
	}
	if queueCapacity <= 0 || c.maxPayloadBytes <= 0 {
		return
	}
	poolCap := queueCapacity
	if poolCap > payloadPoolMaxCountCap {
		poolCap = payloadPoolMaxCountCap
	}
	if poolCap <= 0 {
		return
	}
	maxBytes := c.maxPayloadBytes
	if maxBytes > payloadPoolMaxBytesCap {
		maxBytes = payloadPoolMaxBytesCap
	}
	if maxBytes <= 0 {
		return
	}
	c.payloadPoolMaxBytes = maxBytes
	c.payloadPool = make(chan []byte, poolCap)
}

// Purpose: Acquire a payload buffer (from pool when available).
// Key aspects: Non-blocking; allocates when pool empty or size exceeds cap.
// Upstream: messageHandler.
// Downstream: handlePayload.
func (c *Client) acquirePayloadBuffer(size int) []byte {
	if c == nil || size <= 0 {
		return nil
	}
	pool := c.payloadPool
	if pool == nil || c.payloadPoolMaxBytes <= 0 || size > c.payloadPoolMaxBytes {
		return make([]byte, size)
	}
	select {
	case buf := <-pool:
		if cap(buf) < size {
			return make([]byte, size)
		}
		return buf[:size]
	default:
		return make([]byte, size)
	}
}

// Purpose: Return a payload buffer to the pool when eligible.
// Key aspects: Non-blocking; skips oversized buffers.
// Upstream: workerLoop + messageHandler drop paths.
// Downstream: payloadPool channel.
func (c *Client) releasePayloadBuffer(buf []byte) {
	if c == nil || buf == nil {
		return
	}
	pool := c.payloadPool
	if pool == nil || c.payloadPoolMaxBytes <= 0 {
		return
	}
	if cap(buf) == 0 || cap(buf) > c.payloadPoolMaxBytes {
		return
	}
	buf = buf[:0]
	select {
	case pool <- buf:
	default:
	}
}

// Purpose: Enqueue incoming MQTT payloads for worker processing.
// Key aspects: Copies payload to avoid reuse; drops on full queue.
// Upstream: MQTT subscription callback.
// Downstream: processing channel.
func (c *Client) messageHandler(client mqtt.Client, msg mqtt.Message) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PSKReporter: panic in messageHandler: %v", r)
		}
	}()
	select {
	case <-c.shutdown:
		return
	default:
	}
	raw := msg.Payload()
	now := time.Now().UTC()
	c.lastPayloadAt.Store(now.UnixNano())
	if c.maxPayloadBytes > 0 && len(raw) > c.maxPayloadBytes {
		if count, ok := c.payloadTooLargeCounter.Inc(); ok {
			log.Printf("PSKReporter: payload %d bytes exceeds limit %d, dropped (total=%d)", len(raw), c.maxPayloadBytes, count)
		}
		return
	}
	c.processingMu.RLock()
	processing := c.processing
	c.processingMu.RUnlock()
	if processing == nil {
		if count, ok := c.payloadDropCounter.Inc(); ok {
			log.Printf("PSKReporter: Processing queue unavailable, dropped %d payload(s) total", count)
		}
		return
	}
	payload := c.acquirePayloadBuffer(len(raw))
	if payload == nil {
		return
	}
	copy(payload, raw)
	select {
	case <-c.shutdown:
		c.releasePayloadBuffer(payload)
		return
	case processing <- payload:
		// payload enqueued for workers
	default:
		c.releasePayloadBuffer(payload)
		if count, ok := c.payloadDropCounter.Inc(); ok {
			log.Printf("PSKReporter: Processing queue full, dropped %d payload(s) total", count)
		}
	}
}

// Purpose: Start the PSKReporter worker pool and processing queue.
// Key aspects: Sizes queue based on worker count; idempotent on repeat calls.
// Upstream: Connect.
// Downstream: workerLoop goroutines.
func (c *Client) startWorkerPool() {
	if c.workers <= 0 {
		c.workers = defaultPSKReporterWorkers()
	}
	c.processingMu.Lock()
	if c.processing != nil {
		c.processingMu.Unlock()
		return
	}
	capacity := c.workers * pskReporterQueuePerWorker
	if capacity < 256 {
		capacity = 256
	}
	processing := make(chan []byte, capacity)
	c.processing = processing
	c.initPayloadPool(capacity)
	c.processingMu.Unlock()
	c.workerWg.Add(c.workers)
	for i := 0; i < c.workers; i++ {
		// Goroutine: worker loop parses payloads into spots.
		go c.workerLoop(i, processing)
	}
}

// Purpose: Process payloads from the processing queue.
// Key aspects: Stops on shutdown; recovers from panics to avoid worker loss.
// Upstream: startWorkerPool goroutine.
// Downstream: handlePayload.
func (c *Client) workerLoop(id int, processing <-chan []byte) {
	log.Printf("PSKReporter worker %d started", id)
	defer c.workerWg.Done()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PSKReporter worker %d panic: %v", id, r)
		}
	}()
	for {
		select {
		case <-c.shutdown:
			return
		case payload := <-processing:
			c.handlePayload(payload)
			c.releasePayloadBuffer(payload)
		}
	}
}

// Purpose: Parse a payload and emit a spot to the output channel.
// Key aspects: Validates JSON and drops malformed inputs.
// Upstream: workerLoop.
// Downstream: convertToSpot, spotChan.
func (c *Client) handlePayload(payload []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PSKReporter: panic in handlePayload: %v", r)
		}
	}()
	var pskrMsg PSKRMessage
	if err := jsonFast.Unmarshal(payload, &pskrMsg); err != nil {
		if errCompat := jsonCompat.Unmarshal(payload, &pskrMsg); errCompat != nil {
			c.lastParseErrAt.Store(time.Now().UTC().UnixNano())
			if count, ok := c.parseErrorCounter.Inc(); ok {
				log.Printf("PSKReporter: Failed to parse message (total=%d): %v", count, errCompat)
			}
			return
		}
	}
	if pskrMsg.Report == nil || *pskrMsg.Report == 0 {
		return
	}
	modeInfo, ok := parseModeInfo(pskrMsg.Mode)
	if !ok {
		return
	}
	pathOnly := c.hasPathOnly && c.modeMatchesList(modeInfo.canonical, modeInfo.variant, modeInfo.isPSK, c.pathOnlyModes)
	if !c.allowAllModes {
		if !pathOnly && !c.modeMatchesList(modeInfo.canonical, modeInfo.variant, modeInfo.isPSK, c.allowedModes) {
			return
		}
	}
	s := c.convertToSpot(&pskrMsg, modeInfo)
	if s == nil {
		return
	}
	if pathOnly {
		c.lastSpotAt.Store(time.Now().UTC().UnixNano())
		select {
		case c.pathOnlyChan <- s:
		default:
			if count, ok := c.pathOnlyDropCounter.Inc(); ok {
				log.Printf("PSKReporter: Path-only channel full, dropped %d spot(s) total", count)
			}
		}
		return
	}
	c.lastSpotAt.Store(time.Now().UTC().UnixNano())
	select {
	case c.spotChan <- s:
	default:
		if count, ok := c.spotDropCounter.Inc(); ok {
			log.Printf("PSKReporter: Spot channel full, dropped %d spot(s) total", count)
		}
	}
}

// Purpose: Convert a PSKReporter message into a canonical Spot.
// Key aspects: Preserves observation timestamp; applies skew correction and attaches grids.
// Upstream: handlePayload.
// Downstream: normalizeMessage, skew.ApplyCorrection.
// convertToSpot converts PSKReporter message to our Spot format.
// modeInfo must come from parseModeInfo to avoid re-normalization.
// IMPORTANT: This function sets the spot's Time field to the actual observation time
// from the PSKReporter message, NOT the current system time. This is critical for
// deduplication to work correctly, as the Hash32() function includes the timestamp
// truncated to the minute. If we used the current time instead of the observation time,
// identical spots arriving a few seconds apart could cross a minute boundary and
// generate different hashes, preventing proper deduplication.
func (c *Client) convertToSpot(msg *PSKRMessage, modeInfo pskModeInfo) *spot.Spot {
	// Validate required fields
	if msg.SenderCall == "" || msg.ReceiverCall == "" || msg.Frequency == 0 {
		return nil
	}

	norm := c.normalizeMessage(msg, modeInfo)
	if norm == nil {
		return nil
	}

	dxCall := norm.dxCall
	deCall := norm.deCall
	if !spot.IsValidNormalizedCallsign(dxCall) {
		// log.Printf("PSKReporter: invalid DX call %s", msg.SenderCall) // noisy: caller requested silence
		return nil
	}
	if !spot.IsValidNormalizedCallsign(deCall) {
		// log.Printf("PSKReporter: invalid DE call %s", msg.ReceiverCall) // noisy: caller requested silence
		return nil
	}

	// Validate timestamp is reasonable (not zero, not too far in past/future)
	// PSKReporter timestamps are Unix timestamps in seconds
	if msg.Timestamp == 0 {
		log.Printf("PSKReporter: Invalid timestamp (zero) for spot %s", msg.SenderCall)
		return nil
	}

	// Check if timestamp is within a reasonable range (not more than 1 day old or in the future)
	spotTime := time.Unix(msg.Timestamp, 0)
	age := time.Since(spotTime)
	if age < -1*time.Hour {
		log.Printf("PSKReporter: Spot timestamp is in the future: %s (age: %v)", spotTime.Format(time.RFC3339), age)
		return nil
	}
	if age > 24*time.Hour {
		log.Printf("PSKReporter: Spot timestamp is too old: %s (age: %v)", spotTime.Format(time.RFC3339), age)
		return nil
	}

	// Convert frequency from Hz to kHz and apply skimmer correction for CW/RTTY
	freqKHz := norm.freqKHz
	if isCWorRTTY(norm.modeUpper) {
		freqKHz = skew.ApplyCorrection(c.skewStore, norm.deCall, freqKHz)
	}

	// Create spot
	// In PSKReporter: sender = DX station, receiver = spotter
	// In our model: DXCall = sender, DECall = receiver
	s := spot.NewSpotNormalized(dxCall, deCall, freqKHz, norm.displayMode)
	s.ModeNorm = norm.modeUpper
	s.IsHuman = false

	// CRITICAL: Set the actual observation timestamp from PSKReporter
	// This overwrites the time.Now() that was set in NewSpot()
	// The PSKReporter "t" field contains the Unix timestamp (seconds since epoch)
	// when the signal was actually observed, which is what we need for accurate
	// deduplication across multiple spotters reporting the same signal
	s.Time = spotTime

	// Set report (SNR in dB) when present.
	if msg.Report != nil {
		s.Report = *msg.Report
		s.HasReport = true
	}

	// Comment intentionally left empty for PSKReporter; grids are stored in metadata
	// and rendered in the DX cluster tail to avoid duplicating payload.

	// Set source type and node
	s.SourceType = spot.SourcePSKReporter
	s.SourceNode = "PSKREPORTER"

	s.DXMetadata.Grid = norm.dxGrid
	s.DEMetadata.Grid = norm.deGrid

	s.RefreshBeaconFlag()

	return s
}

// Purpose: Report whether a mode is CW or RTTY.
// Key aspects: Expects normalized/uppercased mode input.
// Upstream: convertToSpot.
// Downstream: None.
func isCWorRTTY(mode string) bool {
	// mode is expected to be uppercased/trimmed by normalizeMessage.
	return mode == "CW" || mode == "RTTY"
}

// Purpose: Normalize and validate a PSKReporter message.
// Key aspects: Uses pre-parsed mode info, validates frequency, normalizes callsigns, uppercases grids.
// Upstream: convertToSpot.
// Downstream: spot.NormalizeCallsign, decorateSpotterCall.
func (c *Client) normalizeMessage(msg *PSKRMessage, modeInfo pskModeInfo) *normalizedPSK {
	if msg == nil {
		return nil
	}
	if modeInfo.canonical == "" || msg.SenderCall == "" || msg.ReceiverCall == "" {
		return nil
	}
	modeUpper := modeInfo.canonical
	displayMode := modeInfo.variant
	if displayMode == "" {
		displayMode = modeUpper
	}
	freqKHz := float64(msg.Frequency) / 1000.0
	if freqKHz <= 0 {
		return nil
	}
	dxCall := spot.NormalizeCallsign(msg.SenderCall)
	deCall := c.decorateSpotterCall(msg.ReceiverCall)
	dxGrid := strings.ToUpper(strings.TrimSpace(msg.SenderLocator))
	deGrid := strings.ToUpper(strings.TrimSpace(msg.ReceiverLocator))
	return &normalizedPSK{
		modeUpper:   modeUpper,
		displayMode: displayMode,
		dxCall:      dxCall,
		deCall:      deCall,
		dxGrid:      dxGrid,
		deGrid:      deGrid,
		freqKHz:     freqKHz,
	}
}

// Purpose: Normalize spotter callsign and optionally append an SSID suffix.
// Key aspects: Uses a cache; appends "-#" only when enabled and safe.
// Upstream: normalizeMessage.
// Downstream: spot.NormalizeCallsign, spot.CallCache.
func (c *Client) decorateSpotterCall(raw string) string {
	cacheKey := "0|" + raw
	if c.appendSSID {
		cacheKey = "1|" + raw
	}
	if cached, ok := c.spotterCache.Get(cacheKey); ok {
		return cached
	}
	normalized := spot.NormalizeCallsign(raw)
	if !c.appendSSID {
		c.spotterCache.Add(cacheKey, normalized)
		return normalized
	}
	if normalized == "" {
		c.spotterCache.Add(cacheKey, normalized)
		return normalized
	}
	if strings.Contains(normalized, "-") {
		c.spotterCache.Add(cacheKey, normalized)
		return normalized
	}
	// Appending "-#" increases length by 2. Skip if it would violate validation limits.
	if len(normalized)+2 > spot.MaxCallsignLength() {
		c.spotterCache.Add(cacheKey, normalized)
		return normalized
	}
	normalized = normalized + "-#"
	c.spotterCache.Add(cacheKey, normalized)
	return normalized
}

type rateCounter struct {
	interval time.Duration
	lastLog  atomic.Int64
	count    atomic.Uint64
}

func newRateCounter() rateCounter {
	return rateCounter{interval: defaultDropLogInterval}
}

// Inc increments a counter and decide if it's time to log.
// Key aspects: Uses atomics to avoid contention on hot paths.
// Upstream: Drop/error logging sites.
// Downstream: log.Printf when true is returned.
func (c *rateCounter) Inc() (uint64, bool) {
	if c == nil {
		return 0, false
	}
	total := c.count.Add(1)
	if c.interval <= 0 {
		return total, true
	}
	now := time.Now().UTC().UnixNano()
	last := c.lastLog.Load()
	if now-last < c.interval.Nanoseconds() {
		return total, false
	}
	if c.lastLog.CompareAndSwap(last, now) {
		return total, true
	}
	return total, false
}

// Purpose: Wait for workers to exit and release the processing queue.
// Key aspects: Blocks until workerWg completes.
// Upstream: Stop.
// Downstream: workerWg.Wait.
func (c *Client) stopWorkerPool() {
	c.workerWg.Wait()
	c.processingMu.Lock()
	c.processing = nil
	c.processingMu.Unlock()
}

// Purpose: Decide if a normalized mode is present in a mode list.
// Key aspects: Supports canonical + variant matching for PSK-family modes.
// Upstream: handlePayload allowlist and path-only routing.
// Downstream: map lookups only.
func (c *Client) modeMatchesList(canonical string, variant string, isPSK bool, allowed map[string]struct{}) bool {
	if len(allowed) == 0 {
		return false
	}
	if canonical != "" {
		if _, ok := allowed[canonical]; ok {
			return true
		}
	}
	if isPSK && variant != "" {
		if _, ok := allowed[variant]; ok {
			return true
		}
	}
	return false
}

// Purpose: Choose a default worker count for PSKReporter ingest.
// Key aspects: Uses NumCPU with a minimum of 4.
// Upstream: startWorkerPool.
// Downstream: runtime.NumCPU.
func defaultPSKReporterWorkers() int {
	workers := runtime.NumCPU()
	// PSKReporter bursts are high volume; keep a minimum of 4 workers even on small CPUs.
	if workers < 4 {
		workers = 4
	}
	return workers
}

// GetSpotChannel exposes the output spot channel.
// Key aspects: Read-only channel for downstream consumers.
// Upstream: main.go pipeline wiring.
// Downstream: None.
func (c *Client) GetSpotChannel() <-chan *spot.Spot {
	return c.spotChan
}

// GetPathOnlyChannel exposes the output path-only channel.
// Key aspects: Returns nil when path-only modes are disabled.
// Upstream: main.go path-only processor.
// Downstream: None.
func (c *Client) GetPathOnlyChannel() <-chan *spot.Spot {
	if c == nil || !c.hasPathOnly {
		return nil
	}
	return c.pathOnlyChan
}

// IsConnected reports whether the MQTT client is connected.
// Key aspects: Safe when client is nil.
// Upstream: Diagnostics or health checks.
// Downstream: mqtt.Client.IsConnected.
func (c *Client) IsConnected() bool {
	return c.client != nil && c.client.IsConnected()
}

func (c *Client) mqttInboundStats() mqtt.InboundPublishStats {
	if c == nil || c.client == nil {
		return mqtt.InboundPublishStats{}
	}
	stats, ok := c.client.(mqtt.InboundPublishStatsProvider)
	if !ok || stats == nil {
		return mqtt.InboundPublishStats{}
	}
	return stats.InboundPublishStats()
}

// HealthSnapshot reports recent activity and queue state for diagnostics.
type HealthSnapshot struct {
	Connected                   bool
	LastPayloadAt               time.Time
	LastSpotAt                  time.Time
	LastParseErrAt              time.Time
	ProcessingQueueLen          int
	ProcessingQueueCap          int
	MQTTInboundQueueLen         int
	MQTTInboundQueueCap         int
	SpotQueueLen                int
	SpotQueueCap                int
	PathOnlyQueueLen            int
	PathOnlyQueueCap            int
	PayloadDrops                uint64
	PayloadTooLarge             uint64
	SpotDrops                   uint64
	PathOnlyDrops               uint64
	ParseErrors                 uint64
	MQTTInboundDropsQoS0        uint64
	MQTTInboundQoS12Timeouts    uint64
	MQTTInboundQoS12Disconnects uint64
}

// HealthSnapshot provides a consistent ingest health snapshot.
// Key aspects: Uses atomics for timestamps and counters; locks to read queue state.
// Upstream: ingest health monitor.
// Downstream: None.
func (c *Client) HealthSnapshot() HealthSnapshot {
	if c == nil {
		return HealthSnapshot{}
	}
	snap := HealthSnapshot{
		Connected:    c.IsConnected(),
		SpotQueueLen: len(c.spotChan),
		SpotQueueCap: cap(c.spotChan),
		PathOnlyQueueLen: func() int {
			if c.pathOnlyChan == nil {
				return 0
			}
			return len(c.pathOnlyChan)
		}(),
		PathOnlyQueueCap: func() int {
			if c.pathOnlyChan == nil {
				return 0
			}
			return cap(c.pathOnlyChan)
		}(),
		PayloadDrops:    c.payloadDropCounter.count.Load(),
		PayloadTooLarge: c.payloadTooLargeCounter.count.Load(),
		SpotDrops:       c.spotDropCounter.count.Load(),
		PathOnlyDrops:   c.pathOnlyDropCounter.count.Load(),
		ParseErrors:     c.parseErrorCounter.count.Load(),
	}
	mqttStats := c.mqttInboundStats()
	snap.MQTTInboundQueueLen = mqttStats.QueueLen
	snap.MQTTInboundQueueCap = mqttStats.QueueCap
	snap.MQTTInboundDropsQoS0 = mqttStats.DroppedQoS0
	snap.MQTTInboundQoS12Timeouts = mqttStats.EnqueueTimeoutQoS12
	snap.MQTTInboundQoS12Disconnects = mqttStats.DisconnectsQoS12
	if ns := c.lastPayloadAt.Load(); ns > 0 {
		snap.LastPayloadAt = time.Unix(0, ns)
	}
	if ns := c.lastSpotAt.Load(); ns > 0 {
		snap.LastSpotAt = time.Unix(0, ns)
	}
	if ns := c.lastParseErrAt.Load(); ns > 0 {
		snap.LastParseErrAt = time.Unix(0, ns)
	}
	c.processingMu.RLock()
	processing := c.processing
	c.processingMu.RUnlock()
	if processing != nil {
		snap.ProcessingQueueLen = len(processing)
		snap.ProcessingQueueCap = cap(processing)
	}
	return snap
}

func formatAge(now time.Time, at time.Time) string {
	if at.IsZero() {
		return "never"
	}
	age := now.Sub(at)
	if age < 0 {
		age = 0
	}
	if age < time.Second {
		return "0s"
	}
	return age.Truncate(time.Second).String()
}

// Stop stops the PSKReporter client and worker pool.
// Key aspects: Unsubscribes, disconnects, and signals shutdown.
// Upstream: main.go shutdown.
// Downstream: stopWorkerPool, mqtt.Client.Disconnect.
func (c *Client) Stop() {
	log.Println("Stopping PSKReporter client...")

	if c.client != nil && c.client.IsConnected() {
		// Unsubscribe
		if len(c.topics) > 0 {
			c.client.Unsubscribe(c.topics...)
		}

		// Disconnect (wait up to 250ms for clean disconnect)
		c.client.Disconnect(250)
	}

	close(c.shutdown)

	c.stopWorkerPool()
	log.Println("PSKReporter client stopped")
}
