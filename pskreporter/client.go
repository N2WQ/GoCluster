// Package pskreporter subscribes to PSKReporter MQTT feeds (FT8/FT4/CW/RTTY)
// and converts incoming JSON payloads into canonical *spot.Spot records.
package pskreporter

import (
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	"dxcluster/cty"
	"dxcluster/skew"
	"dxcluster/spot"
	"dxcluster/uls"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	jsoniter "github.com/json-iterator/go"
)

// normalizedPSK carries normalized tokens to avoid repeated string operations per message.
// All string fields are uppercased and trimmed.
type normalizedPSK struct {
	modeUpper string
	dxCall    string
	deCall    string
	dxGrid    string
	deGrid    string
	freqKHz   float64
}

// Client represents a PSKReporter MQTT client
type Client struct {
	broker       string
	port         int
	topics       []string
	name         string
	client       mqtt.Client
	spotChan     chan *spot.Spot
	shutdown     chan struct{}
	workers      int
	lookup       func() *cty.CTYDatabase
	processing   chan []byte
	workerWg     sync.WaitGroup
	skewStore    *skew.Store
	appendSSID   bool
	spotterCache *spot.CallCache

	infoCache    map[string]infoCacheEntry
	infoCacheMu  sync.Mutex
	infoCacheTTL time.Duration

	unlicensedReporter UnlicensedReporter
	unlicensedQueue    chan unlicensedEvent
}

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// PSKRMessage represents a PSKReporter MQTT message
type PSKRMessage struct {
	SequenceNumber  uint64 `json:"sq"` // Sequence number
	Frequency       int64  `json:"f"`  // Frequency in Hz
	Mode            string `json:"md"` // Mode (FT8, FT4, etc.)
	Report          int    `json:"rp"` // SNR report in dB
	Timestamp       int64  `json:"t"`  // Unix timestamp in seconds (seconds since 1970-01-01)
	SenderCall      string `json:"sc"` // Sender (DX) callsign
	SenderLocator   string `json:"sl"` // Sender grid locator
	ReceiverCall    string `json:"rc"` // Receiver (spotter) callsign
	ReceiverLocator string `json:"rl"` // Receiver grid locator
	SenderCountry   int    `json:"sa"` // Sender ADIF country code
	ReceiverCountry int    `json:"ra"` // Receiver ADIF country code
	Band            string `json:"b"`  // Band (e.g., "20m", "15m")
}

const (
	pskReporterQueuePerWorker = 256
	defaultSpotBuffer         = 25000
)

var (
	callCacheSize = 4096
	callCacheTTL  = 10 * time.Minute
	infoCacheTTL  = 5 * time.Minute
)

// UnlicensedReporter receives drop notifications for US calls failing FCC license checks.
type UnlicensedReporter func(source, role, call, mode string, freqKHz float64)

type unlicensedEvent struct {
	source string
	role   string
	call   string
	mode   string
	freq   float64
}

// Purpose: Configure the callsign normalization cache for PSKReporter.
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

// Purpose: Install a reporter for unlicensed US drops.
// Key aspects: Uses a bounded queue and a background goroutine for delivery.
// Upstream: main.go startup.
// Downstream: unlicensedLoop goroutine.
func (c *Client) SetUnlicensedReporter(rep UnlicensedReporter) {
	c.unlicensedReporter = rep
	if rep != nil && c.unlicensedQueue == nil {
		c.unlicensedQueue = make(chan unlicensedEvent, 256)
		// Goroutine: drain unlicensed queue and invoke reporter callbacks.
		go c.unlicensedLoop()
	}
}

// Purpose: Drain the unlicensed event queue and invoke the reporter safely.
// Key aspects: Recovers from reporter panics to keep the loop alive.
// Upstream: SetUnlicensedReporter goroutine.
// Downstream: UnlicensedReporter callback.
func (c *Client) unlicensedLoop() {
	for {
		select {
		case evt := <-c.unlicensedQueue:
			if evt.call == "" {
				continue
			}
			if rep := c.unlicensedReporter; rep != nil {
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("pskreporter: unlicensed reporter panic: %v", r)
						}
					}()
					rep(evt.source, evt.role, evt.call, evt.mode, evt.freq)
				}()
			}
		case <-c.shutdown:
			return
		}
	}
}

// Purpose: Dispatch an unlicensed event without blocking ingest.
// Key aspects: Enqueues when possible; otherwise calls reporter asynchronously.
// Upstream: convertToSpot US license check.
// Downstream: unlicensedQueue or UnlicensedReporter.
func (c *Client) dispatchUnlicensed(role, call, mode string, freq float64) {
	rep := c.unlicensedReporter
	if rep == nil {
		return
	}
	if c.unlicensedQueue != nil {
		select {
		case c.unlicensedQueue <- unlicensedEvent{source: "PSKREPORTER", role: role, call: call, mode: mode, freq: freq}:
			return
		default:
			// fall through to async direct call
		}
	}
	// Goroutine: best-effort direct reporter call when queue is full.
	go rep("PSKREPORTER", role, call, mode, freq)
}

// Purpose: Construct a PSKReporter MQTT client.
// Key aspects: Initializes channels, caches, and worker settings.
// Upstream: main.go startup.
// Downstream: Client.Connect, worker pool.
func NewClient(broker string, port int, topics []string, name string, workers int, lookup func() *cty.CTYDatabase, skewStore *skew.Store, appendSSID bool, spotBuffer int) *Client {
	if spotBuffer <= 0 {
		spotBuffer = defaultSpotBuffer
	}
	return &Client{
		broker:       broker,
		port:         port,
		topics:       append([]string{}, topics...),
		name:         name,
		spotChan:     make(chan *spot.Spot, spotBuffer), // Buffered ingest to absorb bursts
		shutdown:     make(chan struct{}),
		workers:      workers,
		lookup:       lookup,
		skewStore:    skewStore,
		appendSSID:   appendSSID,
		spotterCache: spot.NewCallCache(callCacheSize, callCacheTTL),
		infoCache:    make(map[string]infoCacheEntry),
		infoCacheTTL: infoCacheTTL,
	}
}

// Purpose: Connect to the PSKReporter MQTT broker and start workers.
// Key aspects: Configures reconnect/keepalive and sets handlers.
// Upstream: main.go startup or reconnect flows.
// Downstream: startWorkerPool, mqtt.Client.Connect.
func (c *Client) Connect() error {
	c.startWorkerPool()
	// Create MQTT client options
	opts := mqtt.NewClientOptions()
	brokerURL := fmt.Sprintf("tcp://%s:%d", c.broker, c.port)
	opts.AddBroker(brokerURL)

	// Set client ID with timestamp for uniqueness
	clientID := fmt.Sprintf("gocluster-%d", time.Now().Unix())
	opts.SetClientID(clientID)

	// Set keep alive and timeouts
	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetCleanSession(true)

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
	log.Printf("PSKReporter: Connection lost: %v", err)
	log.Println("PSKReporter: Will attempt to reconnect...")
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
	payload := make([]byte, len(msg.Payload()))
	copy(payload, msg.Payload())
	select {
	case <-c.shutdown:
		return
	case c.processing <- payload:
		// payload enqueued for workers
	default:
		log.Printf("PSKReporter: Processing queue full, dropping payload")
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
	if c.processing != nil {
		return
	}
	capacity := c.workers * pskReporterQueuePerWorker
	if capacity < 256 {
		capacity = 256
	}
	c.processing = make(chan []byte, capacity)
	c.workerWg.Add(c.workers)
	for i := 0; i < c.workers; i++ {
		// Goroutine: worker loop parses payloads into spots.
		go c.workerLoop(i)
	}
}

// Purpose: Process payloads from the processing queue.
// Key aspects: Stops on shutdown; recovers from panics to avoid worker loss.
// Upstream: startWorkerPool goroutine.
// Downstream: handlePayload.
func (c *Client) workerLoop(id int) {
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
		case payload := <-c.processing:
			c.handlePayload(payload)
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
	if err := json.Unmarshal(payload, &pskrMsg); err != nil {
		log.Printf("PSKReporter: Failed to parse message: %v", err)
		return
	}
	s := c.convertToSpot(&pskrMsg)
	if s == nil {
		return
	}
	select {
	case c.spotChan <- s:
	default:
		log.Println("PSKReporter: Spot channel full, dropping spot")
	}
}

// Purpose: Convert a PSKReporter message into a canonical Spot.
// Key aspects: Preserves observation timestamp; applies skew correction and CTY metadata.
// Upstream: handlePayload.
// Downstream: normalizeMessage, skew.ApplyCorrection, fetchCallsignInfo.
// convertToSpot converts PSKReporter message to our Spot format
// IMPORTANT: This function sets the spot's Time field to the actual observation time
// from the PSKReporter message, NOT the current system time. This is critical for
// deduplication to work correctly, as the Hash32() function includes the timestamp
// truncated to the minute. If we used the current time instead of the observation time,
// identical spots arriving a few seconds apart could cross a minute boundary and
// generate different hashes, preventing proper deduplication.
func (c *Client) convertToSpot(msg *PSKRMessage) *spot.Spot {
	// Validate required fields
	if msg.SenderCall == "" || msg.ReceiverCall == "" || msg.Frequency == 0 {
		return nil
	}

	norm := c.normalizeMessage(msg)
	if norm == nil {
		return nil
	}

	dxCall := norm.dxCall
	deCall := norm.deCall
	if !spot.IsValidCallsign(dxCall) {
		// log.Printf("PSKReporter: invalid DX call %s", msg.SenderCall) // noisy: caller requested silence
		return nil
	}
	if !spot.IsValidCallsign(deCall) {
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

	dxInfo, ok := c.fetchCallsignInfo(dxCall)
	if !ok {
		return nil
	}
	deInfo, ok := c.fetchCallsignInfo(deCall)
	if !ok {
		return nil
	}
	// Prune US spotters lacking an active FCC license early to keep ingest lightweight.
	if deInfo != nil && deInfo.ADIF == 291 && !uls.IsLicensedUS(deCall) {
		c.dispatchUnlicensed("DE", deCall, norm.modeUpper, freqKHz)
		return nil
	}
	// Create spot
	// In PSKReporter: sender = DX station, receiver = spotter
	// In our model: DXCall = sender, DECall = receiver
	s := spot.NewSpot(dxCall, deCall, freqKHz, norm.modeUpper)
	s.IsHuman = false

	// CRITICAL: Set the actual observation timestamp from PSKReporter
	// This overwrites the time.Now() that was set in NewSpot()
	// The PSKReporter "t" field contains the Unix timestamp (seconds since epoch)
	// when the signal was actually observed, which is what we need for accurate
	// deduplication across multiple spotters reporting the same signal
	s.Time = spotTime

	// Set report (SNR in dB)
	s.Report = msg.Report
	s.HasReport = true

	// Build comment with locators
	s.Comment = fmt.Sprintf("%s>%s", norm.dxGrid, norm.deGrid)

	// Set source type and node
	s.SourceType = spot.SourcePSKReporter
	s.SourceNode = "PSKREPORTER"

	s.DXMetadata = metadataFromPrefix(dxInfo)
	s.DEMetadata = metadataFromPrefix(deInfo)
	s.DXMetadata.Grid = norm.dxGrid
	s.DEMetadata.Grid = norm.deGrid

	s.RefreshBeaconFlag()

	return s
}
// Purpose: Convert CTY prefix metadata into Spot CallMetadata.
// Key aspects: Returns zero-value metadata on nil input.
// Upstream: convertToSpot.
// Downstream: None.
func metadataFromPrefix(info *cty.PrefixInfo) spot.CallMetadata {
	if info == nil {
		return spot.CallMetadata{}
	}
	return spot.CallMetadata{
		Continent: info.Continent,
		Country:   info.Country,
		CQZone:    info.CQZone,
		ITUZone:   info.ITUZone,
		ADIF:      info.ADIF,
	}
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
// Key aspects: Uppercases fields, validates frequency, and normalizes callsigns.
// Upstream: convertToSpot.
// Downstream: spot.NormalizeCallsign, decorateSpotterCall.
func (c *Client) normalizeMessage(msg *PSKRMessage) *normalizedPSK {
	if msg == nil {
		return nil
	}
	modeUpper := strings.ToUpper(strings.TrimSpace(msg.Mode))
	if modeUpper == "" || msg.SenderCall == "" || msg.ReceiverCall == "" {
		return nil
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
		modeUpper: modeUpper,
		dxCall:    dxCall,
		deCall:    deCall,
		dxGrid:    dxGrid,
		deGrid:    deGrid,
		freqKHz:   freqKHz,
	}
}

// Purpose: Normalize spotter callsign and optionally append an SSID suffix.
// Key aspects: Uses a cache; appends "-#" only when enabled and safe.
// Upstream: normalizeMessage.
// Downstream: spot.NormalizeCallsign, spot.CallCache.
func (c *Client) decorateSpotterCall(raw string) string {
	cacheKey := raw
	if c.appendSSID {
		cacheKey = "1|" + raw
	} else {
		cacheKey = "0|" + raw
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
	if len(normalized)+2 > 10 {
		c.spotterCache.Add(cacheKey, normalized)
		return normalized
	}
	normalized = normalized + "-#"
	c.spotterCache.Add(cacheKey, normalized)
	return normalized
}

// Purpose: Look up CTY metadata with a small local cache.
// Key aspects: Uses TTL-based cache and tolerates missing CTY DB.
// Upstream: convertToSpot.
// Downstream: getInfoFromCache, setInfoCache, CTYDatabase.LookupCallsign.
func (c *Client) fetchCallsignInfo(call string) (*cty.PrefixInfo, bool) {
	if c.lookup == nil {
		return nil, true
	}
	now := time.Now()
	if call == "" {
		return nil, false
	}
	if info, ok := c.getInfoFromCache(call, now); ok {
		return info, info != nil
	}

	db := c.lookup()
	if db == nil {
		return nil, true
	}
	info, ok := db.LookupCallsign(call)
	c.setInfoCache(call, info, ok, now)
	if !ok {
		// log.Printf("PSKReporter: unknown call %s", call) // suppressed per user request
	}
	return info, ok
}

type infoCacheEntry struct {
	info *cty.PrefixInfo
	ok   bool
	at   time.Time
}

// Purpose: Fetch CTY metadata from the local info cache.
// Key aspects: Enforces TTL expiration; guarded by a mutex.
// Upstream: fetchCallsignInfo.
// Downstream: None.
func (c *Client) getInfoFromCache(call string, now time.Time) (*cty.PrefixInfo, bool) {
	c.infoCacheMu.Lock()
	defer c.infoCacheMu.Unlock()
	entry, ok := c.infoCache[call]
	if !ok {
		return nil, false
	}
	if c.infoCacheTTL > 0 && now.Sub(entry.at) > c.infoCacheTTL {
		delete(c.infoCache, call)
		return nil, false
	}
	return entry.info, entry.ok
}

// Purpose: Store CTY metadata in the local info cache.
// Key aspects: Records timestamp for TTL eviction.
// Upstream: fetchCallsignInfo.
// Downstream: None.
func (c *Client) setInfoCache(call string, info *cty.PrefixInfo, ok bool, now time.Time) {
	c.infoCacheMu.Lock()
	c.infoCache[call] = infoCacheEntry{info: info, ok: ok, at: now}
	c.infoCacheMu.Unlock()
}

// Purpose: Wait for workers to exit and release the processing queue.
// Key aspects: Blocks until workerWg completes.
// Upstream: Stop.
// Downstream: workerWg.Wait.
func (c *Client) stopWorkerPool() {
	c.workerWg.Wait()
	c.processing = nil
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

// Purpose: Expose the output spot channel.
// Key aspects: Read-only channel for downstream consumers.
// Upstream: main.go pipeline wiring.
// Downstream: None.
func (c *Client) GetSpotChannel() <-chan *spot.Spot {
	return c.spotChan
}

// Purpose: Report whether the MQTT client is connected.
// Key aspects: Safe when client is nil.
// Upstream: Diagnostics or health checks.
// Downstream: mqtt.Client.IsConnected.
func (c *Client) IsConnected() bool {
	return c.client != nil && c.client.IsConnected()
}

// Purpose: Stop the PSKReporter client and worker pool.
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
