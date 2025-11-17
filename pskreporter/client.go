package pskreporter

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/spot"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Client represents a PSKReporter MQTT client
type Client struct {
	broker     string
	port       int
	topic      string
	name       string
	client     mqtt.Client
	spotChan   chan *spot.Spot
	shutdown   chan struct{}
	workers    int
	processing chan []byte
	workerWg   sync.WaitGroup
	queueDrops uint64
}

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
)

// NewClient creates a new PSKReporter MQTT client
func NewClient(broker string, port int, topic string, name string, workers int) *Client {
	return &Client{
		broker:   broker,
		port:     port,
		topic:    topic,
		name:     name,
		spotChan: make(chan *spot.Spot, 1000), // Buffer 1000 spots
		shutdown: make(chan struct{}),
		workers:  workers,
	}
}

// Connect establishes connection to PSKReporter MQTT broker
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

	// Set auto-reconnect
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(1 * time.Minute)

	// Set connection handlers
	opts.SetOnConnectHandler(c.onConnect)
	opts.SetConnectionLostHandler(c.onConnectionLost)

	// Create MQTT client
	c.client = mqtt.NewClient(opts)

	if c.name != "" {
		log.Printf("Connecting to %s MQTT broker at %s...", c.name, brokerURL)
	} else {
		log.Printf("Connecting to PSKReporter MQTT broker at %s...", brokerURL)
	}

	// Connect to broker
	token := c.client.Connect()
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to connect to PSKReporter: %w", token.Error())
	}

	log.Println("Connected to PSKReporter MQTT broker")

	return nil
}

// onConnect is called when connection is established
func (c *Client) onConnect(client mqtt.Client) {
	log.Printf("PSKReporter: Connected, subscribing to topic: %s", c.topic)

	// Subscribe to topic
	token := client.Subscribe(c.topic, 0, c.messageHandler)
	if token.Wait() && token.Error() != nil {
		log.Printf("PSKReporter: Failed to subscribe: %v", token.Error())
		return
	}

	log.Println("PSKReporter: Successfully subscribed, receiving spots...")
}

// onConnectionLost is called when connection is lost
func (c *Client) onConnectionLost(client mqtt.Client, err error) {
	log.Printf("PSKReporter: Connection lost: %v", err)
	log.Println("PSKReporter: Will attempt to reconnect...")
}

// messageHandler processes incoming MQTT messages
func (c *Client) messageHandler(client mqtt.Client, msg mqtt.Message) {
	payload := make([]byte, len(msg.Payload()))
	copy(payload, msg.Payload())
	select {
	case <-c.shutdown:
		return
	case c.processing <- payload:
		// payload enqueued for workers
	default:
		drops := atomic.AddUint64(&c.queueDrops, 1)
		log.Printf("PSKReporter: Processing queue full, dropping payload (drops=%d)", drops)
	}
}

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
		go c.workerLoop(i)
	}
}

func (c *Client) workerLoop(id int) {
	defer c.workerWg.Done()
	for {
		select {
		case <-c.shutdown:
			return
		case payload := <-c.processing:
			c.handlePayload(payload)
		}
	}
}

func (c *Client) handlePayload(payload []byte) {
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

	// Convert frequency from Hz to kHz
	freqKHz := float64(msg.Frequency) / 1000.0

	// Create spot
	// In PSKReporter: sender = DX station, receiver = spotter
	// In our model: DXCall = sender, DECall = receiver
	s := spot.NewSpot(msg.SenderCall, msg.ReceiverCall, freqKHz, msg.Mode)

	// CRITICAL: Set the actual observation timestamp from PSKReporter
	// This overwrites the time.Now() that was set in NewSpot()
	// The PSKReporter "t" field contains the Unix timestamp (seconds since epoch)
	// when the signal was actually observed, which is what we need for accurate
	// deduplication across multiple spotters reporting the same signal
	s.Time = spotTime

	// Set report (SNR in dB)
	s.Report = msg.Report

	// Build comment with locators
	s.Comment = fmt.Sprintf("%s>%s",
		msg.SenderLocator,
		msg.ReceiverLocator)

	// Set source type and node
	s.SourceType = spot.SourcePSKReporter
	s.SourceNode = "PSKREPORTER"

	return s
}

func (c *Client) stopWorkerPool() {
	c.workerWg.Wait()
	c.processing = nil
}

func (c *Client) WorkerStats() (workers int, queueLen int, queueDrops uint64) {
	workers = c.workers
	if workers == 0 {
		workers = defaultPSKReporterWorkers()
	}
	if c.processing != nil {
		queueLen = len(c.processing)
	} else {
		queueLen = 0
	}
	queueDrops = atomic.LoadUint64(&c.queueDrops)
	return
}

func defaultPSKReporterWorkers() int {
	workers := runtime.NumCPU() / 2
	if workers < 1 {
		workers = 1
	}
	return workers
}

// GetSpotChannel returns the channel for receiving spots
func (c *Client) GetSpotChannel() <-chan *spot.Spot {
	return c.spotChan
}

// IsConnected returns whether the client is connected
func (c *Client) IsConnected() bool {
	return c.client != nil && c.client.IsConnected()
}

// Stop closes the PSKReporter connection
func (c *Client) Stop() {
	log.Println("Stopping PSKReporter client...")

	if c.client != nil && c.client.IsConnected() {
		// Unsubscribe
		c.client.Unsubscribe(c.topic)

		// Disconnect (wait up to 250ms for clean disconnect)
		c.client.Disconnect(250)
	}

	close(c.shutdown)

	c.stopWorkerPool()
	log.Println("PSKReporter client stopped")
}
