// Package pskreporter implements a client for the PSKReporter MQTT service.
//
// PSKReporter provides real-time digital mode spots (FT8, FT4, WSPR, etc.) via MQTT.
// This client connects to the PSKReporter MQTT broker, subscribes to filtered topics,
// and converts JSON messages into the canonical Spot format.
//
// MQTT Topic Structure:
//   pskr/filter/v2/{band}/{mode}/# - Filtered by band and mode
//   Example: pskr/filter/v2/20m/FT8/# for all 20m FT8 spots
//
// Message Format:
//   JSON with fields: frequency (Hz), mode, sender/receiver callsigns,
//   locators, SNR report, timestamp, ADIF country codes, band
//
// Features:
//   - MQTT auto-reconnect with 1-minute max interval
//   - JSON message parsing with validation
//   - Frequency conversion (Hz → kHz)
//   - Comment formatting with SNR and grid locators
//   - Buffered spot channel (1000 spots)
//   - Connection state callbacks (onConnect, onConnectionLost)
//
// The client feeds spots into the deduplicator's input channel in the unified architecture.
package pskreporter

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"dxcluster/spot"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Client represents a PSKReporter MQTT client.
//
// The client maintains a persistent MQTT connection to the PSKReporter broker,
// subscribes to configured topics, and continuously processes JSON spot messages.
//
// Fields:
//   - broker, port: MQTT broker address (typically mqtt.pskreporter.info:1883)
//   - topic: MQTT topic filter (e.g., "pskr/filter/v2/+/FT8/#" for all FT8)
//   - client: Paho MQTT client instance
//   - spotChan: Buffered channel for outputting parsed spots (capacity 1000)
//   - shutdown: Channel for coordinating graceful shutdown
//
// Thread Safety:
//   - MQTT client library handles message callbacks in separate goroutines
//   - spotChan is buffered and uses non-blocking sends
//   - Auto-reconnect is handled automatically by MQTT library
type Client struct {
	broker   string          // MQTT broker hostname
	port     int             // MQTT broker port (typically 1883)
	topic    string          // MQTT topic filter subscription
	client   mqtt.Client     // Paho MQTT client instance
	spotChan chan *spot.Spot // Output channel for parsed spots (buffered 1000)
	shutdown chan struct{}   // Shutdown coordination channel
}

// PSKRMessage represents a PSKReporter MQTT message in JSON format.
//
// PSKReporter sends compact JSON messages with abbreviated field names
// to reduce bandwidth. This struct maps the JSON fields to Go types.
//
// Data Flow:
//   Sender (DX station) → Receiver (spotter)
//   In our model: Sender = DXCall, Receiver = DECall
//
// All fields use abbreviated JSON tags for compact serialization.
type PSKRMessage struct {
	SequenceNumber  uint64 `json:"sq"` // Sequence number for message ordering
	Frequency       int64  `json:"f"`  // Frequency in Hz (will be converted to kHz)
	Mode            string `json:"md"` // Operating mode (FT8, FT4, WSPR, etc.)
	Report          int    `json:"rp"` // SNR report in dB (positive or negative)
	Timestamp       int64  `json:"t"`  // Unix timestamp (seconds since epoch)
	SenderCall      string `json:"sc"` // Sender (DX station) callsign
	SenderLocator   string `json:"sl"` // Sender Maidenhead grid locator
	ReceiverCall    string `json:"rc"` // Receiver (spotter) callsign
	ReceiverLocator string `json:"rl"` // Receiver Maidenhead grid locator
	SenderCountry   int    `json:"sa"` // Sender ADIF country code
	ReceiverCountry int    `json:"ra"` // Receiver ADIF country code
	Band            string `json:"b"`  // Amateur radio band (e.g., "20m", "15m")
}

// NewClient creates a new PSKReporter MQTT client
func NewClient(broker string, port int, topic string) *Client {
	return &Client{
		broker:   broker,
		port:     port,
		topic:    topic,
		spotChan: make(chan *spot.Spot, 1000), // Buffer 1000 spots
		shutdown: make(chan struct{}),
	}
}

// Connect establishes connection to PSKReporter MQTT broker
func (c *Client) Connect() error {
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

	log.Printf("Connecting to PSKReporter MQTT broker at %s...", brokerURL)

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
	// Parse JSON payload
	var pskrMsg PSKRMessage
	if err := json.Unmarshal(msg.Payload(), &pskrMsg); err != nil {
		log.Printf("PSKReporter: Failed to parse message: %v", err)
		return
	}

	// Convert to our Spot format
	s := c.convertToSpot(&pskrMsg)
	if s == nil {
		return // Invalid spot
	}

	// Send to spot channel (non-blocking)
	select {
	case c.spotChan <- s:
		log.Printf("Parsed PSKReporter spot: %s spotted by %s on %.1f kHz (%s, %+d dB, %s)",
			s.DXCall, s.DECall, s.Frequency, s.Mode, s.Report, pskrMsg.Band)
	default:
		log.Println("PSKReporter: Spot channel full, dropping spot")
	}
}

// convertToSpot converts PSKReporter message to our Spot format
func (c *Client) convertToSpot(msg *PSKRMessage) *spot.Spot {
	// Validate required fields
	if msg.SenderCall == "" || msg.ReceiverCall == "" || msg.Frequency == 0 {
		return nil
	}

	// Convert frequency from Hz to kHz
	freqKHz := float64(msg.Frequency) / 1000.0

	// Create spot
	// In PSKReporter: sender = DX station, receiver = spotter
	// In our model: DXCall = sender, DECall = receiver
	s := spot.NewSpot(msg.SenderCall, msg.ReceiverCall, freqKHz, msg.Mode)

	// Set timestamp from message
	s.Time = time.Unix(msg.Timestamp, 0)

	// Set report (SNR in dB)
	s.Report = msg.Report

	// Build comment with locators
	s.Comment = fmt.Sprintf("%s>%s",
		msg.SenderLocator,
		msg.ReceiverLocator)

	// Set source type
	s.SourceType = spot.SourcePSKReporter

	return s
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
	log.Println("PSKReporter client stopped")
}
