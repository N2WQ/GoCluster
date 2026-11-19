package pskreporter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/url"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"golang.org/x/net/proxy"
)

const (
	mqttConnectTimeout   = 10 * time.Second
	mqttSubscribeTimeout = 5 * time.Second
	mqttWriteTimeout     = 2 * time.Second
	mqttUnsubscribeWait  = 250 * time.Millisecond
	mqttRetryInitial     = 2 * time.Second
	mqttRetryMaximum     = time.Minute
	mqttStableSession    = 30 * time.Second
	mqttRetryLogInterval = 10 * time.Second
)

// The lifecycle lock protects Start/Stop and the active health handoff.
// One supervisor owns sessions. The publish path needs no lifecycle lock.
type mqttLifecycle struct {
	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	stopped bool
	active  mqtt.Client
	ready   bool
	retired mqtt.InboundPublishStats
}

type mqttDial func(context.Context, string, string) (net.Conn, error)

// Start starts processing and returns without waiting for the broker. The
// client owns both output channels until cancellation and worker retirement.
// reportFailure, when present, receives rate-limited diagnostics and must
// return promptly.
func (c *Client) Start(ctx context.Context, reportFailure func(error)) {
	c.start(ctx, proxy.Dial, reportFailure)
}

func (c *Client) start(ctx context.Context, dial mqttDial, reportFailure func(error)) {
	c.lifecycle.mu.Lock()
	defer c.lifecycle.mu.Unlock()
	if c.lifecycle.done != nil || c.lifecycle.stopped {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	c.lifecycle.cancel, c.lifecycle.done = cancel, done
	enablePahoDebugLogging()
	c.startWorkerPool()
	go func() {
		defer close(done)
		defer cancel()
		c.runMQTT(ctx, dial, reportFailure)
		c.closeOutput()
	}()
}

// Stop cancels in-flight MQTT work and joins the producer. Repeated calls can
// share the same completion fence.
func (c *Client) Stop() {
	c.lifecycle.mu.Lock()
	done := c.lifecycle.done
	if done == nil {
		if !c.lifecycle.stopped {
			c.lifecycle.stopped = true
			c.closeOutput()
		}
	} else {
		c.lifecycle.cancel()
		c.lifecycle.ready = false
	}
	c.lifecycle.mu.Unlock()
	if done != nil {
		<-done
	}
}

func (c *Client) closeOutput() {
	close(c.shutdown)
	c.stopWorkerPool()
	close(c.spotChan)
	if c.pathOnlyChan != nil {
		close(c.pathOnlyChan)
	}
}

func (c *Client) Endpoint() string {
	if c == nil {
		return ""
	}
	return fmt.Sprintf("%s:%d", c.broker, c.port)
}

// IsConnected reports whether the open session has all required subscriptions.
func (c *Client) IsConnected() bool {
	ready, _ := c.lifecycle.health()
	return ready
}

func (c *Client) mqttOptions() mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://" + c.Endpoint())
	opts.SetClientID(fmt.Sprintf("gocluster-%d", time.Now().UTC().Unix()))
	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetConnectTimeout(mqttConnectTimeout)
	opts.SetWriteTimeout(mqttWriteTimeout)
	opts.SetCleanSession(true)
	opts.SetOrderMatters(false)
	opts.SetAutoReconnect(false)
	opts.SetConnectRetry(false)

	workers := c.mqttInboundWorkers
	if workers <= 0 {
		workers = c.workers
	}
	depth := c.mqttInboundQueueDepth
	if depth <= 0 {
		depth = min(workers*mqttInboundQueuePerWorker, mqttInboundQueueDepthCap)
	}
	opts.SetInboundPublishWorkers(workers)
	opts.SetInboundPublishQueueDepth(uint(depth))
	opts.SetInboundPublishEnqueueTimeout(max(c.mqttQoS12EnqueueTimeout, 0))
	log.Printf("PSKReporter: MQTT broker=%s inbound queue depth=%d workers=%d qos12_timeout=%s topics=%v",
		c.Endpoint(), depth, workers, max(c.mqttQoS12EnqueueTimeout, 0), c.topics)
	return *opts
}

func (c *Client) runMQTT(ctx context.Context, dial mqttDial, reportFailure func(error)) {
	opts := c.mqttOptions()
	delay := mqttRetryInitial
	var lastFailureLog time.Time
	for ctx.Err() == nil {
		readyDuration, err := c.runMQTTSession(ctx, opts, dial)
		if ctx.Err() != nil {
			return
		}
		if readyDuration >= mqttStableSession {
			delay = mqttRetryInitial
		}
		wait := delay/2 + rand.N(delay/2) // #nosec G404 -- Retry jitter has no security purpose.
		if time.Since(lastFailureLog) >= mqttRetryLogInterval {
			log.Printf("PSKReporter: MQTT feed unavailable at %s: %v (retry in %s)", c.Endpoint(), err, wait.Round(time.Millisecond))
			lastFailureLog = time.Now()
			if reportFailure != nil {
				reportFailure(err)
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = min(delay*2, mqttRetryMaximum)
	}
}

func (c *Client) runMQTTSession(ctx context.Context, opts mqtt.ClientOptions, dial mqttDial) (time.Duration, error) {
	// The supervisor closes the transport directly. Keep it alive briefly on
	// intentional shutdown to retain the established session's unsubscribe.
	transportCtx, closeTransport := context.WithCancel(context.WithoutCancel(ctx))
	// Paho can fall back from MQTT 3.1.1 to 3.1 after the first
	// handshake deadline, so budget for both protocol attempts.
	connectCtx, stopConnect := context.WithTimeout(ctx, 2*opts.ConnectTimeout)
	defer stopConnect()

	lost := make(chan struct{})
	var lossErr error
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		lossErr = err
		close(lost)
	})
	opts.SetCustomOpenConnectionFn(func(broker *url.URL, _ mqtt.ClientOptions) (net.Conn, error) {
		dialCtx, stopDial := context.WithTimeout(connectCtx, opts.ConnectTimeout)
		defer stopDial()
		conn, err := dial(dialCtx, "tcp", broker.Host)
		if err != nil {
			return nil, err
		}
		// Paho performs CONNACK I/O after the dial. Closing the transport
		// interrupts that I/O and later retires an established session.
		stop := context.AfterFunc(transportCtx, func() { _ = conn.Close() })
		return &mqttTransport{Conn: conn, ctx: transportCtx, stopClose: stop}, nil
	})
	client := mqtt.NewClient(&opts)
	c.lifecycle.mu.Lock()
	c.lifecycle.active = client
	c.lifecycle.mu.Unlock()
	connect := client.Connect()
	var readyAt time.Time
	defer func() {
		c.lifecycle.mu.Lock()
		c.lifecycle.ready = false
		c.lifecycle.mu.Unlock()
		if !readyAt.IsZero() && ctx.Err() != nil && client.IsConnectionOpen() {
			client.Unsubscribe(c.topics...).WaitTimeout(mqttUnsubscribeWait)
		}
		closeTransport()
		<-connect.Done()
		if connect.Error() == nil {
			// Paho invokes the loss handler after its comms workers retire.
			// This also fences callbacks before folding session counters.
			<-lost
		}
		c.lifecycle.retire(client)
	}()

	if err := awaitMQTTToken(connectCtx, connect); err != nil {
		return 0, fmt.Errorf("connect: %w", err)
	}
	stopConnect()

	subscribeCtx, stopSubscribe := context.WithTimeout(ctx, mqttSubscribeTimeout)
	err := c.subscribeMQTT(subscribeCtx, client)
	stopSubscribe()
	if err != nil {
		return 0, fmt.Errorf("subscribe: %w", err)
	}
	c.lifecycle.mu.Lock()
	if ctx.Err() == nil && client.IsConnectionOpen() {
		c.lifecycle.ready = true
		readyAt = time.Now()
	}
	c.lifecycle.mu.Unlock()
	if readyAt.IsZero() {
		return 0, mqtt.ErrNotConnected
	}
	log.Printf("PSKReporter: MQTT feed ready at %s (%d subscribed topic(s))", c.Endpoint(), len(c.topics))
	select {
	case <-ctx.Done():
		return time.Since(readyAt), ctx.Err()
	case <-lost:
		return time.Since(readyAt), fmt.Errorf("connection lost: %w", lossErr)
	}
}

type mqttTransport struct {
	net.Conn
	ctx       context.Context
	stopClose func() bool
}

func (conn *mqttTransport) Read(p []byte) (int, error) {
	n, err := conn.Conn.Read(p)
	// Paho suppresses a locally closed socket error when it assumes its own
	// Disconnect already owns cleanup. This close belongs to our supervisor,
	// so report EOF to drive Paho's normal loss and worker-retirement path.
	if errors.Is(err, net.ErrClosed) && conn.ctx.Err() != nil {
		return n, io.EOF
	}
	return n, err
}

func (conn *mqttTransport) Close() error {
	conn.stopClose()
	return conn.Conn.Close()
}

func awaitMQTTToken(ctx context.Context, token mqtt.Token) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-token.Done():
		return token.Error()
	}
}

func (c *Client) subscribeMQTT(ctx context.Context, client mqtt.Client) error {
	if len(c.topics) == 0 {
		return errors.New("no MQTT topics configured")
	}
	for _, topic := range c.topics {
		if err := ctx.Err(); err != nil {
			return err
		}
		token := client.Subscribe(topic, 0, c.messageHandler)
		if err := awaitMQTTToken(ctx, token); err != nil {
			return fmt.Errorf("topic %q: %w", topic, err)
		}
		// Each token corresponds to one filter. Paho may normalize the
		// result key for shared subscriptions, so check the sole grant.
		ack, ok := token.(*mqtt.SubscribeToken)
		if !ok {
			return fmt.Errorf("topic %q: unexpected MQTT subscription token %T", topic, token)
		}
		result := ack.Result()
		if len(result) != 1 {
			return fmt.Errorf("topic %q: missing SUBACK result", topic)
		}
		for _, grant := range result {
			if grant != 0 {
				return fmt.Errorf("topic %q: broker returned SUBACK 0x%02x for QoS 0", topic, grant)
			}
		}
	}
	return nil
}

func mqttPublishStats(client mqtt.Client) mqtt.InboundPublishStats {
	if provider, ok := client.(mqtt.InboundPublishStatsProvider); ok {
		return provider.InboundPublishStats()
	}
	return mqtt.InboundPublishStats{}
}

func (state *mqttLifecycle) retire(client mqtt.Client) {
	state.mu.Lock()
	defer state.mu.Unlock()
	stats := mqttPublishStats(client)
	state.retired.DroppedQoS0 += stats.DroppedQoS0
	state.retired.EnqueueTimeoutQoS12 += stats.EnqueueTimeoutQoS12
	state.retired.DisconnectsQoS12 += stats.DisconnectsQoS12
	state.active = nil
}

func (state *mqttLifecycle) health() (bool, mqtt.InboundPublishStats) {
	state.mu.Lock()
	defer state.mu.Unlock()
	stats := mqttPublishStats(state.active)
	stats.DroppedQoS0 += state.retired.DroppedQoS0
	stats.EnqueueTimeoutQoS12 += state.retired.EnqueueTimeoutQoS12
	stats.DisconnectsQoS12 += state.retired.DisconnectsQoS12
	return state.ready && state.active.IsConnectionOpen(), stats
}
