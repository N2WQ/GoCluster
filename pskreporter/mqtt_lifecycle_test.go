package pskreporter

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const mqttTestTopic = "pskr/test"

func mqttTestClient() *Client {
	return NewClient("localhost", 1883, []string{mqttTestTopic}, "", 1, 1, 1, 0, nil, nil, false, 64, 4096)
}

func mqttTestPipe(ctx context.Context, peers chan<- net.Conn) (net.Conn, error) {
	client, broker := net.Pipe()
	select {
	case peers <- broker:
		return client, nil
	case <-ctx.Done():
		_ = client.Close()
		_ = broker.Close()
		return nil, ctx.Err()
	}
}

func mqttTestAccept(t *testing.T, peers <-chan net.Conn) net.Conn {
	t.Helper()
	broker, _ := mqttTestConnectPacket(t, peers)
	return broker
}

func mqttTestConnectPacket(t *testing.T, peers <-chan net.Conn) (net.Conn, *packets.ConnectPacket) {
	t.Helper()
	select {
	case broker := <-peers:
		t.Cleanup(func() { _ = broker.Close() })
		if err := broker.SetDeadline(time.Now().Add(12 * time.Second)); err != nil {
			t.Fatal(err)
		}
		connect, ok := mqttTestRead(t, broker).(*packets.ConnectPacket)
		if !ok {
			t.Fatal("session did not send CONNECT")
		}
		return broker, connect
	case <-time.After(10 * time.Second):
		t.Fatal("no MQTT attempt")
		return nil, nil
	}
}

func mqttTestRead(t *testing.T, broker net.Conn) packets.ControlPacket {
	t.Helper()
	packet, err := packets.ReadPacket(broker)
	if err != nil {
		t.Fatal(err)
	}
	return packet
}

func mqttTestSubscribe(t *testing.T, broker net.Conn) uint16 {
	t.Helper()
	if err := packets.NewControlPacket(packets.Connack).Write(broker); err != nil {
		t.Fatal(err)
	}
	sub, ok := mqttTestRead(t, broker).(*packets.SubscribePacket)
	if !ok || len(sub.Topics) != 1 || sub.Topics[0] != mqttTestTopic || len(sub.Qoss) != 1 || sub.Qoss[0] != 0 {
		t.Fatalf("wrong subscription: %v", sub)
	}
	return sub.MessageID
}

func mqttTestAck(broker net.Conn, messageID uint16, code byte) error {
	ack := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	ack.MessageID, ack.ReturnCodes = messageID, []byte{code}
	return ack.Write(broker)
}

func mqttTestWait(t *testing.T, condition func() bool, description string) {
	t.Helper()
	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatalf("timed out waiting for %s", description)
		}
	}
}

func mqttTestStop(t *testing.T, client *Client) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		client.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("Stop did not retire MQTT ownership")
	}
	if client.IsConnected() {
		t.Fatal("retired client reports ready")
	}
	if _, ok := <-client.GetSpotChannel(); ok {
		t.Fatal("spot channel remained open")
	}
	if path := client.GetPathOnlyChannel(); path != nil {
		if _, ok := <-path; ok {
			t.Fatal("path-only channel remained open")
		}
	}
}

func mqttTestClosed(t *testing.T, broker net.Conn) {
	t.Helper()
	var read [1]byte
	if _, err := broker.Read(read[:]); !errors.Is(err, io.EOF) {
		t.Fatalf("retired session still held its transport: %v", err)
	}
}

func TestMQTTSubscriptionRecovery(t *testing.T) {
	client := mqttTestClient()
	peers := make(chan net.Conn)
	client.start(t.Context(), func(ctx context.Context, _, _ string) (net.Conn, error) {
		return mqttTestPipe(ctx, peers)
	}, nil)
	t.Cleanup(client.Stop)

	first := mqttTestAccept(t, peers)
	firstID := mqttTestSubscribe(t, first)
	if client.HealthSnapshot().Connected {
		t.Fatal("ready before SUBACK")
	}
	if err := mqttTestAck(first, firstID, 0x80); err != nil {
		t.Fatal(err)
	}
	mqttTestClosed(t, first)
	if client.HealthSnapshot().Connected {
		t.Fatal("refused topic reported ready")
	}

	second := mqttTestAccept(t, peers)
	secondID := mqttTestSubscribe(t, second)
	mqttTestClosed(t, second) // Missing SUBACK has a bounded wait.
	if err := mqttTestAck(second, secondID, 0); err == nil {
		t.Fatal("retired transport accepted a late SUBACK")
	}
	third := mqttTestAccept(t, peers)
	thirdID := mqttTestSubscribe(t, third)
	if client.HealthSnapshot().Connected {
		t.Fatal("pending subscription inherited an old acknowledgement")
	}
	if err := mqttTestAck(third, thirdID, 0); err != nil {
		t.Fatal(err)
	}
	mqttTestWait(t, client.IsConnected, "accepted subscription")
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
	mqttTestWait(t, func() bool { return !client.IsConnected() }, "readiness clearing on transport loss")
	fourth := mqttTestAccept(t, peers)
	fourthID := mqttTestSubscribe(t, fourth)
	if client.IsConnected() {
		t.Fatal("accepted SUBACK from a previous session carried over")
	}
	if err := mqttTestAck(fourth, fourthID, 0); err != nil {
		t.Fatal(err)
	}
	mqttTestWait(t, client.IsConnected, "resubscription after established loss")
}

func TestMQTTFallsBackAfterHandshakeTimeout(t *testing.T) {
	client := mqttTestClient()
	opts := client.mqttOptions()
	opts.SetConnectTimeout(500 * time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	peers := make(chan net.Conn)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = client.runMQTTSession(ctx, opts, func(ctx context.Context, _, _ string) (net.Conn, error) {
			return mqttTestPipe(ctx, peers)
		})
	}()

	first, initial := mqttTestConnectPacket(t, peers)
	if initial.ProtocolVersion != 4 {
		t.Fatalf("initial protocol = %d, want 4", initial.ProtocolVersion)
	}
	mqttTestClosed(t, first) // Withhold CONNACK for the entire first deadline.
	second, legacy := mqttTestConnectPacket(t, peers)
	if legacy.ProtocolVersion != 3 {
		t.Fatalf("fallback protocol = %d, want 3", legacy.ProtocolVersion)
	}
	id := mqttTestSubscribe(t, second)
	if err := mqttTestAck(second, id, 0); err != nil {
		t.Fatal(err)
	}
	mqttTestWait(t, client.IsConnected, "legacy session ready")
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("legacy session did not retire")
	}
}

func TestMQTTStopWhilePending(t *testing.T) {
	for _, phase := range []string{"dial", "backoff", "handshake", "subscription"} {
		t.Run(phase, func(t *testing.T) {
			client := mqttTestClient()
			peers := make(chan net.Conn)
			started := make(chan struct{}, 1)
			dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
				if phase == "dial" || phase == "backoff" {
					select {
					case started <- struct{}{}:
					default:
					}
					if phase == "dial" {
						<-ctx.Done()
						return nil, ctx.Err()
					}
					return nil, errors.New("dial refused")
				}
				return mqttTestPipe(ctx, peers)
			}
			client.start(t.Context(), dial, nil)
			t.Cleanup(client.Stop)
			if phase == "dial" || phase == "backoff" {
				select {
				case <-started:
				case <-time.After(3 * time.Second):
					t.Fatal("dial never started")
				}
				if phase == "backoff" {
					mqttTestWait(t, func() bool {
						client.lifecycle.mu.Lock()
						defer client.lifecycle.mu.Unlock()
						return client.lifecycle.active == nil
					}, "failed attempt retirement")
				}
				mqttTestStop(t, client)
				select {
				case <-started:
					t.Fatal("unexpected additional dial")
				default:
				}
				return
			}
			broker := mqttTestAccept(t, peers)
			if phase == "subscription" {
				mqttTestSubscribe(t, broker)
			}
			mqttTestStop(t, client)
			mqttTestClosed(t, broker)
		})
	}
}

func TestMQTTCountersSurviveSessionRetirement(t *testing.T) {
	client := mqttTestClient()
	peers := make(chan net.Conn)
	client.start(t.Context(), func(ctx context.Context, _, _ string) (net.Conn, error) {
		return mqttTestPipe(ctx, peers)
	}, nil)
	t.Cleanup(client.Stop)
	var previous uint64
	for range 3 {
		broker := mqttTestAccept(t, peers)
		id := mqttTestSubscribe(t, broker)
		if err := mqttTestAck(broker, id, 0); err != nil {
			t.Fatal(err)
		}
		mqttTestWait(t, client.IsConnected, "accepted subscription")
		func() {
			// Hold the application callback while the real Paho inbound
			// queue overflows. Sampling skips the application's queue lock.
			client.processingMu.Lock()
			defer client.processingMu.Unlock()
			packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			packet.TopicName, packet.Payload = mqttTestTopic, []byte("{}")
			for range 32 {
				if err := packet.Write(broker); err != nil {
					t.Fatal(err)
				}
			}
			mqttTestWait(t, func() bool {
				_, stats := client.lifecycle.health()
				return stats.DroppedQoS0 > previous
			}, "real Paho QoS 0 drops")
		}()
		before := client.HealthSnapshot().MQTTInboundDropsQoS0
		if err := broker.Close(); err != nil {
			t.Fatal(err)
		}
		mqttTestWait(t, func() bool {
			snapshot := client.HealthSnapshot()
			if snapshot.MQTTInboundDropsQoS0 < before {
				t.Fatalf("MQTT counter fell at session handoff: %d -> %d", before, snapshot.MQTTInboundDropsQoS0)
			}
			return !snapshot.Connected && snapshot.MQTTInboundQueueCap == 0
		}, "session retirement")
		previous = before
	}
}
