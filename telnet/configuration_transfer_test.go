package telnet

import (
	"bytes"
	"errors"
	"sync"
	"testing"
)

func TestConfigurationInputBudgetSpansServersAndSurvivesCancellation(t *testing.T) {
	initialInput, initialOutput := configurationTransferBudgetStats()
	servers := [2]*Server{{}, {}}
	var releases []func()
	var clients []*Client
	t.Cleanup(func() {
		for _, release := range releases {
			release()
		}
		assertConfigurationTransferStats(t, initialInput, initialOutput)
	})
	for i := range 32 {
		c := &Client{server: servers[i%2], done: make(chan struct{})}
		release, err := acquireConfigurationInput(c)
		if err != nil {
			t.Fatalf("input reservation %d: %v", i, err)
		}
		clients, releases = append(clients, c), append(releases, release)
	}
	assertConfigurationTransferStats(t, initialInput+8<<20, initialOutput)
	if release, err := acquireConfigurationInput(&Client{}); !errors.Is(err, errConfigurationTransferBusy) || release != nil {
		t.Fatalf("33rd input owner admitted: release=%v err=%v", release != nil, err)
	}
	clients[0].interrupt()
	// Cancellation cannot release a buffer/AST still owned by the command.
	assertConfigurationTransferStats(t, initialInput+8<<20, initialOutput)
	releases[0]()
	releases[0]()
	assertConfigurationTransferStats(t, initialInput+(8<<20)-(256<<10), initialOutput)
	if release, err := acquireConfigurationInput(clients[0]); !errors.Is(err, errClientClosed) || release != nil {
		t.Fatalf("closed input owner admitted: release=%v err=%v", release != nil, err)
	}
	release, err := acquireConfigurationInput(&Client{})
	if err != nil {
		t.Fatalf("released input slot was not reusable: %v", err)
	}
	releases = append(releases, release)
	assertConfigurationTransferStats(t, initialInput+8<<20, initialOutput)
}

func TestConfigurationOutputBudgetAndOnePendingPerClient(t *testing.T) {
	initialInput, initialOutput := configurationTransferBudgetStats()
	var reservations []*configurationOutputReservation
	t.Cleanup(func() {
		for _, reservation := range reservations {
			reservation.Release()
		}
		assertConfigurationTransferStats(t, initialInput, initialOutput)
	})
	servers := [2]*Server{{}, {}}
	for i := range 8 {
		c := &Client{server: servers[i%2], done: make(chan struct{}), controlChan: make(chan controlMessage, 1)}
		reservation, err := c.reserveConfigurationOutput()
		if err != nil {
			t.Fatalf("output reservation %d: %v", i, err)
		}
		reservations = append(reservations, reservation)
		if second, err := c.reserveConfigurationOutput(); !errors.Is(err, errConfigurationTransferBusy) || second != nil {
			t.Fatalf("second output owner for one client admitted: reservation=%v err=%v", second != nil, err)
		}
	}
	assertConfigurationTransferStats(t, initialInput, initialOutput+8<<20)
	if ninth, err := (&Client{}).reserveConfigurationOutput(); !errors.Is(err, errConfigurationTransferBusy) || ninth != nil {
		t.Fatalf("ninth output owner admitted: reservation=%v err=%v", ninth != nil, err)
	}
	reservations[0].Release()
	reservations[0].Release()
	replacement, err := reservations[0].client.reserveConfigurationOutput()
	if err != nil {
		t.Fatalf("released client's slot was not reusable: %v", err)
	}
	reservations = append(reservations, replacement)
	assertConfigurationTransferStats(t, initialInput, initialOutput+8<<20)
}

func TestConfigurationOutputQueueTransfersOwnerUntilDiscard(t *testing.T) {
	initialInput, initialOutput := configurationTransferBudgetStats()
	c := &Client{done: make(chan struct{}), controlChan: make(chan controlMessage, 1)}
	reservation, err := c.reserveConfigurationOutput()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reservation.Release(); assertConfigurationTransferStats(t, initialInput, initialOutput) })
	raw := []byte("---\r\nschema_version: 7\r\n...\r\n")
	if err := c.enqueueConfigurationResponse(reservation, raw, false); err != nil {
		t.Fatal(err)
	}
	assertConfigurationTransferStats(t, initialInput, initialOutput+1<<20)
	msg := <-c.controlChan
	if msg.configurationReservation != reservation || !bytes.Equal(msg.raw, raw) || msg.closeAfter {
		t.Fatal("queue lost the response, owner, or terminal flag")
	}
	if second, err := c.reserveConfigurationOutput(); !errors.Is(err, errConfigurationTransferBusy) || second != nil {
		t.Fatal("dequeue released output before writer flush/discard")
	}
	msg.raw = nil
	msg.configurationReservation.Release()
	assertConfigurationTransferStats(t, initialInput, initialOutput)
}

func TestConfigurationOutputFrameLimit(t *testing.T) {
	for _, size := range []int{262_144, 262_145} {
		initialInput, initialOutput := configurationTransferBudgetStats()
		c := &Client{done: make(chan struct{}), controlChan: make(chan controlMessage, 1)}
		reservation, err := c.reserveConfigurationOutput()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(reservation.Release)
		err = c.enqueueConfigurationResponse(reservation, bytes.Repeat([]byte{'x'}, size), false)
		if size == 262_144 {
			if err != nil || len(c.controlChan) != 1 {
				t.Fatalf("exact frame limit rejected: size=%d err=%v", size, err)
			}
			msg := <-c.controlChan
			msg.raw = nil
			msg.configurationReservation.Release()
		} else if !errors.Is(err, errReadbackTooLarge) || len(c.controlChan) != 0 {
			t.Fatalf("oversized frame queued: size=%d err=%v", size, err)
		}
		select {
		case <-c.done:
			t.Fatal("read-only response-size refusal closed the client")
		default:
		}
		assertConfigurationTransferStats(t, initialInput, initialOutput)
	}
}

func TestConfigurationOutputCloseAndQueueFullRelease(t *testing.T) {
	for _, closeFirst := range []bool{false, true} {
		initialInput, initialOutput := configurationTransferBudgetStats()
		c := &Client{done: make(chan struct{}), controlChan: make(chan controlMessage, 1)}
		reservation, err := c.reserveConfigurationOutput()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(reservation.Release)
		want := errControlQueueFull
		if closeFirst {
			c.interrupt()
			want = errClientClosed
		} else {
			c.controlChan <- controlMessage{line: "occupied\n"}
		}
		if err := c.enqueueConfigurationResponse(reservation, []byte("---\r\n...\r\n"), true); !errors.Is(err, want) {
			t.Fatalf("closeFirst=%v err=%v want=%v", closeFirst, err, want)
		}
		select {
		case <-c.done:
		default:
			t.Fatal("closed/full client retained an open session")
		}
		if !c.preserveRecordOnExit.Load() {
			t.Fatal("terminal transfer failure lost protected saved state")
		}
		assertConfigurationTransferStats(t, initialInput, initialOutput)
	}
}

func TestConfigurationOutputConcurrentCloseEnqueueAndRelease(t *testing.T) {
	initialInput, initialOutput := configurationTransferBudgetStats()
	for range 200 {
		c := &Client{done: make(chan struct{}), controlChan: make(chan controlMessage, 1)}
		reservation, err := c.reserveConfigurationOutput()
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var workers sync.WaitGroup
		workers.Add(2)
		go func() {
			defer workers.Done()
			<-start
			_ = c.enqueueConfigurationResponse(reservation, []byte("---\r\n...\r\n"), false)
		}()
		go func() { defer workers.Done(); <-start; c.interrupt() }()
		close(start)
		workers.Wait()
		select {
		case msg := <-c.controlChan:
			msg.raw = nil
			msg.configurationReservation.Release()
		default:
		}
		// A closed client's post-drain enqueue cannot acquire another owner.
		if err := c.enqueueConfigurationResponse(reservation, nil, false); !errors.Is(err, errClientClosed) {
			t.Fatalf("post-close enqueue admitted: %v", err)
		}
		reservation.Release()
		assertConfigurationTransferStats(t, initialInput, initialOutput)
	}
}

func assertConfigurationTransferStats(t *testing.T, wantInput, wantOutput int64) {
	t.Helper()
	input, output := configurationTransferBudgetStats()
	if input != wantInput || output != wantOutput {
		t.Fatalf("transfer bytes input=%d output=%d; want input=%d output=%d", input, output, wantInput, wantOutput)
	}
}
