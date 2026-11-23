package telnet

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/commands"
	"dxcluster/config"
	"dxcluster/pathreliability"
	"dxcluster/spot"

	"github.com/cockroachdb/pebble"
)

type jsonHistoryCapture struct {
	bytes.Buffer
	visible bytes.Buffer
	onFlush func(string) error
}

func (o *jsonHistoryCapture) Flush() error {
	batch := o.String()
	if o.onFlush != nil {
		if err := o.onFlush(batch); err != nil {
			return err
		}
	}
	o.visible.WriteString(batch)
	o.Reset()
	return nil
}

func startJSONHistory(s *Server, c *Client, cmd string) <-chan bool {
	done := make(chan bool, 1)
	go func() {
		handled, proceed := s.handleHistoryCommand(c, cmd)
		done <- handled && proceed
	}()
	return done
}

func takeJSONHistory(t *testing.T, c *Client) *jsonHistoryDelivery {
	t.Helper()
	select {
	case msg := <-c.controlChan:
		if msg.jsonPage == nil {
			t.Fatalf("expected structured page, received %q", msg.line)
		}
		return msg.jsonPage
	case <-time.After(5 * time.Second):
		t.Fatal("history was never published")
		return nil
	}
}

func awaitJSONHistory(t *testing.T, done <-chan bool, want bool) {
	t.Helper()
	select {
	case actual := <-done:
		if actual != want {
			t.Fatalf("reader proceeded=%t, want %t", actual, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not finish history command")
	}
}

func collectJSONTestRows(req archive.HistoryRequest, count int) (archive.HistoryPage, error) {
	for newest := count; newest > 0; newest-- {
		row := spot.NewSpot("K1ABC", "W1XYZ", 14030, "CW")
		row.Comment = strconv.Itoa(newest)
		row.Time = time.Unix(100, 0)
		if !req.Match(row) {
			return archive.HistoryPage{}, fmt.Errorf("test history filter unexpectedly rejected row %d", newest)
		}
		if err := req.Collect(row); err != nil {
			return archive.HistoryPage{}, err
		}
	}
	return archive.HistoryPage{Selected: count, End: archive.HistoryExhausted}, nil
}

func TestJSONHistoryCountsOrderAndLiveOpportunity(t *testing.T) {
	for _, size := range []int{7, 250} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			base := processJSONHistoryBytes.Load()
			s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
				return collectJSONTestRows(req, size)
			})
			c.jsonRequested.Store(true)
			c.spotChan = make(chan *spotEnvelope, 1)
			t.Cleanup(c.interrupt)
			command := "SHOW DX 50"
			if size == 250 {
				command = "SHOW DX 250"
			}
			done := startJSONHistory(s, c, command)
			page := takeJSONHistory(t, c)
			var wire jsonHistoryCapture
			injected := false
			wire.onFlush = func(batch string) error {
				if !injected && strings.Contains(batch, `["h",`) {
					injected = true
					c.spotChan <- &spotEnvelope{spot: spot.NewSpot("W2XYZ", "W1XYZ", 14033, "CW")}
				}
				return nil
			}
			e := new(jsonSpotEncoder)
			err := page.write(c, &wire, e, func(env *spotEnvelope) error { return e.writeLive(&wire, c, env) })
			if err != nil {
				t.Fatal(err)
			}
			awaitJSONHistory(t, done, true)
			lines := strings.Split(strings.TrimSpace(wire.visible.String()), "\n")
			if len(lines) != size+3 || lines[0] != fmt.Sprintf(`["hb",{"count":%d,"older":false}]`, size) ||
				lines[len(lines)-1] != `["he","exhausted",null,{"unreadable":false,"hadMatches":true}]` {
				t.Fatalf("rows=%d records=%d header=%s trailer=%s", size, len(lines), lines[0], lines[len(lines)-1])
			}
			history := 0
			live := 0
			for _, line := range lines[1 : len(lines)-1] {
				var record []any
				if err := json.Unmarshal([]byte(line), &record); err != nil || len(record) != 23 {
					t.Fatalf("invalid record %q: %v", line, err)
				}
				switch record[0] {
				case "s":
					live++
					if history < 1 || history >= size {
						t.Fatalf("live spot appeared after %d history rows", history)
					}
				case "h":
					history++
					if record[9] != strconv.Itoa(history) {
						t.Fatalf("out-of-order history %d: %q", history, record[9])
					}
				default:
					t.Fatalf("unexpected record %q", line)
				}
			}
			if live != 1 || history != size || c.readPausePending.Load() || c.readPauseUntilUnixNano.Load() != 0 ||
				processJSONHistoryBytes.Load() != base {
				t.Fatalf("history=%d live=%d charged-delta=%d", history, live, processJSONHistoryBytes.Load()-base)
			}
		})
	}
}

func TestJSONHistoryContinuationFlushAndBusyRetry(t *testing.T) {
	scans := 0
	pos := []byte("previous archive position")
	s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
		scans++
		switch scans {
		case 1:
			// Legitimate zero-row scan-budget continuation.
			return archive.HistoryPage{End: archive.HistoryBudgetReached, Before: pos, Unreadable: 1}, nil
		case 2:
			if !bytes.Equal(req.Before, pos) {
				return archive.HistoryPage{}, errors.New("early NEXT used wrong cursor")
			}
			if _, err := collectJSONTestRows(req, 1); err != nil {
				return archive.HistoryPage{}, err
			}
			// Occupy the rest of the process budget after the first real
			// reservation. The second collector call must hit admission.
			otherPages := jsonHistoryBudgetBytes - processJSONHistoryBytes.Load()
			processJSONHistoryBytes.Add(otherPages)
			defer processJSONHistoryBytes.Add(-otherPages)
			return collectJSONTestRows(req, 1)
		case 3:
			if !bytes.Equal(req.Before, pos) {
				return archive.HistoryPage{}, errors.New("busy moved cursor")
			}
			return collectJSONTestRows(req, 1)
		default:
			return archive.HistoryPage{}, errors.New("unexpected extra scan")
		}
	})
	c.jsonRequested.Store(true)
	t.Cleanup(c.interrupt)
	first := startJSONHistory(s, c, "SHOW DX 50")
	p := takeJSONHistory(t, c)
	if c.historySearch != nil {
		t.Fatal("published cursor before delivering he")
	}
	var next <-chan bool
	var firstWire jsonHistoryCapture
	firstWire.onFlush = func(batch string) error {
		if strings.HasPrefix(batch, `["he",`) {
			if c.historySearch != nil {
				t.Fatal("cursor available before he flush succeeds")
			}
			next = startJSONHistory(s, c, "SHOW DX NEXT")
		}
		return nil
	}
	if err := p.write(c, &firstWire, new(jsonSpotEncoder), nil); err != nil {
		t.Fatal(err)
	}
	awaitJSONHistory(t, first, true)
	busy := takeJSONHistory(t, c)
	saved := c.historySearch
	if saved == nil || !strings.Contains(firstWire.visible.String(), `["he","budget","`+saved.token+`",{"unreadable":true,"hadMatches":false}]`) {
		t.Fatalf("missing zero-row budget continuation: %q", firstWire.visible.String())
	}
	var busyWire jsonHistoryCapture
	if err := busy.write(c, &busyWire, new(jsonSpotEncoder), nil); err != nil {
		t.Fatal(err)
	}
	awaitJSONHistory(t, next, true)
	if busyWire.visible.String() != "[\"hb\",{\"count\":0,\"older\":true}]\n[\"he\",\"busy\",null,{\"unreadable\":true,\"hadMatches\":false}]\n" ||
		c.historySearch != saved {
		t.Fatalf("busy response %q, saved position changed=%t", busyWire.visible.String(), c.historySearch != saved)
	}
	retry := startJSONHistory(s, c, "SHOW DX NEXT "+saved.token)
	last := takeJSONHistory(t, c)
	var lastWire jsonHistoryCapture
	if err := last.write(c, &lastWire, new(jsonSpotEncoder), nil); err != nil {
		t.Fatal(err)
	}
	awaitJSONHistory(t, retry, true)
	if c.historySearch != nil || !strings.Contains(lastWire.visible.String(), `["he","exhausted",null,{"unreadable":true,"hadMatches":true}]`) {
		t.Fatal(lastWire.visible.String())
	}
}

func TestJSONHistoryFlushFailureAndDiscard(t *testing.T) {
	for _, drain := range []bool{false, true} {
		t.Run(strconv.FormatBool(drain), func(t *testing.T) {
			base := processJSONHistoryBytes.Load()
			s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
				return collectJSONTestRows(req, 7)
			})
			c.jsonRequested.Store(true)
			t.Cleanup(c.interrupt)
			done := startJSONHistory(s, c, "SHOW DX 7")
			p := takeJSONHistory(t, c)
			if drain {
				c.controlChan <- controlMessage{jsonPage: p}
				c.interrupt()
				c.drainBulletinReservations()
			} else {
				var wire jsonHistoryCapture
				wire.onFlush = func(batch string) error {
					if strings.Contains(batch, `["he",`) {
						return io.ErrClosedPipe
					}
					return nil
				}
				if err := p.write(c, &wire, new(jsonSpotEncoder), nil); !errors.Is(err, io.ErrClosedPipe) ||
					strings.Contains(wire.visible.String(), `["he",`) {
					t.Fatalf("failed trailer: err=%v visible=%s", err, wire.visible.String())
				}
			}
			awaitJSONHistory(t, done, false)
			if c.historySearch != nil || processJSONHistoryBytes.Load() != base {
				t.Fatal("released incomplete page advanced cursor or retained its charge")
			}
		})
	}
}

func TestJSONHistoryPartialFailureRetryAndFreshSupersession(t *testing.T) {
	position := []byte("right before the fault")
	var positions [][]byte
	s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
		positions = append(positions, bytes.Clone(req.Before))
		if len(positions) == 4 {
			return archive.HistoryPage{}, errors.New("fresh scan canceled")
		}
		page, err := collectJSONTestRows(req, 3)
		page.End, page.Before = archive.HistoryCountReached, position
		return page, err
	})
	t.Cleanup(c.interrupt)
	c.jsonRequested.Store(true)
	run := func(cmd string, injectInvalid bool) string {
		done := startJSONHistory(s, c, cmd)
		p := takeJSONHistory(t, c)
		if injectInvalid {
			// A semantic fault at row 2 is distinguishable from a mid-record
			// socket error: the former can complete he/failed and safely retry.
			p.rows.first.next.frequency = 0
		}
		var wire jsonHistoryCapture
		if err := p.write(c, &wire, new(jsonSpotEncoder), nil); err != nil {
			t.Fatal(err)
		}
		awaitJSONHistory(t, done, true)
		return wire.visible.String()
	}
	first := run("SHOW DX 3", false)
	saved := c.historySearch
	if saved == nil || !strings.Contains(first, `["he","count","`+saved.token) {
		t.Fatal(first)
	}
	cmd := "SHOW DX NEXT " + saved.token
	failed := run(cmd, true)
	if c.historySearch != saved || strings.Count(failed, `["h",`) != 1 || !strings.Contains(failed, `["he","failed",null,`) {
		t.Fatal(failed)
	}
	retry := run(cmd, false)
	if c.historySearch == nil || c.historySearch == saved || strings.Count(retry, `["h",`) != 3 ||
		!bytes.Equal(positions[1], position) || !bytes.Equal(positions[2], position) {
		t.Fatal(retry)
	}
	fresh := run("SHOW DX 5", false)
	if c.historySearch != nil || len(positions[3]) != 0 || !strings.Contains(fresh, `["he","failed",null,`) {
		t.Fatal(fresh)
	}
}

func TestJSONHistoryGridChangeWhileQueuedOrStreaming(t *testing.T) {
	for _, begun := range []bool{false, true} {
		t.Run(strconv.FormatBool(begun), func(t *testing.T) {
			near, _, coarse, _ := requireDistinctPathCells(t)
			var liveDecision bool
			s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
				for range 2 {
					row := spot.NewSpot("K1ABC", "W1XYZ", 14030, "CW")
					row.DXCellID, row.DECellID = uint16(near), uint16(near)
					if req.Match(row) != liveDecision || !liveDecision {
						return archive.HistoryPage{}, errors.New("NEARBY differs between history and live")
					}
					if err := req.Collect(row); err != nil {
						return archive.HistoryPage{}, err
					}
				}
				return archive.HistoryPage{Selected: 2, End: archive.HistoryCountReached, Before: []byte("next")}, nil
			})
			t.Cleanup(c.interrupt)
			c.filter = passRuleTestFilter(t, "NEARBY")
			c.grid, c.gridCell, c.gridCoarseCell = "FN31", near, coarse
			c.configuredSettings.Grid = "FN31"
			liveSpot := spot.NewSpot("K1ABC", "W1XYZ", 14030, "CW")
			liveSpot.DXCellID, liveSpot.DECellID = uint16(near), uint16(near)
			liveDecision, _, _ = s.matchSnapshot(c.captureMatchingSnapshot(), liveSpot)
			c.jsonRequested.Store(true)
			done := startJSONHistory(s, c, "SHOW DX 2")
			p := takeJSONHistory(t, c)
			change := func() {
				_, handled := s.runPreferenceCommand(c, "test", func(c *Client, _ string) (string, bool) {
					c.pathMu.Lock()
					c.grid, c.gridCell, c.gridCoarseCell = "IO91", 0, 0
					c.configuredSettings.Grid = "IO91"
					c.pathMu.Unlock()
					return "", true
				})
				if !handled {
					t.Fatal("GRID preference mutation failed")
				}
			}
			var wire jsonHistoryCapture
			if begun {
				wire.onFlush = func(batch string) error {
					if strings.HasPrefix(batch, `["h",`) {
						change()
					}
					return nil
				}
			} else {
				change()
			}
			if err := p.write(c, &wire, new(jsonSpotEncoder), nil); err != nil {
				t.Fatal(err)
			}
			awaitJSONHistory(t, done, true)
			wantCount, wantEnd, wantRows := "0", "restart", 0
			if begun {
				wantCount, wantEnd, wantRows = "2", "failed", 1
			}
			reply := wire.visible.String()
			if !strings.HasPrefix(reply, `["hb",{"count":`+wantCount+`,`) ||
				strings.Count(reply, `["h",`) != wantRows || !strings.Contains(reply, `["he","`+wantEnd+`",null,`) ||
				c.historySearch != nil {
				t.Fatalf("stale history: %s", reply)
			}
		})
	}
}

func openJSONHistoryWriter(t *testing.T, s *Server, c *Client) func() string {
	t.Helper()
	s.jsonLimits = newJSONSpotLimits(1024, 0, spot.CurrentTaxonomy())
	server, remote := net.Pipe()
	c.conn, c.writer, c.writerStopped = server, bufio.NewWriter(server), make(chan struct{})
	c.spotChan = make(chan *spotEnvelope, 4)
	go c.writerLoop()
	t.Cleanup(func() {
		remote.Close()
		c.interrupt()
		writerV15Wait(t, c.writerStopped, "native history writer cleanup")
	})
	if err := remote.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(remote)
	readLine := func() string {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		return line
	}
	switched := make(chan bool, 1)
	go func() {
		_, ok := s.handleJSONCommand(c, "SET JSON ON")
		switched <- ok
	}()
	if got := readLine(); got != "JSON ON 1\r\n" {
		t.Fatal(got)
	}
	for range 3 {
		if line := readLine(); !json.Valid([]byte(line)) {
			t.Fatalf("invalid hello/schema: %q", line)
		}
	}
	awaitJSONHistory(t, switched, true)
	return readLine
}

func TestJSONHistoryRetiredSessionReceivesTerminal(t *testing.T) {
	const count = 250
	base := processJSONHistoryBytes.Load()
	s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
		p, err := collectJSONTestRows(req, count)
		p.End, p.Before = archive.HistoryCountReached, []byte("older")
		return p, err
	})
	c.retired = make(chan struct{})
	readerWaiting := make(chan struct{})
	s.testCommandPacingRetired = func(*Client) { close(readerWaiting) }
	readLine := openJSONHistoryWriter(t, s, c)
	readerDone := make(chan bool, 1)
	go func() {
		// The connection's command reader closes its socket on return.
		_, proceed := s.handleHistoryCommand(c, "SHOW DX 250")
		c.interrupt()
		readerDone <- proceed
	}()
	if line := readLine(); !strings.HasPrefix(line, `["hb",{"count":250,`) {
		t.Fatal(line)
	}
	if line := readLine(); !strings.HasPrefix(line, `["h",`) {
		t.Fatal(line)
	}
	// Registration retires the old connection before queueing SendAndClose.
	close(c.retired)
	writerV15Wait(t, readerWaiting, "retired reader waiting for terminal output")
	const terminal = "Replaced by another login"
	if err := c.SendAndClose(terminal + "\n"); err != nil {
		t.Fatal(err)
	}
	countRead := 1
	for {
		line := readLine()
		if strings.HasPrefix(line, `["h",`) {
			countRead++
			continue
		}
		if !strings.HasPrefix(line, `["he","failed",null,`) || countRead >= count {
			t.Fatalf("ended after %d rows with %s", countRead, line)
		}
		break
	}
	if line := readLine(); line != "[\"text\",\""+terminal+"\"]\n" {
		t.Fatal(line)
	}
	awaitJSONHistory(t, readerDone, false)
	if c.historySearch != nil || processJSONHistoryBytes.Load() != base {
		t.Fatal("retired history kept a cursor or reservation")
	}
}

func TestJSONHistoryNativeVersionsThroughWriter(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "history")
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixNano()
	for v := 2; v <= 7; v++ {
		raw, err := os.ReadFile(filepath.Join("..", "archive", "testdata", fmt.Sprintf("history-v%d.bin", v)))
		if err != nil {
			t.Fatal(err)
		}
		var key [14]byte
		copy(key[:], "s|")
		binary.BigEndian.PutUint64(key[2:], uint64(now))
		binary.BigEndian.PutUint32(key[10:], uint32(v))
		if err := db.Set(key[:], raw, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	ar, err := archive.NewWriter(config.ArchiveConfig{DBPath: dir, Synchronous: "off", QueueSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ar.Stop)
	s, c := historyTestSession(nil)
	s.processor = commands.NewProcessor(nil, ar, nil, nil, nil, nil)
	c.initializeConfiguredSettings()
	c.configuredSettings.CWSpeed = true
	readLine := openJSONHistoryWriter(t, s, c)
	done := startJSONHistory(s, c, "SHOW DX 50")
	if line := readLine(); line != "[\"hb\",{\"count\":6,\"older\":false}]\n" {
		t.Fatal(line)
	}
	for v := 2; v <= 7; v++ {
		var row []any
		if err := json.Unmarshal([]byte(readLine()), &row); err != nil || len(row) != 23 || row[0] != "h" || row[1] != nil {
			t.Fatalf("v%d: bad record %v err=%v", v, row, err)
		}
		wantComment, event, speeds := `POTA-1234 "trail"`, "POTA", 0
		if v >= 4 {
			event = "SOTA"
		}
		if v == 7 {
			wantComment, speeds = `CQ "trail"`, 2
		}
		dx := row[18].([]any)
		if row[9] != wantComment || len(row[10].([]any)) != speeds || row[11].([]any)[0] != event ||
			(row[4] == nil) != (v == 2) || (dx[6] == "CT") != (v >= 6) {
			t.Fatalf("v%d history row %v", v, row)
		}
	}
	if line := readLine(); line != "[\"he\",\"exhausted\",null,{\"unreadable\":false,\"hadMatches\":true}]\n" {
		t.Fatal(line)
	}
	awaitJSONHistory(t, done, true)
}

func TestJSONHistoryManyPagesAcrossReconnects(t *testing.T) {
	const clients, rounds = 16, 3
	type active struct {
		client *Client
		read   func() string
		done   <-chan bool
	}
	base := processJSONHistoryBytes.Load()
	for cycle := range rounds {
		var sessions []active
		var perPage int64
		for i := range clients {
			collected := make(chan struct{})
			s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
				p, err := collectJSONTestRows(req, 2)
				close(collected)
				return p, err
			})
			read := openJSONHistoryWriter(t, s, c)
			done := startJSONHistory(s, c, "SHOW DX 2")
			writerV15Wait(t, collected, "small page collection")
			// Each pipe blocks its writer on hb until this peer reads. All prior
			// pages must continue to own their exact reservation.
			used := processJSONHistoryBytes.Load() - base
			if i == 0 {
				perPage = used
			}
			if perPage <= 0 || used != int64(i+1)*perPage {
				t.Fatalf("cycle %d clients %d charged %d, per page %d", cycle, i+1, used, perPage)
			}
			sessions = append(sessions, active{c, read, done})
		}
		t.Logf("cycle %d held %d simultaneous two-row pages with %d charged bytes", cycle+1, clients, clients*perPage)
		for i, session := range sessions {
			if (i+cycle)%2 == 0 {
				for _, prefix := range []string{`["hb",`, `["h",`, `["h",`, `["he","exhausted",`} {
					if line := session.read(); !strings.HasPrefix(line, prefix) {
						t.Fatal(line)
					}
				}
				awaitJSONHistory(t, session.done, true)
			}
			session.client.interrupt()
			writerV15Wait(t, session.client.writerStopped, "history disconnect")
			if (i+cycle)%2 != 0 {
				awaitJSONHistory(t, session.done, false)
			}
			if got := processJSONHistoryBytes.Load() - base; got != int64(clients-i-1)*perPage {
				t.Fatalf("cycle %d after closing client %d: charge %d", cycle, i, got)
			}
		}
	}
}

func TestJSONHistoryNearbyParityAndOwnSpot(t *testing.T) {
	fine, _, coarse, _ := requireDistinctPathCells(t)
	coarseOnly := ""
	for i := range 100 {
		grid := fmt.Sprintf("FN%02d", i)
		if pathreliability.EncodeCoarseCell(grid) == coarse && pathreliability.EncodeCell(grid) != fine {
			coarseOnly = grid
			break
		}
	}
	if coarseOnly == "" {
		t.Fatal("no distinct fine-cell locator within FN31's coarse cell")
	}
	type exemplar struct {
		name           string
		frequency      float64
		dxGrid, deGrid string
		near, own      bool
	}
	cases := []exemplar{
		{"fine-dx", 14030, "FN31", "IO91", true, false},
		{"fine-de", 14030, "IO91", "FN31", true, false},
		{"fine-both", 14030, "FN31", "FN31", true, false},
		{"outside", 14030, "IO91", "IO91", false, false},
		{"no-grid", 14030, "", "", false, false},
		{"missing-de", 14030, "FN31", "", true, false},
		{"missing-dx", 14030, "", "FN31", true, false},
		{"160-dx", 1830, coarseOnly, "IO91", true, false},
		{"160-de", 1830, "IO91", coarseOnly, true, false},
		{"80-dx", 3511, coarseOnly, "IO91", true, false},
		{"80-de", 3511, "IO91", coarseOnly, true, false},
		{"60-dx", 5358, coarseOnly, "", true, false},
		{"60-de", 5358, "", coarseOnly, true, false},
		{"low-both", 3511, coarseOnly, coarseOnly, true, false},
		{"low-outside", 1830, "IO91", "IO91", false, false},
		{"low-no-grid", 5358, "", "", false, false},
		{"coarse-dx-on-20m", 14030, coarseOnly, "", false, false},
		{"coarse-only-on-20m", 14030, "", coarseOnly, false, false},
		{"own", 14030, "IO91", "IO91", false, true},
	}
	cfg := config.ArchiveConfig{DBPath: filepath.Join(t.TempDir(), "archive"), Synchronous: "off", QueueSize: 64}
	ar, err := archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ar.Start()
	now := time.Now().UTC()
	var source []*spot.Spot
	for _, tc := range cases {
		dx := "K1ABC"
		if tc.own {
			dx = "N2WQ"
		}
		sp := spot.NewSpot(dx, "W1XYZ", tc.frequency, "CW")
		sp.DXMetadata.Grid, sp.DEMetadata.Grid = tc.dxGrid, tc.deGrid
		sp.Comment, sp.Time = tc.name, now // same timestamp forces sequence tie order
		sp.EnsureNormalized()
		source = append(source, sp)
		ar.Enqueue(sp)
	}
	ar.Stop()
	ar, err = archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ar.Stop)
	for _, userGrid := range []string{"FN31", ""} {
		t.Run("grid="+userGrid, func(t *testing.T) {
			s, c := historyTestSession(nil)
			t.Cleanup(c.interrupt)
			c.filter = passRuleTestFilter(t, "NEARBY")
			c.grid = userGrid
			c.initializeConfiguredSettings()
			s.processor = commands.NewProcessor(nil, ar, nil, nil, nil, nil)
			c.spotChan = make(chan *spotEnvelope, len(source))
			var expected []string
			for i, tc := range cases {
				want := tc.own || userGrid != "" && tc.near
				s.deliverJob(broadcastJob{spot: source[i], clients: []*Client{c}, allowFast: true, allowMed: true, allowSlow: true})
				select {
				case env := <-c.spotChan:
					if !want || env.spot != source[i] {
						t.Fatalf("unexpected live spot %s", tc.name)
					}
				default:
					if want {
						t.Fatalf("missing live spot %s", tc.name)
					}
				}
				if want {
					expected = append(expected, tc.name)
				}
			}
			text := runHistory(t, s, c, "SHOW DX 50")
			var textNames []string
			for _, line := range strings.Split(text, "\n") {
				for _, tc := range cases {
					if strings.Contains(line, " "+tc.name+" ") {
						textNames = append(textNames, tc.name)
					}
				}
			}
			c.jsonRequested.Store(true)
			done := startJSONHistory(s, c, "SHOW DX 50")
			p := takeJSONHistory(t, c)
			var wire jsonHistoryCapture
			if err := p.write(c, &wire, new(jsonSpotEncoder), nil); err != nil {
				t.Fatal(err)
			}
			awaitJSONHistory(t, done, true)
			var jsonNames []string
			for _, line := range strings.Split(wire.visible.String(), "\n") {
				if strings.HasPrefix(line, `["h",`) {
					var row []any
					if err := json.Unmarshal([]byte(line), &row); err != nil {
						t.Fatal(err)
					}
					jsonNames = append(jsonNames, row[9].(string))
				}
			}
			if !slices.Equal(textNames, expected) || !slices.Equal(jsonNames, expected) {
				t.Fatalf("text=%q JSON=%q expected=%q\n%s", textNames, jsonNames, expected, text)
			}
		})
	}
}
