package peer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
)

const dxSpiderReferenceCommit = "3e9b3621d94dd45c68702e4a0f896aac33f2a91d"

type dxReferenceResult struct {
	TX            []string                  `json:"tx"`
	Channel       map[string]any            `json:"channel"`
	Routes        map[string]map[string]any `json:"routes"`
	RouteNodes    map[string]map[string]any `json:"route_nodes"`
	RouteUsers    map[string]map[string]any `json:"route_users"`
	Normalized    string                    `json:"normalized"`
	Valid         bool                      `json:"valid"`
	Users         map[string]map[string]any `json:"users"`
	UserTotal     int                       `json:"user_total"`
	NodeTotal     int                       `json:"node_total"`
	ReceivedBytes int                       `json:"received_bytes"`
	ReceivedLines int                       `json:"received_lines"`
}

type dxReference struct {
	t      *testing.T
	input  *json.Encoder
	output *json.Decoder
}

// startDXReference requires explicit environment configuration so normal unit
// tests do not silently download or install an external runtime. The dedicated
// script executes this suite with all prerequisites and a pinned clean checkout.
func startDXReference(t *testing.T, outbound bool, call string) (*dxReference, dxReferenceResult) {
	t.Helper()
	root, perl := os.Getenv("DXSPIDER_ROOT"), os.Getenv("DXSPIDER_PERL")
	if root == "" || perl == "" {
		t.Skip("external DXSpider evidence requires DXSPIDER_ROOT and DXSPIDER_PERL; run scripts/pc92-dxspider-interop.ps1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	commit, err := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil || strings.TrimSpace(string(commit)) != dxSpiderReferenceCommit {
		cancel()
		t.Fatalf("reference checkout must be %s: %s %v", dxSpiderReferenceCommit, commit, err)
	}
	if err := exec.CommandContext(ctx, "git", "-C", root, "diff", "--quiet", dxSpiderReferenceCommit, "--", "perl", "data/prefix_data.pl").Run(); err != nil {
		cancel()
		t.Fatalf("reference receiver code or prefix data differs from pinned source: %v", err)
	}
	repo, err := filepath.Abs("..")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	args := []string{}
	if lib := os.Getenv("DXSPIDER_PERL_LIB"); lib != "" {
		args = append(args, "-I", lib)
	}
	args = append(args, filepath.Join(repo, "scripts", "pc92-dxspider-interop.pl"))
	command := exec.CommandContext(ctx, perl, args...)
	command.Env = append(os.Environ(), "DXSPIDER_TEST_STATE="+t.TempDir(), "GOCLUSTER_ROOT="+repo, "LC_ALL=C")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	input, err := command.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = input.Close()
		err := command.Wait()
		cancel()
		if err != nil {
			t.Errorf("actual DXSpider receiver failed: %v\n%s", err, stderr.String())
		}
	})
	decoder := json.NewDecoder(output)
	decoder.UseNumber()
	reference := &dxReference{t: t, input: json.NewEncoder(input), output: decoder}
	return reference, reference.step(map[string]any{"command": "init", "outbound": outbound, "call": call})
}

func (r *dxReference) step(request map[string]any) dxReferenceResult {
	r.t.Helper()
	if err := r.input.Encode(request); err != nil {
		r.t.Fatal(err)
	}
	var result dxReferenceResult
	if err := r.output.Decode(&result); err != nil {
		r.t.Fatalf("actual DXSpider receiver response: %v", err)
	}
	return result
}

func (r *dxReference) frame(line string, calls ...string) dxReferenceResult {
	return r.step(map[string]any{"command": "frame", "line": line, "calls": calls, "chunk": 97})
}

func referenceField(t *testing.T, got map[string]any, field, want string) {
	t.Helper()
	if actual := fmt.Sprint(got[field]); actual != want {
		t.Fatalf("receiver %s=%s want %s (%v)", field, actual, want, got)
	}
}

func (r *dxReference) frameAt(line string, at time.Time, calls ...string) dxReferenceResult {
	return r.step(map[string]any{"command": "frame", "line": line, "at": at.Unix(), "calls": calls, "chunk": 97})
}

// This sender exercises the production controller and sole session writer.
// Scheduling is explicit to make boundary seconds deterministic; the existing
// startup test separately exercises Run and both handshake directions.
func referencePublicationSender(t *testing.T, wall *time.Time) (*protocolController, *session, func() string) {
	t.Helper()
	cfg := config.PeeringConfig{NodeVersion: "5457", NodeBuild: "633", PC92Bitmap: 5, HopCount: 99, WriteQueueSize: 128, MaxLineLength: 65536, PC92MaxBytes: 65536}
	manager, err := NewManager(completeProtocolTestConfig(cfg, "N0CALL"), "N0CALL", nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	local, remote := net.Pipe()
	endpoint := PeerEndpoint{remoteCall: "GB7REF", family: config.PeeringPeerFamilyDXSpider, preferPC9x: true}
	s := newSession(local, dirOutbound, manager, endpoint, manager.sessionSettings(endpoint))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s.ctx, s.cancel = ctx, cancel
	s.pc9x = true
	if err := manager.registerSession(s); err != nil {
		t.Fatal(err)
	}
	p := manager.protocol
	p.wallNow = func() time.Time { return *wall }
	s.startWorker(s.writerLoop)
	t.Cleanup(func() {
		s.close()
		_ = remote.Close()
		s.workers.Wait()
		s.discardQueuedOutput()
		manager.Stop()
	})
	reader := bufio.NewReader(remote)
	return p, s, func() string { return readSessionWire(t, reader, remote) }
}

func referenceWatermark(t *testing.T, result dxReferenceResult, want string) {
	t.Helper()
	got, err := strconv.ParseFloat(fmt.Sprint(result.Routes["N0CALL"]["lastid"]), 64)
	expected, wantErr := strconv.ParseFloat(want, 64)
	if err != nil || wantErr != nil || got != expected {
		t.Fatalf("actual receiver lastid=%v, want %s", result.Routes["N0CALL"], want)
	}
}

func referenceOnlyUser(t *testing.T, result dxReferenceResult, call, address string) {
	t.Helper()
	users, ok := result.Routes["N0CALL"]["users"].([]any)
	if !ok || len(users) != 1 || fmt.Sprint(users[0]) != call {
		t.Fatalf("actual receiver membership=%v want only %s", result.Routes["N0CALL"], call)
	}
	referenceField(t, result.Routes[call], "ip", address)
}

func TestDXSpiderReferenceTimestampBurstCoalesces(t *testing.T) {
	reference, _ := startDXReference(t, false, "N0CALL")
	wall := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	p, recipient, read := referencePublicationSender(t, &wall)
	entries, complete := p.membershipEntries()
	if !complete {
		t.Fatal("incomplete timestamp fixture")
	}
	p.current, p.published, p.dirty = entries, entries, false
	if err := p.sendRecord([]*session{recipient}, "C", entryValues(entries), false); err != nil {
		t.Fatal(err)
	}
	// Exercise the complete legal sequence directly. Periodic requests are now
	// scheduled/coalesced; their count is not a promise of one record each.
	for range 99 {
		if err := p.sendRecord([]*session{recipient}, "K", nil, false); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 150; i++ {
		if err := p.request(protocolRequest{kind: "K", source: recipient}); err != nil {
			t.Fatalf("periodic request %d: %v", i, err)
		}
	}
	p.tick(time.Now()) // The exhausted second must leave the coalesced K pending.
	if p.pendingK.Len() != 1 || !p.pendingK.Value(recipient) || recipient.ctx.Err() != nil {
		t.Fatal("same-second overflow did not coalesce to one pending K on a healthy session")
	}
	var result dxReferenceResult
	for i := 0; i < 100; i++ {
		wire := read()
		stamp := "43200"
		if i > 0 {
			stamp = fmt.Sprintf("43200.%02d", i)
		}
		fields := strings.Split(wire, "^")
		if len(fields) < 4 || fields[2] != stamp || (i == 0 && fields[3] != "C") || (i > 0 && fields[3] != "K") {
			t.Fatalf("wire record %d=%q, expected timestamp %s", i, wire, stamp)
		}
		result = reference.frameAt(wire, wall)
		referenceWatermark(t, result, stamp)
	}
	if len(recipient.priorityLineCh) != 0 || p.pendingK.Len() != 1 {
		t.Fatal("rate overflow fabricated another wire value or lost the pending K")
	}
	wall = wall.Add(time.Second)
	p.tick(time.Now())
	wire := read()
	if !strings.HasPrefix(wire, "PC92^N0CALL^43201^K^") {
		t.Fatalf("coalesced recovery wire=%q", wire)
	}
	result = reference.frameAt(wire, wall)
	referenceWatermark(t, result, "43201")
	if p.pendingK.Len() != 0 || result.ReceivedLines != 101 {
		t.Fatalf("pending=%d actual reference frames=%d", p.pendingK.Len(), result.ReceivedLines)
	}
	t.Logf("actual receiver accepted C + 99 allocated K values through 43200.99, then one coalesced K at 43201 after 150 pending requests; route=%v", result.Routes["N0CALL"])
}

func TestDXSpiderReferenceTimestampMidnight(t *testing.T) {
	reference, _ := startDXReference(t, false, "N0CALL")
	wall := time.Date(2026, 10, 1, 23, 59, 59, 0, time.UTC)
	p, recipient, read := referencePublicationSender(t, &wall)
	send := func(action, call, address, stamp string) (string, dxReferenceResult) {
		t.Helper()
		entry := PC92Entry{Call: call, Flags: 1, IP: netip.MustParseAddr(address)}
		if err := p.sendRecord([]*session{recipient}, action, []PC92Entry{entry}, false); err != nil {
			t.Fatal(err)
		}
		wire := read()
		if !strings.HasPrefix(wire, "PC92^N0CALL^"+stamp+"^"+action+"^") {
			t.Fatalf("boundary wire=%q", wire)
		}
		result := reference.frameAt(wire, wall, call)
		referenceWatermark(t, result, stamp)
		return wire, result
	}
	stale, _ := send("C", "K1OLD", "192.0.2.1", "86399")
	_, result := send("A", "K1OLD", "192.0.2.2", "86399.01")
	referenceOnlyUser(t, result, "K1OLD", "192.0.2.2")
	wall = wall.Add(time.Second)
	_, result = send("C", "K2NEW", "192.0.2.3", "0")
	referenceOnlyUser(t, result, "K2NEW", "192.0.2.3")
	_, result = send("A", "K2NEW", "192.0.2.4", "0.01")
	referenceOnlyUser(t, result, "K2NEW", "192.0.2.4")
	result = reference.frameAt(stale, wall, "K1OLD", "K2NEW")
	referenceWatermark(t, result, "0.01")
	referenceOnlyUser(t, result, "K2NEW", "192.0.2.4")
	t.Logf("actual receiver accepted 86399.01 -> 0 -> 0.01 and rejected pre-midnight C replay; route=%v", result.Routes["N0CALL"])
}

func TestDXSpiderReferenceSenderRestartRetainedWatermark(t *testing.T) {
	reference, _ := startDXReference(t, false, "N0CALL")
	wall := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	p, recipient, read := referencePublicationSender(t, &wall)
	old := PC92Entry{Call: "K1OLD", Flags: 1, IP: netip.MustParseAddr("192.0.2.1")}
	for _, action := range []string{"C", "A"} {
		if err := p.sendRecord([]*session{recipient}, action, []PC92Entry{old}, false); err != nil {
			t.Fatal(err)
		}
		result := reference.frameAt(read(), wall, old.Call)
		referenceOnlyUser(t, result, old.Call, old.IP.String())
	}
	// Negative control: a fresh same-second integer must not replace the
	// receiver's retained .01 watermark or its authoritative membership.
	bad, err := EncodePC92(&PC92Record{Origin: "N0CALL", Timestamp: "43200", Action: "C", Subject: p.rootEntry(), Members: []PC92Entry{{Call: "K2BAD", Flags: 1}}, Hop: 99})
	if err != nil {
		t.Fatal(err)
	}
	result := reference.frameAt(bad, wall, old.Call, "K2BAD")
	referenceWatermark(t, result, "43200.01")
	referenceOnlyUser(t, result, old.Call, old.IP.String())
	// Restart only the sender controller/generator. The actual DXSpider process,
	// channel, Route objects and lastid remain alive and are never rewritten.
	fresh := newProtocolController(p.manager)
	var clock atomic.Int64
	clock.Store(wall.UnixNano())
	clockReads := make(chan struct{}, 2)
	fresh.wallNow = func() time.Time {
		select {
		case clockReads <- struct{}{}:
		default:
		}
		return time.Unix(0, clock.Load()).UTC()
	}
	waited := make(chan error, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		waited <- fresh.waitStartupSecond(recipient.ctx)
	}()
	t.Cleanup(func() { recipient.close(); <-joined })
	for i := 0; i < 2; i++ {
		select {
		case <-clockReads:
		case <-time.After(time.Second):
			t.Fatal("sender restart did not enter its startup-second wait")
		}
	}
	select {
	case err := <-waited:
		t.Fatalf("startup second wait returned before UTC advancement: %v", err)
	default:
	}
	wall = wall.Add(time.Second)
	clock.Store(wall.UnixNano())
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("sender restart failed to resume after UTC advancement")
	}
	newUser := PC92Entry{Call: "K3NEW", Flags: 1, IP: netip.MustParseAddr("192.0.2.3")}
	for i, action := range []string{"C", "A"} {
		newUser.IP = netip.MustParseAddr(fmt.Sprintf("192.0.2.%d", i+3))
		if err := fresh.sendRecord([]*session{recipient}, action, []PC92Entry{newUser}, false); err != nil {
			t.Fatal(err)
		}
		result = reference.frameAt(read(), wall, newUser.Call)
		referenceOnlyUser(t, result, newUser.Call, newUser.IP.String())
	}
	referenceWatermark(t, result, "43201.01")
	t.Logf("actual receiver retained 43200.01 across fresh sender controller, rejected same-second 43200, and accepted restart C/A through 43201.01; route=%v", result.Routes["N0CALL"])
}

func TestDXSpiderReferencePC18IdentityAndK(t *testing.T) {
	for _, tc := range []struct {
		name       string
		releaseTag string
	}{
		{"empty release tag", ""},
		{"numbered release", "261003r2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reference, _ := startDXReference(t, true, "N0CALL")
			banner, err := BuildPC18Banner("261003", tc.releaseTag, "91abcdef0123", "2026-10-03T16:42:10Z", "go1.26.4")
			if err != nil {
				t.Fatal(err)
			}
			line, err := FormatPC18(banner, "5457", true)
			if err != nil {
				t.Fatal(err)
			}
			want := "PC18^GoCluster Version: 261003"
			if tc.releaseTag != "" {
				want += " Release tag: 261003r2"
			}
			want += ` Commit: \x391abcdef0123 Built: 2026-10-03T16:42:10Z Go: go1.26.4 pc9x^5457^`
			if line != want {
				t.Fatalf("receiver input = %q, want %q", line, want)
			}
			result := reference.frame(line)
			referenceField(t, result.Channel, "version", "54.57")
			referenceField(t, result.Channel, "sort", "A")
			referenceField(t, result.Channel, "do_pc9x", "1")
			referenceField(t, result.Channel, "do_pc91", "false")
			referenceField(t, result.Users["N0CALL"], "version", "54.57")
			stamp, err := NewTimestampGenerator().Next()
			if err != nil {
				t.Fatal(err)
			}
			wire, err := EncodePC92(&PC92Record{Origin: "N0CALL", Timestamp: stamp, Action: "K", Subject: PC92Entry{Call: "N0CALL", Flags: 5, Version: "5457", Build: "633"}, Hop: 99})
			if err != nil {
				t.Fatal(err)
			}
			result = reference.frame(wire)
			referenceField(t, result.Channel, "version", "54.57")
			referenceField(t, result.Channel, "sort", "A")
			referenceField(t, result.Channel, "do_pc9x", "1")
			referenceField(t, result.Channel, "do_pc91", "false")
			referenceField(t, result.Users["N0CALL"], "version", "54.57")
			referenceField(t, result.Users["N0CALL"], "sort", "S")
			referenceField(t, result.Routes["N0CALL"], "version", "5457")
			referenceField(t, result.Routes["N0CALL"], "build", "633")
			t.Logf("real DXSpider PC18/K: channel=%v user=%v route=%v", result.Channel, result.Users["N0CALL"], result.Routes["N0CALL"])
		})
	}
}

func TestDXSpiderReferenceComplete62171ByteSnapshot(t *testing.T) {
	const origin = "N99999ABCDEFG-1"
	reference, _ := startDXReference(t, false, origin)
	ip := netip.MustParseAddr("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")
	record := &PC92Record{Origin: origin, Timestamp: "86399.99", Action: "C", Subject: PC92Entry{Call: origin, Flags: 5, Version: "9999999999", Build: "9999999999", IP: ip}, Hop: 99}
	for i := 0; i < 1000; i++ {
		record.Members = append(record.Members, PC92Entry{Call: fmt.Sprintf("N%05dABCDEFG-1", i), Flags: 1, IP: ip})
	}
	for i := 0; i < 64; i++ {
		record.Members = append(record.Members, PC92Entry{Call: fmt.Sprintf("K%05dABCDEFG-1", i), Flags: 5, Version: "9999999999", Build: "9999999999", IP: ip})
	}
	wire, err := EncodePC92(record)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) != 62171 {
		t.Fatalf("complete snapshot bytes=%d, approved fixture expects 62171", len(wire))
	}
	at := time.Date(2026, 10, 1, 23, 59, 59, 0, time.UTC).Unix()
	result := reference.step(map[string]any{"command": "frame", "line": wire, "at": at, "chunk": 4093, "calls": []string{"N00000ABCDEFG-1", "N00999ABCDEFG-1", "K00063ABCDEFG-1"}})
	if result.UserTotal != 1000 || result.NodeTotal != 66 {
		t.Fatalf("partial DXSpider acceptance: users=%d nodes=%d", result.UserTotal, result.NodeTotal)
	}
	users := result.Routes[origin]["users"].([]any)
	nodes := result.Routes[origin]["nodes"].([]any)
	if len(users) != 1000 || len(nodes) != 64 {
		t.Fatalf("receiver membership users=%d nodes=%d", len(users), len(nodes))
	}
	if result.ReceivedBytes != 62173 || result.ReceivedLines != 1 {
		t.Fatalf("actual Msg/ExtMsg framing counters=%+v", result)
	}
	referenceField(t, result.Routes["N00999ABCDEFG-1"], "ip", ip.String())
	referenceField(t, result.Routes["K00063ABCDEFG-1"], "version", "9999999999")
	t.Log("unmodified Msg/ExtMsg/DXProt accepted 62,171-byte C + CRLF across 4,093-byte chunks: all 1,000 users and 64 peers present")
}

func TestDXSpiderReferenceCThenAMetadataRecovery(t *testing.T) {
	reference, _ := startDXReference(t, false, "N0CALL")
	generator := NewTimestampGenerator()
	send := func(action, address string) dxReferenceResult {
		t.Helper()
		stamp, err := generator.Next()
		if err != nil {
			t.Fatal(err)
		}
		wire, err := EncodePC92(&PC92Record{Origin: "N0CALL", Timestamp: stamp, Action: action, Subject: PC92Entry{Call: "N0CALL", Flags: 5, Version: "5457", Build: "633"}, Members: []PC92Entry{{Call: "K1ABC", Flags: 1, IP: netip.MustParseAddr(address)}}, Hop: 99})
		if err != nil {
			t.Fatal(err)
		}
		return reference.frame(wire, "K1ABC")
	}
	result := send("C", "192.0.2.1")
	referenceField(t, result.Routes["K1ABC"], "ip", "192.0.2.1")
	result = send("C", "192.0.2.2")
	// This observes the real reference limitation that motivates mandatory A.
	referenceField(t, result.Routes["K1ABC"], "ip", "192.0.2.1")
	result = send("A", "192.0.2.2")
	referenceField(t, result.Routes["K1ABC"], "ip", "192.0.2.2")
	t.Log("actual receiver retained existing IP after C and converged to new IP after A")
}

func TestDXSpiderReferenceGoSessionStartup(t *testing.T) {
	for _, tc := range []struct {
		name      string
		direction direction
		pc9x      bool
	}{
		{"Go outbound PC9x", dirOutbound, true},
		{"Go outbound legacy", dirOutbound, false},
		{"Go inbound PC9x", dirInbound, true},
		{"Go inbound legacy", dirInbound, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reference, first := startDXReference(t, tc.direction == dirInbound, "EA8/N0CALL/P-00")
			cfg := config.PeeringConfig{MaxPeers: 64, NodeVersion: "5457", NodeBuild: "633", LegacyVersion: "5457", PC92Bitmap: 5, HopCount: 99, WriteQueueSize: 128, MaxLineLength: 65536, PC92MaxBytes: 65536,
				Timeouts: config.PeeringTimeouts{LoginSeconds: 10, InitSeconds: 10},
				Peers:    []config.PeeringPeer{{Enabled: true, RemoteCallsign: "GB7REF", Direction: config.PeeringPeerDirectionInbound, Family: config.PeeringPeerFamilyDXSpider, PreferPC9x: tc.pc9x}}}
			cfg.LocalCallsign = "EA8/N0CALL/P-00"
			manager, err := NewManager(cfg, "N0CALL-00", nil, 0, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := manager.SetBuildIdentity("261003", "261003r2", "91abcdef0123", "2026-10-03T16:42:10Z", "go1.26.4"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			if err := manager.Start(ctx); err != nil {
				cancel()
				t.Fatal(err)
			}
			local, remote := net.Pipe()
			endpoint := manager.inboundPeers["GB7REF"]
			session := newSession(local, tc.direction, manager, endpoint, manager.sessionSettings(endpoint))
			done := make(chan error, 1)
			go func() { done <- session.Run() }()
			t.Cleanup(func() { cancel(); _ = remote.Close(); manager.Stop(); <-done })
			reader := bufio.NewReader(remote)
			var wire []string
			read := func() string {
				t.Helper()
				_ = remote.SetReadDeadline(time.Now().Add(10 * time.Second))
				line, err := reader.ReadString('\n')
				if err != nil {
					t.Fatalf("Go session wire: %v transcript=%v", err, wire)
				}
				line = strings.TrimRight(line, "\r\n")
				wire = append(wire, "Go> "+line)
				return line
			}
			write := func(line string) {
				t.Helper()
				wire = append(wire, "DX> "+line)
				_ = remote.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if _, err := io.WriteString(remote, line+"\r\n"); err != nil {
					t.Fatal(err)
				}
			}
			if tc.direction == dirOutbound {
				if got := read(); got != "N0CALL" {
					t.Fatalf("login=%q", got)
				}
				for _, line := range first.TX {
					write(line)
				}
			} else {
				if got := read(); got != "login:" {
					t.Fatalf("prompt=%q", got)
				}
				write("GB7REF")
				line := read()
				want := `PC18^GoCluster Version: 261003 Release tag: 261003r2 Commit: \x391abcdef0123 Built: 2026-10-03T16:42:10Z Go: go1.26.4`
				if tc.pc9x {
					want += " pc9x"
				}
				want += "^5457^"
				if line != want {
					t.Fatalf("session PC18 = %q, want %q", line, want)
				}
				result := reference.frame(line)
				referenceField(t, result.Channel, "version", "54.57")
				referenceField(t, result.Users["N0CALL"], "version", "54.57")
				if tc.pc9x {
					referenceField(t, result.Channel, "do_pc9x", "1")
					referenceField(t, result.Channel, "do_pc91", "false")
				} else {
					referenceField(t, result.Channel, "do_pc9x", "0")
				}
				for _, line := range result.TX {
					write(line)
				}
			}
			completion := "PC22^"
			if tc.direction == dirOutbound {
				completion = "PC20^"
			}
			var result dxReferenceResult
			for i := 0; i < 10; i++ {
				line := read()
				result = reference.frame(line)
				for _, reply := range result.TX {
					write(reply)
				}
				if line == completion {
					break
				}
				if i == 9 {
					t.Fatal("missing handshake completion")
				}
			}
			if tc.pc9x {
				for _, action := range []string{"C", "A"} {
					line := read()
					if !pc92TypeLine(action).match(line) {
						t.Fatalf("recovery %s missing: %s", action, line)
					}
					result = reference.frame(line)
				}
				referenceField(t, result.Channel, "do_pc9x", "1")
				referenceField(t, result.Routes["N0CALL"], "version", "5457")
			} else {
				referenceField(t, result.Channel, "do_pc9x", "0")
			}
			referenceField(t, result.Channel, "state", "normal")
			t.Logf("actual DXSpider receiver transcript:\n%s", strings.Join(wire, "\n"))
		})
	}
}
