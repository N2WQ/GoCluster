package peer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestOriginalPeerSpotDateGrammar(t *testing.T) {
	good := []struct {
		date  string
		year  int
		month time.Month
		day   int
	}{
		{"01-Oct-2026", 2026, time.October, 1}, {" 1-Oct-2026", 2026, time.October, 1},
		{"04-Oct-2026", 2026, time.October, 4}, {" 4-Oct-2026", 2026, time.October, 4},
		{"09-oCt-2026", 2026, time.October, 9}, {" 9-oCt-2026", 2026, time.October, 9},
		{"10-Oct-2026", 2026, time.October, 10}, {"31-Oct-2026", 2026, time.October, 31},
		{"29-Feb-2024", 2024, time.February, 29}, {"29-Feb-0000", 0, time.February, 29},
		{"01-Jan-0000", 0, time.January, 1}, {" 1-Jan-0000", 0, time.January, 1},
		{"31-Dec-9999", 9999, time.December, 31}, {" 9-Jan-9999", 9999, time.January, 9},
	}
	for _, tc := range good {
		t.Run(tc.date, func(t *testing.T) {
			want := time.Date(tc.year, tc.month, tc.day, 12, 34, 0, 0, time.UTC)
			for _, kind := range []string{"PC11", "PC61", "PC26"} {
				frame := peerSpotDateFrame(t, kind, tc.date, 3)
				stamp, err := validateOriginalPeerSpot(frame)
				if err != nil || stamp != want || frame.Fields[2] != tc.date {
					t.Fatalf("%s original date %q: stamp=%v want=%v fields=%q err=%v", kind, tc.date, stamp, want, frame.Fields, err)
				}
			}
		})
	}
	for _, date := range []string{
		"4-Oct-2026", "  4-Oct-2026", " 4-Oct-2026 ", "04-Oct-2026 ", " 04-Oct-2026",
		"\t4-Oct-2026", "\u00a04-Oct-2026", "4 -Oct-2026", "04- Oct-2026", "04-Oct- 2026",
		" 0-Oct-2026", "00-Oct-2026", "32-Oct-2026", "31-Apr-2026", "29-Feb-2025",
		"29-Feb-1900", " 4-Foo-2026", "04-Oct-202", "04-Oct-20260", "04-Oct-20 6", "",
	} {
		t.Run("reject/"+date, func(t *testing.T) {
			for _, kind := range []string{"PC11", "PC61", "PC26"} {
				if _, err := validateOriginalPeerSpot(peerSpotDateFrame(t, kind, date, 3)); err == nil {
					t.Fatalf("%s accepted malformed date %q", kind, date)
				}
			}
		})
	}
}

func peerSpotDateFrame(t *testing.T, kind, date string, hop int) *Frame {
	t.Helper()
	wire := strings.Replace(originalSpotTestWire(kind, "CQ"), "01-oCt-2026^1200Z", date+"^1234Z", 1)
	wire = strings.TrimSuffix(wire, "H3^") + fmt.Sprintf("H%d^", hop)
	frame, err := ParseFrame(wire)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func TestPeerSpotValidatedTimestampBeforeAdmission(t *testing.T) {
	want := time.Date(2024, time.October, 4, 12, 34, 0, 0, time.UTC)
	for _, kind := range []string{"PC11", "PC61", "PC26"} {
		for _, forward := range []bool{false, true} {
			for _, hop := range []int{0, 1, 3} {
				t.Run(fmt.Sprintf("%s/forward=%t/hop=%d", kind, forward, hop), func(t *testing.T) {
					frame := peerSpotDateFrame(t, kind, " 4-Oct-2024", hop)
					m, source, destination, ingest := peerSpotTestManager(forward, true)
					m.HandleFrame(frame, source)
					if len(ingest) != 1 {
						t.Fatal("valid original did not enter local processing with age disabled")
					}
					if local := <-ingest; local.Time != want {
						t.Fatalf("local time=%v want validated %v", local.Time, want)
					}
					if forward && hop > 1 {
						key := fmt.Sprintf("dx:%s:K1ABC:W1XYZ:14074.0:%d", kind, want.Unix())
						if !m.dedupe.contains(key, time.Now()) || m.dedupe.items.Len() != 1 {
							t.Fatal("peer key did not use the validated original timestamp")
						}
						wantLine := strings.TrimSuffix(frame.Raw, "H3^") + "H2^~"
						if len(destination.writeCh) != 1 || <-destination.writeCh != wantLine {
							t.Fatalf("original date changed or relay missing: want %q", wantLine)
						}
					} else if m.dedupe.items.Len() != 0 || len(destination.writeCh) != 0 {
						t.Fatal("valid local-only input entered peer dedupe or relay")
					}
					if len(source.writeCh) != 0 {
						t.Fatal("source received its own relay")
					}
				})
			}
		}
	}
}

func TestPeerSpotSpacePaddedDateRetainsStaleAdmissionGate(t *testing.T) {
	for _, kind := range []string{"PC11", "PC61", "PC26"} {
		for _, forward := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/forward=%t", kind, forward), func(t *testing.T) {
				frame := peerSpotDateFrame(t, kind, " 4-Oct-2024", 3)
				m, source, destination, ingest := peerSpotTestManager(forward, true)
				m.maxAgeSeconds = 60
				m.HandleFrame(frame, source)
				entries, keyBytes, refused := m.dedupe.occupancy()
				if len(ingest) != 0 || len(destination.writeCh) != 0 || len(source.writeCh) != 0 || entries != 0 || keyBytes != 0 || refused != 0 {
					t.Fatal("stale original used fallback-to-now or entered local/peer admission")
				}
			})
		}
	}
}

func TestPeerSpotDateSpellingsSharePeerDedupeIdentity(t *testing.T) {
	want := time.Date(2024, time.October, 4, 12, 34, 0, 0, time.UTC)
	for _, kind := range []string{"PC11", "PC61", "PC26"} {
		for _, first := range []string{"04-Oct-2024", " 4-Oct-2024"} {
			t.Run(kind+"/first="+first, func(t *testing.T) {
				second := " 4-Oct-2024"
				if first == second {
					second = "04-Oct-2024"
				}
				m, source, destination, ingest := peerSpotTestManager(true, true)
				for i, date := range []string{first, second} {
					frame := peerSpotDateFrame(t, kind, date, 3)
					m.HandleFrame(frame, source)
					if len(ingest) != 1 {
						t.Fatal("peer duplicate suppression changed local eligibility")
					}
					if local := <-ingest; local.Time != want {
						t.Fatalf("date %q yielded local time %v, want %v", date, local.Time, want)
					}
					if i == 0 {
						wantLine := strings.TrimSuffix(frame.Raw, "H3^") + "H2^~"
						if len(destination.writeCh) != 1 || <-destination.writeCh != wantLine {
							t.Fatal("first original date was changed or lost")
						}
					} else if len(destination.writeCh) != 0 {
						t.Fatal("equivalent original date created a second peer identity")
					}
				}
				key := fmt.Sprintf("dx:%s:K1ABC:W1XYZ:14074.0:%d", kind, want.Unix())
				if m.dedupe.items.Len() != 1 || !m.dedupe.contains(key, time.Now()) {
					t.Fatal("date spellings did not retain the existing peer identity")
				}
			})
		}
	}
}

// The sender process is one-shot: CommandContext owns cancellation and joins
// it before returning. Both CWD and all DXSpider state are test-owned, while
// the pinned source and formatter remain unmodified.
func generateReferencePeerSpots(t *testing.T, at time.Time) map[string]string {
	t.Helper()
	return generateReferencePeerSpotsWithInput(t, at, "CQ TEST", "203.0.113.7")
}

func generateReferencePeerSpotsWithInput(t *testing.T, at time.Time, comment, ip string) map[string]string {
	t.Helper()
	root, perl := os.Getenv("DXSPIDER_ROOT"), os.Getenv("DXSPIDER_PERL")
	if root == "" || perl == "" {
		t.Skip("spot sender observations require explicit DXSPIDER_ROOT and DXSPIDER_PERL; this skip is not interoperability evidence")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	commit, err := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil || strings.TrimSpace(string(commit)) != dxSpiderReferenceCommit {
		t.Fatalf("reference checkout must be %s: %s %v", dxSpiderReferenceCommit, commit, err)
	}
	if err := exec.CommandContext(ctx, "git", "-C", root, "diff", "--quiet", dxSpiderReferenceCommit, "--", "perl", "data/prefix_data.pl").Run(); err != nil {
		t.Fatalf("reference sender source or prefix data differs from its pin: %v", err)
	}
	repo, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	args := []string{}
	if lib := os.Getenv("DXSPIDER_PERL_LIB"); lib != "" {
		args = append(args, "-I", lib)
	}
	args = append(args, filepath.Join(repo, "scripts", "spot-dxspider-generate.pl"))
	command := exec.CommandContext(ctx, perl, args...)
	command.Dir = t.TempDir()
	command.Env = append(os.Environ(), "DXSPIDER_TEST_STATE="+command.Dir, "GOCLUSTER_ROOT="+repo, "DXSPIDER_TEST_AT="+strconv.FormatInt(at.Unix(), 10), "DXSPIDER_TEST_COMMENT="+comment, "DXSPIDER_TEST_IP="+ip, "LC_ALL=C")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("actual DXSpider spot sender failed: %v\n%s", err, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("actual DXSpider spot sender emitted diagnostics: %s", stderr.String())
	}
	var encoded map[string]string
	if err := json.Unmarshal(output, &encoded); err != nil {
		t.Fatal(err)
	}
	if len(encoded) != 3 {
		t.Fatalf("sender did not return all three pinned generators: %q", encoded)
	}
	frames := make(map[string]string, 3)
	for _, kind := range []string{"PC11", "PC61", "PC26"} {
		frames[kind] = decodeSpotReferenceBytes(t, encoded[kind])
	}
	return frames
}

func readNativePeerSpotFrame(t *testing.T, sentence string) *Frame {
	t.Helper()
	local, remote := net.Pipe()
	reader := newLineReader(local, MaxPeerFrameBytes, MaxPeerFrameBytes, nil)
	joined, written := make(chan struct{}), make(chan error, 1)
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close(); <-joined; reader.release() })
	deadline := time.Now().Add(time.Second)
	go func() {
		defer close(joined)
		if err := remote.SetWriteDeadline(deadline); err != nil {
			written <- err
			return
		}
		_, err := io.WriteString(remote, sentence+"\r\n")
		written <- err
	}()
	line, err := reader.ReadLine(deadline)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	frame, err := ParseFrame(line)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

type nativePeerSpotObservation struct {
	frame       *Frame
	local       *spot.Spot
	wire        string
	received    *Frame
	peerEntries int
}

func observeNativePeerSpot(t *testing.T, sentence string, forward, modern bool) nativePeerSpotObservation {
	t.Helper()
	frame := readNativePeerSpotFrame(t, sentence)
	source := &session{id: "src", remoteCall: "N1SRC", ctx: context.Background(), writeCh: make(chan string, 1)}
	destination, remote := newTransportTestSession(t)
	destination.id, destination.pc9x = "dst", modern
	ingest := make(chan *spot.Spot, 1)
	manager := &Manager{
		cfg: config.PeeringConfig{ForwardSpots: forward}, ingest: ingest, dedupe: newBoundedDedupe(time.Minute, 16, 4096),
		sessions: sessionTestIndex(map[string]*session{"src": source, "dst": destination}),
	}
	// Fixed historical sender fixtures intentionally disable the age cutoff.
	// Root-owned stale-admission tests cover that gate with the same spelling.
	manager.HandleFrame(frame, source)
	result := nativePeerSpotObservation{frame: frame}
	select {
	case result.local = <-ingest:
	default:
	}
	result.peerEntries, _, _ = manager.dedupe.occupancy()
	if len(source.writeCh) != 0 {
		t.Fatal("native spot relay returned to its source")
	}
	if len(destination.writeCh) != 0 {
		result.wire, result.received = readNativePeerSpotWriter(t, destination, remote)
	}
	return result
}

func TestDXSpiderReferenceGeneratedPeerSpotDates(t *testing.T) {
	for _, day := range []int{1, 9, 10, 31} {
		t.Run(fmt.Sprintf("day=%d", day), func(t *testing.T) {
			at := time.Date(2026, time.October, day, 12, 0, 0, 0, time.UTC)
			generated := generateReferencePeerSpots(t, at)
			referenceDate := fmt.Sprintf("%2d-Oct-2026", day)
			for _, kind := range []string{"PC11", "PC61", "PC26"} {
				payload := "14074.1^K1ABC^" + referenceDate + "^1200Z^CQ TEST^W1XYZ^GB7REF"
				suffix := "^H3^~"
				switch kind {
				case "PC61":
					payload += "^203.0.113.7"
				case "PC26":
					suffix = "^ ^~"
				}
				if got, want := generated[kind], kind+"^"+payload+suffix; got != want {
					t.Fatalf("pinned %s did not emit the controlled reference date: got %q want %q", kind, got, want)
				}
				for _, zeroPad := range []bool{false, true} {
					date, sentence := referenceDate, generated[kind]
					if zeroPad {
						date = at.Format("02-Jan-2006")
						sentence = strings.Replace(sentence, "^"+referenceDate+"^", "^"+date+"^", 1)
					}
					for _, forward := range []bool{false, true} {
						for _, modern := range []bool{false, true} {
							t.Run(fmt.Sprintf("%s/zero-padded-equivalent=%t/forward=%t/modern=%t", kind, zeroPad, forward, modern), func(t *testing.T) {
								result := observeNativePeerSpot(t, sentence, forward, modern)
								if result.local == nil || !result.local.Time.Equal(at) || result.local.Time.Location() != time.UTC {
									t.Fatalf("native generated input lacks the validated local timestamp: local=%+v want=%s", result.local, at)
								}
								if result.frame.Fields[2] != date {
									t.Fatalf("native reader/parser changed original date bytes: %q != %q", result.frame.Fields[2], date)
								}
								wantWire, wantEntries := "", 0
								if forward && kind != "PC26" {
									outKind := kind
									outPayload := "14074.1^K1ABC^" + date + "^1200Z^CQ TEST^W1XYZ^GB7REF"
									if kind == "PC61" && modern {
										outPayload += "^203.0.113.7"
									} else if kind == "PC61" {
										outKind = "PC11"
									}
									wantWire, wantEntries = outKind+"^"+outPayload+"^H2^~\r\n", 1
								}
								if result.wire != wantWire || result.peerEntries != wantEntries {
									t.Fatalf("native forwarding/no-hop gates or literal date changed: wire=%q entries=%d wantWire=%q wantEntries=%d", result.wire, result.peerEntries, wantWire, wantEntries)
								}
								if kind == "PC26" && result.frame.Hop != 0 {
									t.Fatalf("actual PC26 generator acquired a transport hop: %d", result.frame.Hop)
								}
							})
						}
					}
				}
			}
		})
	}
}

func TestDXSpiderReferenceGeneratedPC26AddedTransportHop(t *testing.T) {
	at := time.Date(2026, time.October, 9, 12, 0, 0, 0, time.UTC)
	generated := generateReferencePeerSpots(t, at)["PC26"]
	// This is explicitly a transport adaptation of the actual no-hop merge
	// record, not a claim that DXSpider's PC26 generator emits H3.
	sentence := strings.TrimSuffix(generated, "~") + "H3^~"
	for _, modern := range []bool{false, true} {
		t.Run(fmt.Sprintf("modern=%t", modern), func(t *testing.T) {
			result := observeNativePeerSpot(t, sentence, true, modern)
			if result.local == nil || !result.local.Time.Equal(at) || result.frame.Hop != 3 || result.peerEntries != 1 {
				t.Fatalf("adapted hop-bearing PC26 failed its local/peer admission: %+v", result)
			}
			want := ""
			if modern {
				want = "PC26^14074.1^K1ABC^ 9-Oct-2026^1200Z^CQ TEST^W1XYZ^GB7REF^ ^H2^~\r\n"
			}
			if result.wire != want {
				t.Fatalf("adapted PC26 date or destination gate changed: %q want %q", result.wire, want)
			}
		})
	}
}

func TestDXSpiderReferenceGeneratedInvalidCalendarRejected(t *testing.T) {
	at := time.Date(2026, time.October, 31, 12, 0, 0, 0, time.UTC)
	generated := generateReferencePeerSpots(t, at)
	for _, kind := range []string{"PC11", "PC61", "PC26"} {
		for _, date := range []string{"31-Apr-2026", "29-Feb-2026", " 0-Oct-2026"} {
			sentence := strings.Replace(generated[kind], "^31-Oct-2026^", "^"+date+"^", 1)
			for _, forward := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/date=%s/forward=%t", kind, date, forward), func(t *testing.T) {
					result := observeNativePeerSpot(t, sentence, forward, true)
					if result.local != nil || result.wire != "" || result.peerEntries != 0 {
						t.Fatalf("reference-derived invalid calendar entered local processing, peer dedupe or relay: %+v", result)
					}
				})
			}
		}
	}
}
