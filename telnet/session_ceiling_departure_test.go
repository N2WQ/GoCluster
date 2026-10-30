package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/filter"
)

// TestSessionCeilingDepartingSameIdentity exercises real final teardown while
// a same-identity login is waiting for its stripe. The removal hook holds the
// old stripe only after its membership was actually deleted and unlocked.
func TestSessionCeilingDepartingSameIdentity(t *testing.T) {
	for _, competingWinner := range []bool{false, true} {
		t.Run(fmt.Sprintf("competing_winner_%t", competingWinner), func(t *testing.T) {
			s := newSessionCeilingServer(t, 1)
			old := v7CurrentTestClient(t, s, "K1ABC-1")
			old.configuredSettings = filter.SettingsConfiguration{Dialect: "go", NoiseClass: "URBAN", DedupePolicy: "FAST"}
			old.configurationInitialized = true
			cfg, err := old.captureV7Configuration()
			if err != nil {
				t.Fatal(err)
			}
			cfg.Settings.NoiseClass = "QUIET"
			baseline, err := filter.V7PresetFromConfiguration(cfg, "go")
			if err != nil {
				t.Fatal(err)
			}
			old.v7PresetReferences.GO = &filter.V7PresetReference{Name: "CONTEST", Baseline: baseline}
			s.registerClient(old)
			if err := old.saveFilter(); err != nil {
				t.Fatalf("initial owner save: %v", err)
			}
			if err := filter.SaveV7Preset(old.callsign, "CONTEST", baseline); err != nil {
				t.Fatalf("initial shared preset: %v", err)
			}
			profile := filepath.Join(filter.UserDataDir, old.callsign+".yaml")
			library := presetLibraryPathFor(old.callsign)
			for _, path := range []string{profile + ".pre-v6.bak", library + ".pre-v6.bak"} {
				if err := os.WriteFile(path, []byte("existing backup for "+filepath.Base(path)), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			old.configuredSettings.NoiseClass = "INDUSTRIAL"
			old.interrupt()

			enteredSave, allowSave := make(chan struct{}), make(chan struct{})
			removed, allowStripe := make(chan struct{}), make(chan struct{})
			waiting := make(chan struct{})
			var saveOnce, saveReleaseOnce, removeOnce, stripeReleaseOnce sync.Once
			var replacementProbes atomic.Int32
			replacement := configurationTestClient(s, old.callsign)
			s.commitConfigurationFn = func(prepared *filter.PreparedUserRecord) error {
				saveOnce.Do(func() { close(enteredSave); <-allowSave })
				return prepared.Commit()
			}
			s.testLoginCapacityProbe = func(c *Client, _, _ bool) {
				if c == replacement && replacementProbes.Add(1) == 2 {
					close(waiting)
				}
			}
			s.testAfterClientRemoval = func(c *Client) {
				if c == old {
					removeOnce.Do(func() { close(removed); <-allowStripe })
				}
			}
			unblockSave := func() { saveReleaseOnce.Do(func() { close(allowSave) }) }
			unblockStripe := func() { stripeReleaseOnce.Do(func() { close(allowStripe) }) }
			oldDone := make(chan struct{})
			go func() { defer close(oldDone); s.unregisterClient(old) }()
			var replacementFinished <-chan struct{}
			t.Cleanup(func() {
				unblockSave()
				unblockStripe()
				waitConfigurationTest(t, oldDone)
				if replacementFinished != nil {
					waitConfigurationTest(t, replacementFinished)
				}
				s.clientsMutex.RLock()
				owners := make([]*Client, 0, len(s.clients))
				for _, c := range s.clients {
					owners = append(owners, c)
				}
				s.clientsMutex.RUnlock()
				for _, c := range owners {
					s.unregisterClient(c)
				}
			})
			waitConfigurationTest(t, enteredSave)
			result, finished := ceilingRestoreAsync(s, replacement, time.Now().Add(10*time.Second))
			replacementFinished = finished
			// The second observation is a periodic probe: the initial probe has
			// completed and the reconnect is still waiting for the held stripe.
			waitConfigurationTest(t, waiting)
			ceilingOccupancy(t, s, 1, 0)
			select {
			case err := <-result:
				t.Fatalf("reconnect returned before owner teardown: %v", err)
			default:
			}
			unblockSave()
			waitConfigurationTest(t, removed)
			ceilingOccupancy(t, s, 0, 0)

			paths := []string{profile, library, profile + ".pre-v6.bak", library + ".pre-v6.bak"}
			before := make(map[string][]byte, len(paths))
			for _, path := range paths {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("read post-teardown state %s: %v", path, err)
				}
				before[path] = data
			}
			if !bytes.Contains(before[profile], []byte("noise_class: INDUSTRIAL")) {
				t.Fatal("old owner's final save did not precede the reconnect decision")
			}
			rootBefore := departureDirectoryNames(t, filepath.Dir(profile))
			libraryBefore := departureDirectoryNames(t, filepath.Dir(library))

			var competitor *Client
			if competingWinner {
				for i := 2; i < 10000; i++ {
					call := fmt.Sprintf("K%dXYZ-1", i)
					if configurationStripe(call) != configurationStripe(old.callsign) {
						competitor = configurationTestClient(s, call)
						break
					}
				}
				if competitor == nil {
					t.Fatal("could not find a competing identity on a different stripe")
				}
				if _, err := s.restoreAndRegisterClient(competitor, time.Now().UTC(), time.Now().Add(5*time.Second)); err != nil {
					t.Fatalf("competing identity did not take freed slot: %v", err)
				}
				ceilingOccupancy(t, s, 1, 0)
			}
			rootEntries := departureDirectoryNames(t, filepath.Dir(profile))
			libraryEntries := departureDirectoryNames(t, filepath.Dir(library))
			if competingWinner {
				expectedRoot := append(strings.Split(rootBefore, "\n"), competitor.callsign+".yaml")
				slices.Sort(expectedRoot)
				if want := strings.Join(expectedRoot, "\n"); rootEntries != want || libraryEntries != libraryBefore {
					t.Fatalf("unexpected artifacts while competitor logged in: profiles=%q want=%q presets=%q want=%q", rootEntries, want, libraryEntries, libraryBefore)
				}
				// The competing full registry is rechecked even while the old
				// session still owns its stripe. The snapshot predates this race.
				err := ceilingResult(t, result)
				if !errors.Is(err, errSessionFull) {
					t.Fatalf("replacement after competitor filled capacity = %v", err)
				}
				waitConfigurationTest(t, finished)
				for _, path := range paths {
					after, err := os.ReadFile(path)
					if err != nil || !bytes.Equal(before[path], after) {
						t.Fatalf("rejected reconnect changed %s: %v", path, err)
					}
				}
				if got := departureDirectoryNames(t, filepath.Dir(profile)); got != rootEntries {
					t.Fatalf("rejected reconnect changed profile directory: %q != %q", got, rootEntries)
				}
				if got := departureDirectoryNames(t, filepath.Dir(library)); got != libraryEntries {
					t.Fatalf("rejected reconnect changed preset directory: %q != %q", got, libraryEntries)
				}
				s.clientsMutex.RLock()
				current := s.clients[competitor.callsign]
				s.clientsMutex.RUnlock()
				if current != competitor {
					t.Fatal("rejected reconnect displaced the competing owner")
				}
				unblockStripe()
				waitConfigurationTest(t, oldDone)
			} else {
				unblockStripe()
				waitConfigurationTest(t, oldDone)
				err := ceilingResult(t, result)
				waitConfigurationTest(t, finished)
				if err != nil {
					t.Fatalf("reconnect did not acquire freed capacity: %v", err)
				}
				if replacement.configuredSettings.NoiseClass != "INDUSTRIAL" || replacement.v7PresetReferences.GO == nil || replacement.v7PresetReferences.GO.Name != "CONTEST" || replacement.v7PresetReferences.GO.Baseline.Settings.NoiseClass != "QUIET" {
					t.Fatal("reconnect did not restore the final saved preferences and preset baseline")
				}
				s.clientsMutex.RLock()
				current := s.clients[replacement.callsign]
				s.clientsMutex.RUnlock()
				if current != replacement {
					t.Fatal("reconnect did not become the current owner")
				}
			}
			ceilingOccupancy(t, s, 1, 0)
		})
	}
}

func departureDirectoryNames(t *testing.T, path string) string {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	return strings.Join(names, "\n")
}
