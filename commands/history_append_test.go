package commands

import (
	"bytes"
	"errors"
	"math"
	"slices"
	"strings"
	"testing"

	"dxcluster/archive"
	"dxcluster/spot"
)

// Literal expectations protect the old LF-status/CRLF-record text contract
// and the new all-CRLF byte contract without using either renderer as an oracle.
func TestHistoryAppenderStatusTextContract(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		page                          archive.HistoryPage
		token                         string
		older, unreadable, hadMatches bool
		wantBytes, wantText           string
	}{
		{
			name: "no matches ignores exhausted token", token: "IGNORED",
			wantBytes: "No matching retained spots.\r\n",
			wantText:  "No matching retained spots.\n",
		},
		{
			name: "prior matches", hadMatches: true,
			wantBytes: "End of retained history search.\r\n",
			wantText:  "End of retained history search.\n",
		},
		{
			name: "older empty page", older: true,
			wantBytes: "Older retained history page:\r\nEnd of retained history search.\r\n",
			wantText:  "Older retained history page:\nEnd of retained history search.\n",
		},
		{
			name: "cumulative unreadable", unreadable: true,
			wantBytes: "Warning: unreadable archive records were skipped; matching history may be incomplete.\r\nEnd of retained history search.\r\n",
			wantText:  "Warning: unreadable archive records were skipped; matching history may be incomplete.\nEnd of retained history search.\n",
		},
		{
			name: "page unreadable", page: archive.HistoryPage{Unreadable: 2},
			wantBytes: "Warning: unreadable archive records were skipped; matching history may be incomplete.\r\nEnd of retained history search.\r\n",
			wantText:  "Warning: unreadable archive records were skipped; matching history may be incomplete.\nEnd of retained history search.\n",
		},
		{
			name: "count without connection", page: archive.HistoryPage{End: archive.HistoryCountReached},
			wantBytes: "Older history remains; continuation requires a connected client.\r\n",
			wantText:  "Older history remains; continuation requires a connected client.\n",
		},
		{
			name: "count with token", page: archive.HistoryPage{End: archive.HistoryCountReached}, token: "H100112233445566778899AABBCCDDEEFF00",
			wantBytes: "Continue older history: SHOW DX NEXT H100112233445566778899AABBCCDDEEFF00\r\n",
			wantText:  "Continue older history: SHOW DX NEXT H100112233445566778899AABBCCDDEEFF00\n",
		},
		{
			name: "budget without connection", page: archive.HistoryPage{End: archive.HistoryBudgetReached},
			wantBytes: "Search work limit reached; this page is incomplete.\r\nOlder history remains; continuation requires a connected client.\r\n",
			wantText:  "Search work limit reached; this page is incomplete.\nOlder history remains; continuation requires a connected client.\n",
		},
		{
			name: "budget with all statuses", page: archive.HistoryPage{End: archive.HistoryBudgetReached, Unreadable: 1},
			token: "TOKEN", older: true, unreadable: true, hadMatches: true,
			wantBytes: "Older retained history page:\r\nWarning: unreadable archive records were skipped; matching history may be incomplete.\r\nSearch work limit reached; this page is incomplete.\r\nContinue older history: SHOW DX NEXT TOKEN\r\n",
			wantText:  "Older retained history page:\nWarning: unreadable archive records were skipped; matching history may be incomplete.\nSearch work limit reached; this page is incomplete.\nContinue older history: SHOW DX NEXT TOKEN\n",
		},
		{
			name: "record means matches", page: archive.HistoryPage{Spots: []*spot.Spot{{DXCall: "K1ABC"}}},
			wantBytes: "record K1ABC\r\nEnd of retained history search.\r\n",
			wantText:  "record K1ABC\r\nEnd of retained history search.\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			got, err := AppendHistoryPage(nil, tc.page, tc.token, tc.older, tc.unreadable, tc.hadMatches, len(tc.wantBytes), func(dst []byte, s *spot.Spot, budget int) ([]byte, error) {
				calls++
				if budget != len("record K1ABC") {
					t.Fatalf("record budget = %d", budget)
				}
				return append(dst, "record "+s.DXCall...), nil
			})
			if err != nil || string(got) != tc.wantBytes || calls != len(tc.page.Spots) {
				t.Fatalf("byte page = %q, %v, calls=%d; want %q", got, err, calls, tc.wantBytes)
			}
			gotText := RenderHistoryPageWithFormatter(tc.page, tc.token, tc.older, tc.unreadable, tc.hadMatches, func(s *spot.Spot) string {
				return "record " + s.DXCall
			})
			if gotText != tc.wantText {
				t.Fatalf("text page = %q; want %q", gotText, tc.wantText)
			}
		})
	}
}

func TestHistoryAppenderPreservesTextFormatter(t *testing.T) {
	originalWidth := spot.CurrentDXClusterLayout().LineLength
	if err := spot.SetDXClusterLineLength(78); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := spot.SetDXClusterLineLength(originalWidth); err != nil {
			t.Error(err)
		}
	})
	s := &spot.Spot{DXCall: "K1ABC", DECall: "W1AAA", Frequency: 14030}
	page := archive.HistoryPage{Spots: []*spot.Spot{s}}
	const want = "DX de W1AAA:     14030.00  K1ABC                                         0000Z\r\nEnd of retained history search.\n"
	if got := RenderHistoryPage(page, "", false, false, false); got != want {
		t.Fatalf("ordinary text changed: %q; want %q", got, want)
	}
	calls := 0
	got := RenderHistoryPageWithFormatter(page, "", false, false, false, func(got *spot.Spot) string {
		calls++
		if got != s {
			t.Fatal("formatter did not receive the owned spot")
		}
		return "caller CW/DIAG display"
	})
	if calls != 1 || got != "caller CW/DIAG display\r\nEnd of retained history search.\n" {
		t.Fatalf("caller formatter replaced: %q, calls=%d", got, calls)
	}
}

func TestHistoryAppenderEveryByteBudget(t *testing.T) {
	const prefix = "prefix:"
	for _, tc := range []struct {
		name  string
		page  archive.HistoryPage
		token string
		older bool
		want  string
	}{
		{
			name: "empty", want: "prefix:No matching retained spots.\r\n",
		},
		{
			name: "records and statuses",
			page: archive.HistoryPage{
				Spots: []*spot.Spot{{DXCall: "NEW"}, {DXCall: "OLD"}},
				End:   archive.HistoryBudgetReached, Unreadable: 1,
			},
			token: "TOKEN", older: true,
			want: "prefix:Older retained history page:\r\nOLD:\u00e9\r\nNEW:\u00e9\r\nWarning: unreadable archive records were skipped; matching history may be incomplete.\r\nSearch work limit reached; this page is incomplete.\r\nContinue older history: SHOW DX NEXT TOKEN\r\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for limit := -1; limit <= len(tc.want)+2; limit++ {
				backing := make([]byte, len(tc.want)+16)
				copy(backing, prefix)
				dst := backing[:len(prefix)]
				got, err := AppendHistoryPage(dst, tc.page, tc.token, tc.older, false, false, limit, func(dst []byte, s *spot.Spot, budget int) ([]byte, error) {
					record := s.DXCall + ":\u00e9"
					if len(record) > budget {
						return dst, ErrHistoryResponseTooLarge
					}
					return append(dst, record...), nil
				})
				if limit < len(tc.want) {
					if !errors.Is(err, ErrHistoryResponseTooLarge) || string(got) != prefix || &got[0] != &dst[0] {
						t.Fatalf("budget %d: partial page %q, %v", limit, got, err)
					}
				} else if err != nil || string(got) != tc.want || &got[0] != &dst[0] {
					t.Fatalf("budget %d: got %q, %v; want %q in caller backing", limit, got, err, tc.want)
				}
			}
		})
	}
}

func TestHistoryAppenderOrderAndExactCap(t *testing.T) {
	page := archive.HistoryPage{Spots: []*spot.Spot{{DXCall: "NEW"}, {DXCall: "OLD"}}}
	const want = "prefix:Older retained history page:\r\nOLD\r\nNEW\r\nEnd of retained history search.\r\n"
	for _, limit := range []int{len(want) - 1, len(want), math.MaxInt} {
		dst := append(make([]byte, 0, 256), "prefix:"...)
		var calls []string
		var budgets []int
		got, err := AppendHistoryPage(dst, page, "", true, false, false, limit, func(dst []byte, s *spot.Spot, budget int) ([]byte, error) {
			calls = append(calls, s.DXCall)
			budgets = append(budgets, budget)
			if len(s.DXCall) > budget {
				return dst, ErrHistoryResponseTooLarge
			}
			return append(dst, s.DXCall...), nil
		})
		if !slices.Equal(calls, []string{"OLD", "NEW"}) {
			t.Fatalf("order = %v", calls)
		}
		extra := limit - len(want)
		if !slices.Equal(budgets, []int{6 + extra, 3 + extra}) {
			t.Fatalf("budgets = %v; each record CRLF must already be reserved", budgets)
		}
		if limit < len(want) {
			if !errors.Is(err, ErrHistoryResponseTooLarge) || string(got) != "prefix:" {
				t.Fatalf("last-record refusal returned partial success: %q, %v", got, err)
			}
		} else if err != nil || string(got) != want {
			t.Fatalf("exact/large budget = %q, %v; want %q", got, err, want)
		}
	}

	const capBytes = 65536
	const fixed = "P:Older retained history page:\r\n\r\nEnd of retained history search.\r\n"
	record := strings.Repeat("x", capBytes-len(fixed))
	for _, extra := range []int{0, 1} {
		page := archive.HistoryPage{Spots: []*spot.Spot{{Comment: record + strings.Repeat("x", extra)}}}
		dst := append(make([]byte, 0, capBytes), "P:"...)
		got, err := AppendHistoryPage(dst, page, "", true, false, false, capBytes, func(dst []byte, s *spot.Spot, budget int) ([]byte, error) {
			if len(s.Comment) > budget {
				return dst, ErrHistoryResponseTooLarge
			}
			return append(dst, s.Comment...), nil
		})
		if extra == 0 {
			want := "P:Older retained history page:\r\n" + record + "\r\nEnd of retained history search.\r\n"
			if err != nil || len(got) != capBytes || string(got) != want || &got[0] != &dst[0] {
				t.Fatalf("65,536-byte cap: len=%d err=%v", len(got), err)
			}
		} else if !errors.Is(err, ErrHistoryResponseTooLarge) || string(got) != "P:" {
			t.Fatalf("65,537-byte page: len=%d err=%v", len(got), err)
		}
	}
}

func TestHistoryAppenderRejectsBeforeCallback(t *testing.T) {
	appendRecord := func([]byte, *spot.Spot, int) ([]byte, error) {
		t.Fatal("callback invoked without room for all mandatory text/CRLF")
		return nil, nil
	}
	for _, tc := range []struct {
		name  string
		page  archive.HistoryPage
		token string
		older bool
		limit int
	}{
		{name: "negative", limit: -1},
		{name: "prefix", limit: len("prefix") - 1},
		{name: "heading", older: true, limit: len("prefixOlder retained history page:\r\n") - 1},
		{name: "status CRLF", limit: len("prefixNo matching retained spots.\r\n") - 1},
		{
			name: "all row CRLF", page: archive.HistoryPage{Spots: []*spot.Spot{{}, {}}},
			limit: len("prefixEnd of retained history search.\r\n") + 3,
		},
		{
			name: "large token", page: archive.HistoryPage{End: archive.HistoryCountReached},
			token: strings.Repeat("T", 65537), limit: 65536,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backing := []byte("prefix unused")
			snapshot := bytes.Clone(backing)
			got, err := AppendHistoryPage(backing[:6], tc.page, tc.token, tc.older, false, false, tc.limit, appendRecord)
			if string(got) != "prefix" || !errors.Is(err, ErrHistoryResponseTooLarge) || !bytes.Equal(backing, snapshot) {
				t.Fatalf("preflight failed: %q, %v, backing=%q", got, err, backing)
			}
		})
	}
}

func TestHistoryAppenderFailuresDiscardPage(t *testing.T) {
	fail := errors.New("record unavailable")
	page := archive.HistoryPage{Spots: []*spot.Spot{{DXCall: "NEW"}, {DXCall: "OLD"}}}
	dst := append(make([]byte, 0, 256), "prefix"...)
	calls := 0
	got, err := AppendHistoryPage(dst, page, "", true, false, false, 200, func(dst []byte, s *spot.Spot, _ int) ([]byte, error) {
		calls++
		dst = append(dst, s.DXCall...)
		if calls == 2 {
			return dst, fail
		}
		return dst, nil
	})
	// Earlier records may have touched private backing, but none are published.
	if string(got) != "prefix" || &got[0] != &dst[0] || !errors.Is(err, fail) || calls != 2 {
		t.Fatalf("last callback failure = %q, %v, calls=%d", got, err, calls)
	}
	for _, tc := range []struct {
		name     string
		page     archive.HistoryPage
		appender func([]byte, *spot.Spot, int) ([]byte, error)
		wantErr  string
		wantSize bool
	}{
		{name: "nil on empty page", wantErr: "nil"},
		{name: "nil with records", page: page, wantErr: "nil"},
		{
			name: "oversized callback", page: page, wantSize: true,
			appender: func(dst []byte, _ *spot.Spot, budget int) ([]byte, error) {
				return append(dst, make([]byte, budget+1)...), nil
			},
		},
		{
			name: "shortened callback", page: page, wantErr: "shortened",
			appender: func(dst []byte, _ *spot.Spot, _ int) ([]byte, error) {
				return dst[:len(dst)-1], nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := AppendHistoryPage(dst, tc.page, "", true, false, false, 200, tc.appender)
			if string(got) != "prefix" || &got[0] != &dst[0] || err == nil ||
				(tc.wantSize && !errors.Is(err, ErrHistoryResponseTooLarge)) ||
				(tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("malformed callback = %q, %v", got, err)
			}
		})
	}
}
