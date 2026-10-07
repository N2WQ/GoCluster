package archive

import (
	"context"
	"fmt"
	"time"

	"dxcluster/config"
	"dxcluster/spot"

	"github.com/cockroachdb/pebble"
)

const historyMaxLimit = 250

// HistoryEnd describes why a page stopped. Count and budget boundaries retain
// an exclusive continuation position; Exhausted means the eligible key range
// was exhausted, not that every stored record was readable.
type HistoryEnd uint8

const (
	HistoryExhausted HistoryEnd = iota
	HistoryCountReached
	HistoryBudgetReached
)

// HistoryRequest owns no persistent archive resources. Before is a copied full
// primary key, and Done cancels this request independently of Writer.Stop.
type HistoryRequest struct {
	Limit  int
	Before []byte
	Match  func(*spot.Spot) bool
	Now    time.Time
	Done   <-chan struct{}
}

// HistoryPage selects newest-first rows. Examined includes a non-consuming
// lookahead when present; Before names the last consumed key, so that lookahead
// remains eligible on the next live page. Unreadable counts skipped bad keys
// and records; callers must preserve this warning across a continued search.
type HistoryPage struct {
	Spots      []*spot.Spot
	Before     []byte
	Examined   int
	Unreadable int
	End        HistoryEnd
}

// beginRead registers a request before it can create an iterator. Stop signals
// cancellation first, then acquires readMu exclusively before closing Pebble.
func (w *Writer) beginRead() error {
	if w == nil {
		return fmt.Errorf("archive: writer is nil")
	}
	w.readMu.RLock()
	if w.db == nil {
		w.readMu.RUnlock()
		return fmt.Errorf("archive: writer is nil")
	}
	if err := w.readCanceled(nil); err != nil {
		w.readMu.RUnlock()
		return err
	}
	return nil
}

func (w *Writer) readCanceled(done <-chan struct{}) error {
	select {
	case <-w.stop:
		return fmt.Errorf("archive: writer stopped")
	case <-done:
		return context.Canceled
	default:
		return nil
	}
}

// ReadHistoryPage scans one fresh view of the timestamp primary range. Its
// 200000 candidate budget reserves one visit for continuation lookahead, so a
// page consumes at most 199999 rows. Writes and retention cleanup are unchanged;
// each continuation applies its own request-time cutoff and iterator view.
func (w *Writer) ReadHistoryPage(request HistoryRequest) (page HistoryPage, err error) {
	if err = w.beginRead(); err != nil {
		return page, err
	}
	defer w.readMu.RUnlock()
	if request.Limit < 1 || request.Limit > historyMaxLimit {
		return page, fmt.Errorf("archive: history limit must be between 1 and %d", historyMaxLimit)
	}
	if len(request.Before) != 0 {
		if ts, valid := parseSpotKey(request.Before); !valid || ts < 0 {
			return page, fmt.Errorf("archive: invalid history continuation key")
		}
	}
	if err = w.readCanceled(request.Done); err != nil {
		return page, err
	}
	now := request.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	retention := w.cfg.RetentionSeconds
	if retention <= 0 {
		retention = config.DefaultArchiveRetentionSeconds
	}
	lower := spotIterLower
	if cutoff := retentionCutoff(now.UTC().UnixNano(), retention); cutoff > 0 {
		lower = spotKeyBytes(cutoff, 0)
	}
	iter, err := w.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: spotIterUpper})
	if err != nil {
		return page, fmt.Errorf("archive: history iterator: %w", err)
	}
	defer func() {
		if closeErr := iter.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("archive: history close iterator: %w", closeErr)
		}
	}()
	return w.scanHistoryPage(iter, request)
}

func (w *Writer) scanHistoryPage(iter *pebble.Iterator, request HistoryRequest) (HistoryPage, error) {
	page := HistoryPage{Spots: make([]*spot.Spot, 0, request.Limit)}
	var valid bool
	if len(request.Before) == 0 {
		valid = iter.Last()
	} else {
		valid = iter.SeekLT(request.Before)
	}
	for valid {
		if err := w.readCanceled(request.Done); err != nil {
			return page, err
		}
		page.Examined++
		ts, keyValid := parseSpotKey(iter.Key())
		page.Before = page.Before[:0]
		if !keyValid || ts < 0 {
			page.Unreadable++
		} else {
			page.Before = append(page.Before, iter.Key()...)
			if row, decodeErr := decodeSpot(ts, iter.Value()); decodeErr != nil {
				page.Unreadable++
			} else if request.Match == nil || request.Match(row) {
				page.Spots = append(page.Spots, row)
			}
		}
		valid = iter.Prev()
		if len(page.Spots) == request.Limit || page.Examined == recentScanMax-1 {
			if valid {
				// This visit establishes older search without consuming its row.
				page.Examined++
				if ts, ok := parseSpotKey(page.Before); !ok || ts < 0 {
					return page, fmt.Errorf("archive: malformed history boundary prevents continuation")
				}
				page.End = HistoryBudgetReached
				if len(page.Spots) == request.Limit {
					page.End = HistoryCountReached
				}
			}
			break
		}
	}
	if err := w.readCanceled(request.Done); err != nil {
		return page, err
	}
	if err := iter.Error(); err != nil {
		return page, fmt.Errorf("archive: history iterate: %w", err)
	}
	if page.End == HistoryExhausted {
		page.Before = nil
	}
	return page, nil
}
