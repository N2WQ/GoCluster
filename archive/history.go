package archive

import (
	"context"
	"errors"
	"fmt"
	"time"

	"dxcluster/config"
	"dxcluster/spot"

	"github.com/cockroachdb/pebble"
)

const historyMaxLimit = 250

// MaxHistoryRecordBytes is the largest value accepted by any readable layout.
// V7 has seventeen unrestricted uint16-length strings, two two-letter states,
// and two CW fields limited to 1024 bytes each. Older layouts are smaller.
const MaxHistoryRecordBytes = recordHeaderSize + fieldDXState*65535 + 2*2 + 2*1024

// MaxHistoryScans limits simultaneous collecting scans across archive instances.
// A scan owns one iterator and decodes one candidate at a time. Its slot is
// released after iterator close, before the caller can publish its page.
//
// On the 64-bit Go 1.27.1 builds the conservative candidate-work allowance
// is 128 MiB per slot, independent of the number of rows examined:
//   - <3 MiB for input plus 21 field copies, <4 MiB for the EVENT, two
//     toxicity CSV and CW string-header vectors. Legacy comment EVENT
//     recovery examines one token at a time.
//   - Token uppercasing consumes <3 MiB of backing in aggregate. Each raw
//     byte yields at most four UTF-8 bytes; builder growth and allocator
//     rounding bound a backing by 16*inputBytes+32, including tiny strings.
//   - A decoded Spot has 10 normalized strings. The filter's lazy view has
//     fewer than 24 text fields. Even including active normalization, copied
//     CTY headers, filter/format temporaries and a growing builder's old and
//     new storage, there are fewer than 64 full-field derived backings. Each
//     begins at one uint16 field and costs <1,049,000 bytes by the same bound.
//     Already canonical taxonomy names share immutable backing.
//   - CTY's temporary slash-segment vector is <5 MiB; one uncharged compact
//     projection is <1 MiB. Matching uses fixed-depth programs and fixed
//     observation captures, with one 1 KiB literal-comment table.
//
// These are simultaneous GoCluster candidate resources. Pebble's shared
// cache/iterator internals, existing process caches, the Go collector and
// separately charged retained history pages have different owners.
const MaxHistoryScans = 2

var historyScanSlots = make(chan struct{}, MaxHistoryScans)

// ErrHistoryBusy asks the caller to discard any partial collected projection
// and keep the previous continuation available for retry.
var ErrHistoryBusy = errors.New("archive: history capacity busy")

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
	// BorrowMatch permits a reader to reuse the candidate after Match returns.
	// Set only for a synchronous matcher that neither mutates nor retains the
	// Spot. Readers may always supply owned candidates. Selected Spots in a
	// non-collecting result remain independently owned in either case.
	BorrowMatch bool
	// Collect receives selected rows newest first while the iterator is open.
	// It must charge and copy the fields it needs before returning, must not
	// retain the Spot or block on delivery, and must release all charges if
	// ReadHistoryPage returns any error. A nil collector requests owned Spots.
	Collect func(*spot.Spot) error
	Now     time.Time
	Done    <-chan struct{}
}

// HistoryPage selects newest-first rows. Examined includes a non-consuming
// lookahead when present; Before names the last consumed key, so that lookahead
// remains eligible on the next live page. Unreadable counts skipped bad keys
// and records; callers must preserve this warning across a continued search.
type HistoryPage struct {
	Spots      []*spot.Spot
	Selected   int // Includes rows accepted by Collect when Spots is nil.
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
	if request.Collect != nil {
		select {
		case historyScanSlots <- struct{}{}:
			defer func() { <-historyScanSlots }()
		default:
			return HistoryPage{}, ErrHistoryBusy
		}
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
		if err != nil && request.Collect != nil {
			page = HistoryPage{}
		}
	}()
	return w.scanHistoryPage(iter, request)
}

func (w *Writer) scanHistoryPage(iter *pebble.Iterator, request HistoryRequest) (HistoryPage, error) {
	var page HistoryPage
	var scratch *spot.Spot
	if request.Collect == nil {
		page.Spots = make([]*spot.Spot, 0, request.Limit)
	}
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
			row, decodeErr := decodeHistorySpot(ts, iter, scratch)
			if decodeErr != nil {
				page.Unreadable++
			} else if request.Match == nil || request.Match(row) {
				if request.Collect == nil {
					page.Spots = append(page.Spots, row)
					row = nil // The page owns it through native text/CC11 presentation.
				} else if err := request.Collect(row); err != nil {
					return HistoryPage{}, err
				}
				page.Selected++
			}
			if request.BorrowMatch && decodeErr == nil {
				scratch = row
				if scratch != nil {
					// Release strings and vectors before fetching the next candidate.
					// A collector has already copied the selected fields it retains.
					*scratch = spot.Spot{}
				}
			}
		}
		valid = iter.Prev()
		if page.Selected == request.Limit || page.Examined == recentScanMax-1 {
			if valid {
				// This visit establishes older search without consuming its row.
				page.Examined++
				if ts, ok := parseSpotKey(page.Before); !ok || ts < 0 {
					return page, fmt.Errorf("archive: malformed history boundary prevents continuation")
				}
				page.End = HistoryBudgetReached
				if page.Selected == request.Limit {
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

func decodeHistorySpot(ts int64, iter *pebble.Iterator, scratch *spot.Spot) (*spot.Spot, error) {
	// The length of an external Pebble value is available before fetching it.
	// An oversized value was already unreadable in decodeRecord. Skip it
	// without materializing a potentially oversized iterator value buffer.
	value := iter.LazyValue()
	if value.Len() > MaxHistoryRecordBytes {
		return nil, errInvalidRecord
	}
	return decodeSpotInto(ts, iter.Value(), scratch)
}
