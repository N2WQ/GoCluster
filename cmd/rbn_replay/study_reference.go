// Study references use only frozen raw archive observations. They never consume
// corrections, SNR weights, licensing, model ranks or the legacy stability
// collector. Day-owned raw/index storage has the enforced input-size bound in
// study_plan.go; per-query family scratch is discarded before the next row.
package main

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	rbnfeed "dxcluster/rbn"
	"dxcluster/spot"
)

type studyRaw struct {
	Row     rbnHistoryRow
	Valid   bool
	Ordinal int64
	Family  string
	A       bool
	Episode string
}

type studyReference struct {
	Strong   bool   `json:"strong"`
	Identity string `json:"identity,omitempty"`
	Families int    `json:"families"`
	Reason   string `json:"reason"`
}

type studyReceiverGroup struct {
	Aliases []string `json:"aliases"`
}

type studyRawDay struct {
	Rows          []studyRaw
	Index         map[string][]int
	Aliases       map[string]string
	InputSHA256   string
	Start, End    time.Time
	First, Last   time.Time
	WarmupSeconds int
}

func studyReceiverBase(receiver string) string {
	receiver = spot.NormalizeCallsign(receiver)
	if dash := strings.IndexByte(receiver, '-'); dash >= 0 {
		receiver = receiver[:dash]
	}
	vote, base, ok := spot.CorrectionFamilyKeyPair(receiver)
	if !ok {
		return receiver
	}
	if base != "" {
		return base
	}
	return vote
}

func loadStudyReceiverAliases(file studyFile) (map[string]string, error) {
	if err := studyCheckFile(file); err != nil {
		return nil, err
	}
	data, err := studyReadBounded(file.Path, studyMaxJSONBytes)
	if err != nil {
		return nil, err
	}
	var groups []studyReceiverGroup
	if err := studyDecodeJSON(data, &groups); err != nil || groups == nil {
		return nil, errors.Join(err, errors.New("receiver metadata must be an explicit array of alias groups"))
	}
	parents := make(map[string]string)
	root := func(key string) string {
		parent, ok := parents[key]
		if !ok {
			parents[key] = key
			return key
		}
		for parent != parents[parent] {
			parent = parents[parent]
		}
		parents[key] = parent
		return parent
	}
	for _, group := range groups {
		if len(group.Aliases) < 1 || len(group.Aliases) > 256 {
			return nil, errors.New("receiver alias group must contain 1..256 aliases")
		}
		first := ""
		for _, alias := range group.Aliases {
			key := studyReceiverBase(alias)
			if key == "" || len(alias) > 64 || len(parents) >= 100_000 {
				return nil, errors.New("receiver metadata exceeds alias size/cardinality bound")
			}
			current := root(key)
			if first == "" {
				first = current
				continue
			}
			first = root(first)
			if first < current {
				parents[current] = first
			} else {
				parents[first], first = current, current
			}
		}
	}
	for key := range parents {
		parents[key] = root(key)
	}
	return parents, nil
}

func (d *studyRawDay) family(receiver string) string {
	base := studyReceiverBase(receiver)
	if family, ok := d.Aliases[base]; ok {
		return family
	}
	return base
}

func studyCohortA(family string) bool {
	// Equal probability, not equal observed traffic volume. A family's aliases
	// always hash together. Site/decoder independence has not been established.
	sum := studySHA256Bytes([]byte(studyReferenceVersion + "\x00" + family))
	return sum[0]&1 == 0
}

func loadStudyRawDay(in studyInput, aliases map[string]string, warmup int) (_ *studyRawDay, err error) {
	if err := studyCheckFile(in.Archive); err != nil {
		return nil, err
	}
	z, err := zip.OpenReader(in.Archive.Path)
	if err != nil {
		return nil, err
	}
	var total uint64
	for _, f := range z.File {
		if f.UncompressedSize64 > in.MaxCSVBytes-total {
			return nil, errors.Join(errors.New("study ZIP exceeds frozen uncompressed byte bound"), z.Close())
		}
		total += f.UncompressedSize64
	}
	if err := z.Close(); err != nil {
		return nil, err
	}
	day, compact, err := parseUTCDate(in.DateUTC)
	if err != nil {
		return nil, err
	}
	parser, err := openRBNHistoryCSV(in.Archive.Path, compact+".csv")
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, parser.Close()) }()
	d := &studyRawDay{Start: day, End: day.Add(24 * time.Hour), Aliases: aliases,
		InputSHA256: strings.ToLower(in.Archive.SHA256), Index: make(map[string][]int), WarmupSeconds: warmup}
	for ordinal := int64(1); ; ordinal++ {
		row, ok, readErr := parser.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
		if ordinal > int64(in.MaxRows) {
			return nil, errors.New("study input exceeds frozen row bound")
		}
		raw := studyRaw{Row: row, Valid: ok, Ordinal: ordinal}
		if ok {
			if row.Time.Before(day) || !row.Time.Before(d.End) || row.Time.Before(d.Last) {
				return nil, fmt.Errorf("study row %d is out of day or time order", ordinal)
			}
			if math.IsNaN(row.FreqKHz) || math.IsInf(row.FreqKHz, 0) || row.FreqKHz > 1_000_000 ||
				len(row.DXCall) > 64 || len(row.Spotter) > 64 || len(row.Mode) > 8 || len(row.Band) > 16 {
				return nil, fmt.Errorf("study row %d exceeds finite frequency/string bound", ordinal)
			}
			if d.First.IsZero() {
				d.First = row.Time
			}
			d.Last = row.Time
			raw.Family = d.family(row.Spotter)
			raw.A = studyCohortA(raw.Family)
			if studyReferenceRow(row) {
				key := row.Band + "\x00" + row.Mode
				d.Index[key] = append(d.Index[key], len(d.Rows))
			}
		}
		d.Rows = append(d.Rows, raw)
	}
	if len(d.Rows) == 0 || d.First.IsZero() {
		return nil, errors.New("study input has no valid raw observations")
	}
	d.assignEpisodes()
	return d, nil
}

func studyReferenceRow(row rbnHistoryRow) bool {
	return row.SpotClass == rbnfeed.SpotClassCQ && !spot.IsBeaconCall(row.DXCall)
}

func studyFocalRow(raw studyRaw) bool {
	return raw.Valid && raw.A && studyReferenceRow(raw.Row) && spot.IsCallCorrectionCandidate(raw.Row.Mode)
}

// Anchors never move or chain. Three adjacent 100 Hz bins suffice to find an
// active anchor; selection is earliest UTC then original ordinal, independent
// of every model arm. Retired bins are day-owned and bounded by frozen rows.
func (d *studyRawDay) assignEpisodes() {
	type anchor struct {
		at      time.Time
		freq    float64
		ordinal int64
		id      string
	}
	type key struct {
		band, mode string
		bin        int64
	}
	bins := make(map[key][]anchor)
	for i := range d.Rows {
		raw := &d.Rows[i]
		if !studyFocalRow(*raw) {
			continue
		}
		row := raw.Row
		bin := int64(math.Floor(row.FreqKHz * 10))
		var chosen anchor
		for delta := int64(-1); delta <= 1; delta++ {
			k := key{row.Band, row.Mode, bin + delta}
			anchors := bins[k]
			for len(anchors) > 0 && anchors[0].at.Add(studyEpisodeSeconds*time.Second).Before(row.Time) {
				anchors[0] = anchor{}
				anchors = anchors[1:]
			}
			if len(anchors) == 0 {
				delete(bins, k)
			} else {
				bins[k] = anchors
			}
			for _, a := range anchors {
				if math.Abs(a.freq-row.FreqKHz)*1000 <= 100+1e-6 &&
					(chosen.id == "" || a.at.Before(chosen.at) || a.at.Equal(chosen.at) && a.ordinal < chosen.ordinal) {
					chosen = a
				}
			}
		}
		if chosen.id == "" {
			chosen = anchor{at: row.Time, freq: row.FreqKHz, ordinal: raw.Ordinal,
				id: fmt.Sprintf("%s:%d", d.InputSHA256, raw.Ordinal)}
			k := key{row.Band, row.Mode, bin}
			bins[k] = append(bins[k], chosen)
		}
		raw.Episode = chosen.id
	}
}

func (d *studyRawDay) reference(raw studyRaw, later bool) studyReference {
	focal := raw.Row
	lo, hi := focal.Time.Add(-30*time.Second), focal.Time
	if later {
		lo, hi = focal.Time.Add(80*time.Second), focal.Time.Add(studyEpisodeSeconds*time.Second)
	}
	if focal.Time.Before(d.Start.Add(time.Duration(d.WarmupSeconds) * time.Second)) {
		return studyReference{Reason: "unavailable_context"}
	}
	if lo.Before(d.First) || hi.After(d.Last) || lo.Before(d.Start) || !hi.Before(d.End) {
		return studyReference{Reason: "boundary"}
	}
	indices := d.Index[focal.Band+"\x00"+focal.Mode]
	start := sort.Search(len(indices), func(i int) bool { return !d.Rows[indices[i]].Row.Time.Before(lo) })
	core := make(map[string]string)
	guard := make(map[string]map[string]bool)
	for _, index := range indices[start:] {
		other := d.Rows[index]
		if other.Row.Time.After(hi) {
			break
		}
		if later && !other.Row.Time.After(lo) || !later && other.A || other.Family == raw.Family {
			continue
		}
		delta := math.Abs(other.Row.FreqKHz-focal.FreqKHz) * 1000
		if delta > 500+1e-6 {
			continue
		}
		identity := spot.CorrectionVoteKey(other.Row.DXCall)
		if identity == "" {
			return studyReference{Reason: "reference_conflict"}
		}
		if guard[identity] == nil {
			guard[identity] = make(map[string]bool)
		}
		guard[identity][other.Family] = true
		if delta <= 100+1e-6 {
			if previous, ok := core[other.Family]; ok && previous != identity {
				return studyReference{Reason: "receiver_conflict"}
			}
			core[other.Family] = identity
		}
	}
	label := ""
	for _, identity := range core {
		if label != "" && identity != label {
			return studyReference{Reason: "reference_conflict", Families: len(core)}
		}
		label = identity
	}
	for identity, families := range guard {
		if identity != label && len(families) >= 2 {
			return studyReference{Reason: "cochannel", Families: len(core)}
		}
	}
	if len(core) < 3 {
		return studyReference{Reason: "insufficient_families", Families: len(core)}
	}
	return studyReference{Strong: true, Identity: label, Families: len(core), Reason: "strong"}
}
