package telnet

import (
	"math/bits"
	"strings"
	"sync/atomic"
	"unsafe"

	"dxcluster/archive"
	"dxcluster/spot"
)

// One archive field has at most 65,535 source bytes. Only DX call can grow
// through Unicode normalization (at most four bytes per input byte). DE call,
// band, mode, source and comment each take at most one original field.
// CW labels occupy at most 1,024 text bytes and 170 string headers. Country
// and node have their own published limits. The remaining station strings are
// finite protocol vocabularies or grid locators. Even with an 8 KiB allocation
// rounding allowance per row, the complete 250-row page fits in 160 MiB.
const jsonHistoryBudgetBytes = 160 << 20

var processJSONHistoryBytes atomic.Int64

type jsonHistoryStation struct {
	call, country, continent, state, grid, grid2 string
	adif, cq, itu                                int
	region                                       spot.IARURegion
	derived                                      bool
}

// Only projected public fields survive selection. The linked rows reverse the
// newest-first scan into text display order without another page-sized buffer.
type jsonHistoryRow struct {
	next                                            *jsonHistoryRow
	dx, de                                          jsonHistoryStation
	band, mode, source, node, comment               string
	speeds                                          []string
	events                                          spot.EventMask
	frequency, observed                             float64
	timeNS                                          int64
	report                                          int
	confidence                                      byte
	human, beacon, toxic, hasReport                 bool
	omittedNode, omittedDXCountry, omittedDECountry bool
}

type jsonHistoryRows struct {
	first   *jsonHistoryRow
	count   int
	charged int64
}

// Pointer-bearing row and label-vector allocations use Go size classes that
// may include a malloc header. Round up to the next power of two including
// that header. Both objects are small (at most 170 labels). The byte builder
// already exposes its allocator-rounded capacity directly.
func jsonHistorySmallAllocation(n uintptr) int64 {
	return 1 << bits.Len(uint(n+unsafe.Sizeof(uintptr(0))-1))
}

func jsonHistoryString(value string, limit int) (string, bool) {
	if len(value) > limit {
		return "", true
	}
	return value, false
}

func jsonProjectStation(displayCall string, meta spot.CallMetadata, normalizedGrid, normalizedContinent string, countryLimit int) (jsonHistoryStation, bool) {
	country, omitted := jsonHistoryString(meta.Country, countryLimit)
	row := jsonHistoryStation{
		call: displayCall, adif: meta.ADIF, country: country, cq: meta.CQZone,
		itu: meta.ITUZone, derived: meta.GridDerived,
	}
	if normalizedContinent == "" {
		normalizedContinent = meta.Continent
	}
	switch normalizedContinent {
	case "NA", "SA", "EU", "AF", "AS", "OC", "AN":
		row.continent = normalizedContinent
	}
	row.region = meta.IARURegion
	if spot.IsState(meta.State) {
		row.state = meta.State
	}
	// v1 exposes a six-character prefix even when a source or old archive
	// retains a higher-precision locator. Slice before uppercase conversion
	// so a long malformed source never creates an input-sized writer scratch.
	if normalizedGrid == "" {
		normalizedGrid = strings.TrimSpace(meta.Grid)
	}
	grid := strings.ToUpper(normalizedGrid[:min(len(normalizedGrid), 6)])
	if validExactGrid(grid) {
		row.grid, row.grid2 = grid, grid[:2]
	} else if len(grid) == 2 && grid[0] >= 'A' && grid[0] <= 'R' && grid[1] >= 'A' && grid[1] <= 'R' {
		row.grid2 = grid
	}
	return row, omitted
}

func jsonProjectSpot(s *Server, sp *spot.Spot, showSpeeds, historical bool) jsonHistoryRow {
	de := sp.DECall
	if sp.DECallStripped != "" {
		de = sp.DECallStripped
	}
	row := jsonHistoryRow{
		band: sp.Band, mode: sp.Mode, source: string(sp.SourceType), comment: sp.Comment,
		events: sp.Events, frequency: sp.Frequency, observed: sp.ObservedFrequency,
		timeNS: sp.Time.UnixNano(), report: sp.Report, hasReport: sp.HasReport,
		human: sp.IsHuman, beacon: sp.IsBeacon, toxic: sp.ToxicityStatus == spot.ToxicityToxic,
	}
	if sp.CWDisplayAvailable {
		if !historical || sp.CWDisplayComment != "" || !sp.IsBeacon ||
			(sp.Comment != spot.BeaconCommentDefault && sp.Comment != spot.BeaconCommentNCDXF) {
			row.comment = sp.CWDisplayComment
		}
		if showSpeeds {
			row.speeds = sp.CWSpeeds
		}
	}
	if conf := strings.TrimSpace(sp.Confidence); conf != "" {
		row.confidence = firstPrintableASCIIOrQuestion(conf)
	}
	row.node, row.omittedNode = jsonHistoryString(sp.SourceNode, jsonNodeLimit)
	dxMetadata, deMetadata := sp.DXMetadata, sp.DEMetadata
	if historical {
		context := s.ve7ccContext(true)
		dxMetadata = ve7ccSupplement(dxMetadata, sp.DXCall, context.cty, &ve7ccFormatNotes{})
		deMetadata = ve7ccSupplement(deMetadata, sp.DECall, context.cty, &ve7ccFormatNotes{})
		dxMetadata.IARURegion = spot.ResolveIARURegion(dxMetadata.ADIF, sp.DXContinentNorm)
		deMetadata.IARURegion = spot.ResolveIARURegion(deMetadata.ADIF, sp.DEContinentNorm)
	}
	row.dx, row.omittedDXCountry = jsonProjectStation(sp.DXCall, dxMetadata, sp.DXGridNorm, sp.DXContinentNorm, jsonDXCountryLimit)
	row.de, row.omittedDECountry = jsonProjectStation(de, deMetadata, sp.DEGridNorm, sp.DEContinentNorm, jsonDECountryLimit)
	return row
}

func (p *jsonHistoryRows) collect(s *Server, sp *spot.Spot, showSpeeds bool) error {
	if !validJSONFrequencies(sp.Frequency, sp.ObservedFrequency) {
		return errJSONInvalidSpot
	}
	row := jsonProjectSpot(s, sp, showSpeeds, true)
	return p.retain(&row)
}

func (p *jsonHistoryRows) retain(row *jsonHistoryRow) error {
	// A slice of pointers to fields, including short substrings of larger
	// decoded fields, makes copying and sizing use exactly the same inputs.
	fields := [...]*string{&row.band, &row.mode, &row.source, &row.node, &row.comment,
		&row.dx.call, &row.dx.country, &row.dx.continent, &row.dx.state, &row.dx.grid,
		&row.dx.grid2, &row.de.call, &row.de.country, &row.de.continent, &row.de.state,
		&row.de.grid, &row.de.grid2}
	var size int
	for _, field := range fields {
		size += len(*field)
	}
	for _, label := range row.speeds {
		size += len(label)
	}
	var backing strings.Builder
	// One candidate's allocations belong to the bounded scan slot until the
	// CAS transfers them. Grow exposes the allocated byte capacity, including
	// Go's size-class slack. No append or substring can retain a decoder buffer.
	backing.Grow(size)
	charge := int64(backing.Cap()) + jsonHistorySmallAllocation(unsafe.Sizeof(*row))
	if len(row.speeds) != 0 {
		charge += jsonHistorySmallAllocation(uintptr(len(row.speeds)) * unsafe.Sizeof(""))
	}
	if p.count == 0 {
		// Include the queued delivery (which embeds *p's headers) and pending
		// cursor. The remaining 512 bytes cover the empty completion channel,
		// copied 14-byte position, opaque token and allocator granularity.
		charge += jsonHistorySmallAllocation(unsafe.Sizeof(jsonHistoryDelivery{})) +
			jsonHistorySmallAllocation(unsafe.Sizeof(historySearch{})) + 512
	}
	for {
		used := processJSONHistoryBytes.Load()
		if charge > jsonHistoryBudgetBytes-used {
			return archive.ErrHistoryBusy
		}
		if processJSONHistoryBytes.CompareAndSwap(used, used+charge) {
			break
		}
	}
	own := func(v string) string {
		if v == "" {
			return ""
		}
		start := backing.Len()
		backing.WriteString(v)
		return backing.String()[start:]
	}
	for _, field := range fields {
		*field = own(*field)
	}
	if len(row.speeds) != 0 {
		speeds := make([]string, len(row.speeds))
		for i, label := range row.speeds {
			speeds[i] = own(label)
		}
		row.speeds = speeds
	} else {
		row.speeds = nil
	}
	row.next, p.first = p.first, row
	p.count++
	p.charged += charge
	return nil
}

// The selector or the writer owns the page exclusively. Discard every row
// before allowing another scan to acquire the released capacity.
func (p *jsonHistoryRows) release() {
	charge := p.charged
	*p = jsonHistoryRows{}
	processJSONHistoryBytes.Add(-charge)
}
