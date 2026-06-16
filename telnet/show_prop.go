// File role: Owns the telnet SHOW PROP command parser, target resolution, and
// response formatter for on-demand VOACAP propagation outlooks.
// Crawler notes: Start here for CW defaulting, per-band mode normalization,
// command-triggered fallback refreshes, and EFF/RX/TX glyph-table output.
// Related docs: telnet/README.md, docs/OPERATOR_GUIDE.md,
// pathreliability/README.md, docs/decisions/ADR-0172-show-prop-worker-refresh-and-glyph-columns.md.
// Related tests: telnet/show_prop_test.go.
package telnet

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"dxcluster/cty"
	"dxcluster/pathreliability"
	"dxcluster/spot"
	"dxcluster/strutil"
)

const showPropUsage = "Usage: SHOW PROP <call|prefix|grid> [band] [mode]\n"
const showPropNoOpenRowsMessage = "No HIGH/MEDIUM/LOW rows in current forecast window."

type showPropCommand struct {
	target string
	band   string
	mode   string
}

type showPropTarget struct {
	input  string
	grid   string
	source string
	cell   pathreliability.CellID
}

type showPropBandRows struct {
	band    string
	mode    string
	records []pathreliability.VOACAPCachedForecast
	status  pathreliability.VOACAPForecastWindowStatus
}

func (s *Server) handleShowPropCommand(client *Client, line string) (string, bool) {
	cmd, handled, err := parseShowPropCommand(line)
	if !handled {
		return "", false
	}
	if err != "" {
		return err, true
	}
	if s == nil || s.pathPredictor == nil || !s.pathPredictor.Config().Enabled {
		return "Path reliability is disabled.\n", true
	}
	if s.pathClosedFallback == nil {
		return "VOACAP fallback is disabled.\n", true
	}
	provider, providerOK := s.pathClosedFallback.(pathreliability.VOACAPForecastWindowProvider)
	waitProvider, waitProviderOK := s.pathClosedFallback.(pathreliability.VOACAPForecastWindowWaitProvider)
	if !providerOK && !waitProviderOK {
		return "VOACAP outlook is unavailable.\n", true
	}

	cfg := s.pathPredictor.Config()
	state := client.pathSnapshot()
	userGrid := strutil.NormalizeUpper(state.grid)
	userCell := state.gridCell
	if userCell == pathreliability.InvalidCell && userGrid != "" {
		userCell = pathreliability.EncodeCell(userGrid)
	}
	if userGrid == "" || userCell == pathreliability.InvalidCell {
		return "Grid not set. Use SET GRID <4-6 char maidenhead>.\n", true
	}

	target, ok := s.resolveShowPropTarget(cmd.target)
	if !ok {
		return fmt.Sprintf("Could not resolve target grid for %s.\n", strings.TrimSpace(cmd.target)), true
	}
	bands, errMsg := showPropBands(cmd.band, cfg)
	if errMsg != "" {
		return errMsg, true
	}
	modeRaw := strutil.NormalizeUpper(cmd.mode)
	if modeRaw == "" {
		modeRaw = "CW"
	}
	now := s.now()
	noiseClass := strutil.NormalizeUpper(state.noiseClass)
	if noiseClass == "" {
		noiseClass = "QUIET"
	}
	noisePenalty := s.noisePenaltyForClass(noiseClass)
	hours := cfg.VOACAPFallback.ForecastHours
	rows := make([]showPropBandRows, 0, len(bands))
	singleBand := strings.TrimSpace(cmd.band) != ""
	for _, band := range bands {
		mode, ok := normalizeShowPropModeForBand(modeRaw, band, cfg)
		if !ok {
			return fmt.Sprintf("Unsupported mode for SHOW PROP: %s\nSupported: %s\n", modeRaw, showPropSupportedModes(cfg)), true
		}
		req := pathreliability.VOACAPClosedRequest{
			UserCell:              userCell,
			DXCell:                target.cell,
			UserGrid:              userGrid,
			DXGrid:                target.grid,
			Band:                  band,
			Mode:                  mode,
			ReceiveNoisePenaltyDB: noisePenalty,
		}
		if waitProviderOK {
			wait := time.Duration(0)
			if singleBand {
				wait = time.Duration(cfg.VOACAPFallback.ShowPropWaitMilliseconds) * time.Millisecond
			}
			window, status := waitProvider.CheckForecastWindowWait(req, now, wait)
			rows = append(rows, showPropBandRows{band: band, mode: mode, records: window.Records, status: status})
			continue
		}
		window, ok := provider.CheckForecastWindow(req, now)
		status := pathreliability.VOACAPForecastWindowReady
		if !ok {
			status = pathreliability.VOACAPForecastWindowRefreshing
		}
		rows = append(rows, showPropBandRows{band: band, mode: mode, records: window.Records, status: status})
	}
	return formatShowPropResponse(userGrid, target, cmd, modeRaw, noiseClass, hours, rows, cfg), true
}

func parseShowPropCommand(line string) (showPropCommand, bool, string) {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return showPropCommand{}, false, ""
	}
	first := strutil.NormalizeUpper(fields[0])
	var args []string
	switch first {
	case "SHOW/PROP", "SH/PROP":
		args = fields[1:]
	case "SHOW", "SH":
		if len(fields) < 2 || strutil.NormalizeUpper(fields[1]) != "PROP" {
			return showPropCommand{}, false, ""
		}
		args = fields[2:]
	default:
		return showPropCommand{}, false, ""
	}
	if len(args) < 1 {
		return showPropCommand{}, true, showPropUsage
	}
	if len(args) > 3 {
		return showPropCommand{}, true, showPropUsage
	}
	cmd := showPropCommand{target: args[0]}
	for _, arg := range args[1:] {
		if spot.IsValidBand(arg) {
			if cmd.band != "" {
				return showPropCommand{}, true, showPropUsage
			}
			cmd.band = spot.NormalizeBand(arg)
			continue
		}
		if cmd.mode != "" {
			return showPropCommand{}, true, showPropUsage
		}
		cmd.mode = arg
	}
	return cmd, true, ""
}

func (s *Server) resolveShowPropTarget(raw string) (showPropTarget, bool) {
	input := strings.TrimSpace(raw)
	if input == "" {
		return showPropTarget{}, false
	}
	grid := strutil.NormalizeUpper(input)
	if _, _, ok := pathreliability.GridCenterLatLon(grid); ok {
		if target, ok := showPropTargetFromGrid(input, grid, "grid"); ok {
			return target, true
		}
	}
	lookup := spot.NormalizeCallsign(input)
	if lookup == "" {
		return showPropTarget{}, false
	}
	if s != nil && s.gridLookup != nil {
		if grid, derived, ok := s.gridLookup(lookup); ok {
			source := "gridstore"
			if derived {
				source = "gridstore-derived"
			}
			if target, ok := showPropTargetFromGrid(input, grid, source); ok {
				return target, true
			}
		}
	}
	if s == nil || s.ctyLookup == nil {
		return showPropTarget{}, false
	}
	db := s.ctyLookup()
	if db == nil {
		return showPropTarget{}, false
	}
	info, ok := db.LookupCallsignPortable(lookup)
	if !ok {
		return showPropTarget{}, false
	}
	grid, ok = cty.Grid4FromLatLon(info.Latitude, info.Longitude)
	if !ok {
		return showPropTarget{}, false
	}
	return showPropTargetFromGrid(input, grid, "cty-derived")
}

func showPropTargetFromGrid(input string, grid string, source string) (showPropTarget, bool) {
	grid = strutil.NormalizeUpper(grid)
	cell := pathreliability.EncodeCell(grid)
	if cell == pathreliability.InvalidCell || pathreliability.EncodeCoarseCell(grid) == pathreliability.InvalidCell {
		return showPropTarget{}, false
	}
	return showPropTarget{
		input:  strings.TrimSpace(input),
		grid:   grid,
		source: source,
		cell:   cell,
	}, true
}

func showPropBands(raw string, cfg pathreliability.Config) ([]string, string) {
	if strings.TrimSpace(raw) != "" {
		band := spot.NormalizeBand(raw)
		if _, ok := pathreliability.VOACAPFallbackCenterFrequencyMHz(cfg.VOACAPFallback, band); !ok {
			return nil, fmt.Sprintf("Unsupported VOACAP band: %s\nSupported: %s\n", band, strings.Join(pathreliability.VOACAPFallbackBands(cfg.VOACAPFallback), ", "))
		}
		return []string{band}, ""
	}
	bands := pathreliability.VOACAPFallbackBands(cfg.VOACAPFallback)
	if len(bands) == 0 {
		return nil, "No VOACAP fallback bands are configured.\n"
	}
	return bands, ""
}

func normalizeShowPropModeForBand(mode string, band string, cfg pathreliability.Config) (string, bool) {
	mode = spot.CanonicalMode(mode)
	if mode == "SSB" {
		if freq, ok := pathreliability.VOACAPFallbackCenterFrequencyMHz(cfg.VOACAPFallback, band); ok {
			mode = spot.NormalizeVoiceMode(mode, freq*1000)
		}
	}
	if mode == "" {
		return "", false
	}
	_, ok := cfg.ModeThresholds[strings.ToUpper(mode)]
	return strings.ToUpper(mode), ok
}

func showPropSupportedModes(cfg pathreliability.Config) string {
	modes := make([]string, 0, len(cfg.ModeThresholds)+1)
	for mode := range cfg.ModeThresholds {
		modes = append(modes, strings.ToUpper(mode))
	}
	sort.Strings(modes)
	return strings.Join(modes, ", ")
}

func formatShowPropResponse(userGrid string, target showPropTarget, cmd showPropCommand, mode string, noiseClass string, hours int, rows []showPropBandRows, cfg pathreliability.Config) string {
	var b strings.Builder
	b.WriteString("PROP ")
	b.WriteString(userGrid)
	b.WriteString(" -> ")
	b.WriteString(target.grid)
	b.WriteString(" target=")
	b.WriteString(target.input)
	b.WriteString(" source=")
	b.WriteString(target.source)
	b.WriteString(" mode=")
	b.WriteString(mode)
	if strings.TrimSpace(cmd.band) != "" {
		b.WriteString(" band=")
		b.WriteString(spot.NormalizeBand(cmd.band))
	}
	b.WriteString(" noise=")
	b.WriteString(noiseClass)
	if ssn, ok := showPropFirstSSN(rows); ok {
		b.WriteString(" ssn=")
		b.WriteString(strconv.Itoa(ssn))
	}
	if hours > 0 {
		b.WriteString(" hours=")
		b.WriteString(strconv.Itoa(hours))
	}
	b.WriteByte('\n')
	singleBand := strings.TrimSpace(cmd.band) != ""
	anyRecords := false
	anyOpenRecords := false
	for _, row := range rows {
		if len(row.records) > 0 {
			anyRecords = true
		}
		for i := range row.records {
			if showPropOpenReliability(showPropReliability(row.records[i], row.mode, cfg)) {
				anyOpenRecords = true
				break
			}
		}
		if anyRecords && anyOpenRecords {
			break
		}
	}
	if !anyRecords && singleBand {
		if len(rows) > 0 {
			b.WriteString(showPropStatusMessage(rows[0].status, false))
		} else {
			b.WriteString("Still computing; ask again shortly.")
		}
		b.WriteByte('\n')
		return b.String()
	}
	if anyRecords && !anyOpenRecords && singleBand {
		b.WriteString(showPropNoOpenRowsMessage)
		b.WriteByte('\n')
		if len(rows) > 0 && rows[0].status != pathreliability.VOACAPForecastWindowReady {
			b.WriteString(showPropStatusMessage(rows[0].status, true))
			b.WriteByte('\n')
		}
		return b.String()
	}
	if singleBand {
		b.WriteString("UTC  EFF  RX  TX  REL\n")
	} else {
		b.WriteString("BAND  UTC  EFF  RX  TX  REL\n")
	}
	for _, row := range rows {
		if len(row.records) == 0 {
			message := showPropStatusMessage(row.status, false)
			if singleBand {
				b.WriteString(message)
				b.WriteByte('\n')
			} else {
				fmt.Fprintf(&b, "%-5s %s\n", row.band, message)
			}
			continue
		}
		openRecords := 0
		for i := range row.records {
			forecast := row.records[i]
			rel := showPropReliability(forecast, row.mode, cfg)
			if !showPropOpenReliability(rel) {
				continue
			}
			openRecords++
			eff := showPropGlyph(forecast.EffectiveDB(), row.mode, cfg)
			rx := showPropGlyph(forecast.ReceiveDB(), row.mode, cfg)
			tx := showPropGlyph(forecast.TransmitDB(), row.mode, cfg)
			if singleBand {
				fmt.Fprintf(&b, "%02dZ  %-3s  %-2s  %-2s  %s\n", forecast.Record.HourUTC, eff, rx, tx, rel)
			} else {
				fmt.Fprintf(&b, "%-5s %02dZ  %-3s  %-2s  %-2s  %s\n", row.band, forecast.Record.HourUTC, eff, rx, tx, rel)
			}
		}
		if openRecords == 0 {
			if singleBand {
				b.WriteString(showPropNoOpenRowsMessage)
				b.WriteByte('\n')
			} else {
				fmt.Fprintf(&b, "%-5s %s\n", row.band, showPropNoOpenRowsMessage)
			}
		}
		if row.status != pathreliability.VOACAPForecastWindowReady {
			message := showPropStatusMessage(row.status, true)
			if singleBand {
				b.WriteString(message)
				b.WriteByte('\n')
			} else {
				fmt.Fprintf(&b, "%-5s %s\n", row.band, message)
			}
		}
	}
	return b.String()
}

func showPropOpenReliability(rel string) bool {
	switch rel {
	case "HIGH", "MEDIUM", "LOW":
		return true
	default:
		return false
	}
}

func showPropGlyph(db float64, mode string, cfg pathreliability.Config) string {
	rounded := float64(int(math.Round(db)))
	if pathreliability.ClosedForDB(rounded, mode, cfg) {
		return cfg.GlyphSymbols.Closed
	}
	return pathreliability.GlyphForDB(rounded, mode, cfg)
}

func showPropReliability(forecast pathreliability.VOACAPCachedForecast, mode string, cfg pathreliability.Config) string {
	effective := float64(int(math.Round(forecast.EffectiveDB())))
	if pathreliability.ClosedForDB(effective, mode, cfg) {
		return "CLOSED"
	}
	return pathreliability.ClassForDB(effective, mode, cfg)
}

func showPropStatusMessage(status pathreliability.VOACAPForecastWindowStatus, hasRecords bool) string {
	switch status {
	case pathreliability.VOACAPForecastWindowReady:
		if !hasRecords {
			return "Still computing; ask again shortly."
		}
		return ""
	case pathreliability.VOACAPForecastWindowBusy:
		return "VOACAP busy; ask again shortly."
	case pathreliability.VOACAPForecastWindowFailed:
		if hasRecords {
			return "Refresh failed; showing cached rows."
		}
		return "VOACAP outlook failed; ask again shortly."
	case pathreliability.VOACAPForecastWindowUnavailable:
		return "VOACAP outlook unavailable."
	default:
		if hasRecords {
			return "Refreshing; ask again shortly for full horizon."
		}
		return "Still computing; ask again shortly."
	}
}

func showPropFirstSSN(rows []showPropBandRows) (int, bool) {
	for _, row := range rows {
		for i := range row.records {
			forecast := row.records[i]
			if forecast.SSN > 0 {
				return forecast.SSN, true
			}
		}
	}
	return 0, false
}
