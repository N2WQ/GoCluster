package telnet

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"dxcluster/cty"
	"dxcluster/pathreliability"
	"dxcluster/spot"
	"dxcluster/strutil"
)

const showPropUsage = "Usage: SHOW PROP <call|prefix|grid> [band] [mode]\n"

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
	band      string
	mode      string
	records   []pathreliability.VOACAPCachedForecast
	computing bool
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
	provider, ok := s.pathClosedFallback.(pathreliability.VOACAPForecastWindowProvider)
	if !ok {
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
		modeRaw = "FT8"
	}
	now := s.now()
	noiseClass := strutil.NormalizeUpper(state.noiseClass)
	if noiseClass == "" {
		noiseClass = "QUIET"
	}
	noisePenalty := s.noisePenaltyForClass(noiseClass)
	hours := cfg.VOACAPFallback.ForecastHours
	rows := make([]showPropBandRows, 0, len(bands))
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
		window, ok := provider.CheckForecastWindow(req, now)
		if !ok {
			rows = append(rows, showPropBandRows{band: band, mode: mode, computing: true})
			continue
		}
		rows = append(rows, showPropBandRows{band: band, mode: mode, records: window.Records})
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
	for _, row := range rows {
		if len(row.records) > 0 {
			anyRecords = true
			break
		}
	}
	if !anyRecords && singleBand {
		b.WriteString("Computing, ask again shortly.\n")
		return b.String()
	}
	if singleBand {
		b.WriteString("UTC  EFF  RX   TX   REL\n")
	} else {
		b.WriteString("BAND  UTC  EFF  RX   TX   REL\n")
	}
	for _, row := range rows {
		if row.computing {
			if singleBand {
				b.WriteString("Computing, ask again shortly.\n")
			} else {
				fmt.Fprintf(&b, "%-5s computing, ask again shortly\n", row.band)
			}
			continue
		}
		for _, forecast := range row.records {
			rel := showPropReliability(forecast, row.mode, cfg)
			eff := int(math.Round(forecast.EffectiveDB()))
			rx := int(math.Round(forecast.ReceiveDB()))
			tx := int(math.Round(forecast.TransmitDB()))
			if singleBand {
				fmt.Fprintf(&b, "%02dZ  %3d  %3d  %3d  %s\n", forecast.Record.HourUTC, eff, rx, tx, rel)
			} else {
				fmt.Fprintf(&b, "%-5s %02dZ  %3d  %3d  %3d  %s\n", row.band, forecast.Record.HourUTC, eff, rx, tx, rel)
			}
		}
	}
	return b.String()
}

func showPropReliability(forecast pathreliability.VOACAPCachedForecast, mode string, cfg pathreliability.Config) string {
	effective := float64(int(math.Round(forecast.EffectiveDB())))
	if pathreliability.ClosedForDB(effective, mode, cfg) {
		return "CLOSED"
	}
	return pathreliability.ClassForDB(effective, mode, cfg)
}

func showPropFirstSSN(rows []showPropBandRows) (int, bool) {
	for _, row := range rows {
		for _, forecast := range row.records {
			if forecast.SSN > 0 {
				return forecast.SSN, true
			}
		}
	}
	return 0, false
}
