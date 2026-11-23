// File role: Encode the JSON v1 public spot projection with one reusable
// writer buffer. Each accepted row is completed before another record starts.
package telnet

import (
	"errors"
	"io"
	"math"
	"strconv"
	"sync/atomic"

	"dxcluster/filter"
	"dxcluster/spot"
)

const (
	jsonSpotSchemaLine    = `["schema","s",1,["type","id","time","freq","obsFreq","band","mode","modeProv","report","comment","cwSpeeds","events","source","node","human","beacon","conf","confPct","dx","de","toxic","path","glyph"]]` + "\n"
	jsonStationSchemaLine = `["schema","station",1,["call","adif","country","cont","cq","itu","state","grid","gridDerived","grid2","iaru"]]` + "\n"
)

var errJSONInvalidSpot = errors.New("invalid JSON spot frequency")

func validJSONFrequencies(frequency, observed float64) bool {
	return frequency > 0 && observed >= 0 && !math.IsInf(frequency, 0) && !math.IsInf(observed, 0)
}

type jsonSpotCounters struct {
	node, dxCountry, deCountry, invalidUTF8 atomic.Uint64
}

// JSONSpotStats counts optional spot fields omitted for size and invalid
// source bytes replaced in JSON output on this server.
type JSONSpotStats struct {
	NodeOmissions, DXCountryOmissions, DECountryOmissions, InvalidUTF8Bytes uint64
}

func (s *Server) JSONSpotStats() JSONSpotStats {
	return JSONSpotStats{
		NodeOmissions: s.jsonSpotStats.node.Load(), DXCountryOmissions: s.jsonSpotStats.dxCountry.Load(),
		DECountryOmissions: s.jsonSpotStats.deCountry.Load(), InvalidUTF8Bytes: s.jsonSpotStats.invalidUTF8.Load(),
	}
}

// The normal path sends one append to the connection's batch. A large string
// borrows the same space to quote bounded pieces directly into that batch.
type jsonSpotEncoder struct {
	buf     jsonQuoteScratch
	w       io.Writer
	used    int
	invalid uint64
	err     error
}

func (e *jsonSpotEncoder) flush() {
	if e.used != 0 && e.err == nil {
		var n int
		n, e.err = e.w.Write(e.buf[:e.used])
		if n != e.used && e.err == nil {
			e.err = io.ErrShortWrite
		}
	}
	e.used = 0
}

func (e *jsonSpotEncoder) room(needed int) []byte {
	if needed > len(e.buf)-e.used {
		e.flush()
	}
	return e.buf[:e.used]
}

// literal is used only for small static grammar tokens, never an input string.
func (e *jsonSpotEncoder) literal(token string) {
	e.used = len(append(e.room(len(token)), token...))
}

func (e *jsonSpotEncoder) text(value string) {
	if e.err != nil {
		return
	}
	if len(value) > jsonQuoteChunkBytes {
		e.flush()
		if e.err == nil {
			var count int
			count, e.err = writeJSONString(e.w, &e.buf, value)
			e.invalid += uint64(count)
		}
		return
	}
	result, count := appendJSONString(e.room(6*len(value)+2), value)
	e.used = len(result)
	e.invalid += uint64(count)
}

func (e *jsonSpotEncoder) nullableText(value string) {
	if value == "" {
		e.literal("null")
		return
	}
	e.text(value)
}

func (e *jsonSpotEncoder) integer(value int64, present bool) {
	if !present {
		e.literal("null")
		return
	}
	e.used = len(strconv.AppendInt(e.room(20), value, 10))
}

func (e *jsonSpotEncoder) boolean(value bool) {
	e.used = len(strconv.AppendBool(e.room(5), value))
}

func (e *jsonSpotEncoder) station(v *jsonHistoryStation) {
	e.literal("[")
	e.text(v.call)
	e.literal(",")
	e.integer(int64(v.adif), v.adif > 0)
	e.literal(",")
	e.nullableText(v.country)
	e.literal(",")
	e.nullableText(v.continent)
	e.literal(",")
	e.integer(int64(v.cq), v.cq > 0)
	e.literal(",")
	e.integer(int64(v.itu), v.itu > 0)
	e.literal(",")
	e.nullableText(v.state)
	e.literal(",")
	e.nullableText(v.grid)
	e.literal(",")
	e.boolean(v.derived)
	e.literal(",")
	e.nullableText(v.grid2)
	switch v.region {
	case spot.IARURegion1:
		e.literal(",1]")
	case spot.IARURegion2:
		e.literal(",2]")
	case spot.IARURegion3:
		e.literal(",3]")
	default:
		e.literal(",null]")
	}
}

func jsonRecipientPath(c *Client, env *spotEnvelope) (string, string) {
	s := c.server
	if s == nil || !s.pathDisplay || s.pathPredictor == nil {
		return "", ""
	}
	prediction, reused := s.reusablePathPredictionForClient(c, env.spot, env.pathPrediction)
	if reused {
		s.recordDisplayedPathPrediction(prediction)
	} else {
		var ok bool
		prediction, ok = s.pathPredictionForClient(c, env.spot)
		if !ok {
			return filter.PathClassInsufficient, ""
		}
	}
	glyph := s.pathGlyphFromPrediction(prediction)
	if glyph != "" {
		if firstPrintableASCIIOrQuestion(glyph) == '?' {
			glyph = "?"
		} else {
			glyph = glyph[:1]
		}
	}
	return pathClassFromPrediction(prediction), glyph
}

func (e *jsonSpotEncoder) writeLive(w io.Writer, c *Client, env *spotEnvelope) error {
	if env == nil || env.spot == nil || !validJSONFrequencies(env.spot.Frequency, env.spot.ObservedFrequency) {
		return errJSONInvalidSpot
	}
	row := jsonProjectSpot(c.server, env.spot, c.cwSpeedEnabled(), false)
	path, glyph := jsonRecipientPath(c, env)
	return e.write(w, c, &row, env.spot, path, glyph)
}

func (e *jsonSpotEncoder) writeHistory(w io.Writer, c *Client, row *jsonHistoryRow) error {
	if row == nil || !validJSONFrequencies(row.frequency, row.observed) {
		return errJSONInvalidSpot
	}
	return e.write(w, c, row, nil, "", "")
}

// Only live records carry a process-local ID, provenance, confidence percent
// and recipient path. All other positions share the same implementation.
func (e *jsonSpotEncoder) write(w io.Writer, c *Client, row *jsonHistoryRow, live *spot.Spot, path, glyph string) error {
	e.w, e.err, e.used, e.invalid = w, nil, 0, 0
	if live == nil {
		e.literal(`["h",null,`)
	} else {
		e.literal(`["s",`)
		e.integer(int64(live.ID), live.ID > 0 && live.ID <= 1<<53-1)
		e.literal(",")
	}
	if live != nil && live.Time.IsZero() {
		e.literal("null")
	} else {
		e.literal(`"`)
		e.integer(row.timeNS, true)
		e.literal(`"`)
	}
	e.literal(",")
	e.used = len(strconv.AppendFloat(e.room(384), row.frequency, 'f', 2, 64))
	e.literal(",")
	if row.observed == 0 || row.observed == row.frequency {
		e.literal("null")
	} else {
		e.used = len(strconv.AppendFloat(e.room(32), row.observed, 'g', -1, 64))
	}
	e.literal(",")
	e.text(row.band)
	e.literal(",")
	e.text(row.mode)
	e.literal(",")
	if live != nil {
		e.nullableText(string(live.ModeProvenance))
	} else {
		e.literal("null")
	}
	e.literal(",")
	e.integer(int64(row.report), row.hasReport)
	e.literal(",")
	e.text(row.comment)
	e.literal(",[")
	for i, speed := range row.speeds {
		if i > 0 {
			e.literal(",")
		}
		e.text(speed)
	}
	e.literal("],[")
	if row.events != 0 {
		first := true
		spot.CurrentTaxonomy().VisitEventNames(row.events, func(name string) {
			if !first {
				e.literal(",")
			}
			first = false
			e.text(name)
		})
	}
	e.literal("],")
	e.text(row.source)
	e.literal(",")
	e.nullableText(row.node)
	e.literal(",")
	e.boolean(row.human)
	e.literal(",")
	e.boolean(row.beacon)
	e.literal(",")
	if row.confidence == 0 {
		e.literal("null")
	} else {
		e.text(string(row.confidence))
	}
	e.literal(",")
	if live != nil {
		e.integer(int64(live.ConfidencePercent), live.ConfidencePercentOK)
	} else {
		e.literal("null")
	}
	e.literal(",")
	e.station(&row.dx)
	e.literal(",")
	e.station(&row.de)
	e.literal(",")
	e.boolean(row.toxic)
	e.literal(",")
	e.nullableText(path)
	e.literal(",")
	e.nullableText(glyph)
	e.literal("]\n")
	e.flush()
	e.w = nil
	if e.err == nil && c.server != nil {
		stats := &c.server.jsonSpotStats
		if row.omittedDXCountry {
			stats.dxCountry.Add(1)
		}
		if row.omittedDECountry {
			stats.deCountry.Add(1)
		}
		if row.omittedNode {
			stats.node.Add(1)
		}
		if e.invalid != 0 {
			stats.invalidUTF8.Add(e.invalid)
		}
	}
	return e.err
}
