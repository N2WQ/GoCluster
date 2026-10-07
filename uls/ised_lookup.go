// File role: Resolves Canadian membership and registered province from one ISED
// snapshot. Events are evaluated on inclusive UTC dates; prefix matches prove
// callsign plausibility only, never residency or organizational eligibility.
package uls

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// probeCanadianDatabase verifies both projections and the publication manifest.
// It is a cold handle/startup probe; no schema work runs on cached lookup paths.
func probeCanadianDatabase(ctx context.Context, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version;").Scan(&version); err != nil {
		return err
	}
	if version != CanadianSchemaVersion {
		return fmt.Errorf("ised: unsupported schema %d", version)
	}
	for _, query := range []string{
		"SELECT call_sign,state FROM CA LIMIT 0;",
		"SELECT id,special,start_day,end_day,trustee,use_by,uncertain,prefix_kind FROM Events LIMIT 0;",
	} {
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	mainSHA, specialSHA, err := canadianSourcePair(ctx, db)
	if err != nil {
		return err
	}
	for _, hash := range []string{mainSHA, specialSHA} {
		if len(hash) != 64 {
			return errors.New("ised: invalid source manifest")
		}
		if _, err := hex.DecodeString(hash); err != nil {
			return errors.New("ised: invalid source manifest")
		}
	}
	return nil
}

func canadianSourcePair(ctx context.Context, db *sql.DB) (mainSHA, specialSHA string, err error) {
	err = db.QueryRowContext(ctx, "SELECT main_sha,special_sha FROM SourceMeta WHERE id=1;").Scan(&mainSHA, &specialSHA)
	return mainSHA, specialSHA, err
}

type canadianEvidence struct {
	found, unknown, stateSet, conflict bool
	state                              string
}

func (e *canadianEvidence) add(state string) {
	if !isCanadianProvince(state) {
		state = ""
	}
	e.found = true
	if !e.stateSet {
		e.state = state
		e.stateSet = true
		return
	}
	if e.state != state {
		e.conflict = true
	}
}

// queryCanadianLicense touches a primary-key assignment and at most three
// indexed event keys. The base/trustee joins are primary-key probes, avoiding a
// registry scan and nested queries while a row cursor owns the SQL connection.
func queryCanadianLicense(ctx context.Context, db *sql.DB, canonical string, now time.Time) (LookupResult, error) {
	var evidence canadianEvidence
	var ordinary string
	err := db.QueryRowContext(ctx, "SELECT state FROM CA WHERE call_sign=?;", canonical).Scan(&ordinary)
	if err == nil {
		evidence.add(ordinary)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return LookupResult{}, err
	}
	key2, key3 := "", ""
	if len(canonical) > 2 {
		key2 = canonical[:2]
	}
	if len(canonical) > 3 {
		key3 = canonical[:3]
	}
	base := canadianSubstitutionBase(canonical)
	rows, err := db.QueryContext(ctx, `SELECT e.special,e.start_day,e.end_day,e.use_by,e.uncertain,e.prefix_kind,trustee.state,base.state FROM Events e LEFT JOIN CA trustee ON trustee.call_sign=e.trustee LEFT JOIN CA base ON base.call_sign=? WHERE e.special IN(?,?,?);`, base, canonical, key2, key3)
	if err != nil {
		return LookupResult{}, err
	}
	defer rows.Close()
	day := now.UTC().Format("2006-01-02")
	for rows.Next() {
		var special, start, end, useBy string
		var uncertain, prefixKind int
		var trusteeState, baseState sql.NullString
		if err := rows.Scan(&special, &start, &end, &useBy, &uncertain, &prefixKind, &trusteeState, &baseState); err != nil {
			return LookupResult{}, err
		}
		// Invalid/reversed dates cannot establish inactive membership. Keeping
		// the candidate unknown prevents rejecting legitimate upstream errors.
		if !isedDate(start) || !isedDate(end) || start > end {
			evidence.unknown = true
			continue
		}
		if day < start || day > end {
			continue
		}
		if uncertain != 0 {
			evidence.unknown = true
			continue
		}
		if prefixKind == 0 {
			if special == canonical {
				evidence.add(trusteeState.String)
			}
			continue
		}
		if base == "" {
			evidence.unknown = true
			continue
		}
		if !isedUseByMatches(useBy, base) {
			continue
		}
		if baseState.Valid {
			evidence.add(baseState.String)
		}
	}
	if err := rows.Err(); err != nil {
		return LookupResult{}, err
	}
	if evidence.found {
		state := evidence.state
		if evidence.conflict || evidence.unknown {
			state = ""
		}
		return LookupResult{Available: true, Found: true, State: state}, nil
	}
	if evidence.unknown {
		return LookupResult{}, nil
	}
	return LookupResult{Available: true}, nil
}

// canadianOrdinaryPrefix is the finite RIC-9 Table I assignment mapping. The
// active export still supplies the dates; this table alone never admits a call.
// https://ised-isde.canada.ca/site/spectrum-management-telecommunications/en/licences-and-certificates/radiocom-information-circulars-ric/ric-9-call-sign-policy-and-special-event-prefixes
func canadianOrdinaryPrefix(special string, digit byte) string {
	switch special {
	case "CG", "CK", "VX", "XM", "VC":
		if digit >= '1' && digit <= '9' {
			return "VE"
		}
	case "CF", "CJ", "VG", "XL", "VB":
		if digit >= '1' && digit <= '7' {
			return "VA"
		}
	case "CH", "CY", "XJ", "XN", "VD":
		if digit == '1' || digit == '2' {
			return "VO"
		}
	case "CI", "CZ", "XK", "XO", "VF":
		if digit >= '0' && digit <= '2' {
			return "VY"
		}
	}
	return ""
}

func canadianSubstitutionBase(call string) string {
	if len(call) < 4 {
		return ""
	}
	prefix := canadianOrdinaryPrefix(call[:2], call[2])
	if prefix == "" {
		return ""
	}
	return prefix + call[2:]
}

func validISEDSpecialPrefix(value string) bool {
	if len(value) == 3 {
		return canadianOrdinaryPrefix(value[:2], value[2]) != ""
	}
	if len(value) != 2 {
		return false
	}
	for digit := byte('0'); digit <= '9'; digit++ {
		if canadianOrdinaryPrefix(value, digit) != "" {
			return true
		}
	}
	return false
}

func validISEDUseBy(value string) bool {
	if value == "" {
		return true
	}
	// The delimiter is declared evidence, not prose. More than four items is
	// outside the four ordinary prefix families and remains conservatively unknown.
	parts := strings.Split(value, "/")
	if len(parts) > 4 {
		return false
	}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if len(part) < 2 || isedIdentity(part) == "" {
			return false
		}
		if part[:2] != "VE" && part[:2] != "VA" && part[:2] != "VO" && part[:2] != "VY" {
			return false
		}
		if len(part) > 2 && (part[2] < '0' || part[2] > '9') {
			return false
		}
		if len(part) > 2 && !validISEDOrdinaryDesignator(part[:2], part[2]) {
			return false
		}
	}
	return true
}

func validISEDOrdinaryDesignator(prefix string, digit byte) bool {
	switch prefix {
	case "VE":
		return digit == '0' || canadianOrdinaryPrefix("CG", digit) != ""
	case "VA":
		return canadianOrdinaryPrefix("CF", digit) != ""
	case "VO":
		return canadianOrdinaryPrefix("CH", digit) != ""
	case "VY":
		return canadianOrdinaryPrefix("CI", digit) != ""
	}
	return false
}

func isedUseByMatches(useBy, base string) bool {
	if useBy == "" {
		return true
	}
	for _, part := range strings.Split(useBy, "/") {
		part = strings.TrimSpace(part)
		if len(part) <= 3 && strings.HasPrefix(base, part) || part == base {
			return true
		}
	}
	return false
}
