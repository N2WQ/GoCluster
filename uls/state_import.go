// File role: Joins FCC licensee mailing-state evidence onto active amateur
// licenses. Scratch candidates belong only to this database build, not runtime.
package uls

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"dxcluster/spot"
)

// importStates keeps one candidate per active callsign in a TEMP table. Identical
// duplicates agree; disagreements (including blank versus known), bad state or
// mismatched identity poison only the state, never active-license membership.
func importStates(ctx context.Context, db *sql.DB, dir string) error {
	path := resolveFile(dir, []string{"EN.DAT"})
	if path == "" {
		return fmt.Errorf("fcc uls: missing data file for EN")
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("fcc uls: open EN: %w", err)
	}
	defer f.Close()
	return importStatesReader(ctx, db, f)
}

// importStatesReader keeps parsing and transactional evidence ownership together.
// The reader seam also permits bounded in-memory fuzzing without filesystem I/O.
func importStatesReader(ctx context.Context, db *sql.DB, source io.Reader) (err error) {
	var active int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM AM;").Scan(&active); err != nil {
		return err
	}
	if active == 0 {
		return fmt.Errorf("fcc uls: empty active amateur projection")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("fcc uls: rollback state import: %w", rollbackErr))
		}
	}()
	if _, err := tx.ExecContext(ctx, `CREATE TEMP TABLE state_candidates(call_sign TEXT PRIMARY KEY, state TEXT NOT NULL, uncertain INTEGER NOT NULL);`); err != nil {
		return err
	}
	// Coverage is bounded by distinct active AM identities. A callsign can have
	// several active licenses; missing evidence for any one leaves it unknown.
	if _, err := tx.ExecContext(ctx, `CREATE TEMP TABLE state_coverage(unique_system_identifier INTEGER, call_sign TEXT, PRIMARY KEY(unique_system_identifier, call_sign));`); err != nil {
		return err
	}
	coverage, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO state_coverage SELECT unique_system_identifier, call_sign FROM AM WHERE unique_system_identifier = ? AND call_sign = ?;`)
	if err != nil {
		return err
	}
	defer coverage.Close()
	const merge = ` ON CONFLICT(call_sign) DO UPDATE SET uncertain = CASE WHEN state_candidates.uncertain != 0 OR excluded.uncertain != 0 OR state_candidates.state != excluded.state THEN 1 ELSE 0 END;`
	matched, err := tx.PrepareContext(ctx, `INSERT INTO state_candidates SELECT call_sign, ?, ? FROM AM WHERE unique_system_identifier = ? AND call_sign = ?`+merge)
	if err != nil {
		return err
	}
	defer matched.Close()
	mismatched, err := tx.PrepareContext(ctx, `INSERT INTO state_candidates SELECT call_sign, '', 1 FROM AM WHERE unique_system_identifier = ?`+merge)
	if err != nil {
		return err
	}
	defer mismatched.Close()
	scanner := bufio.NewScanner(source)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	usable, malformed := 0, 0
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		fields := strings.Split(scanner.Text(), "|")
		if len(fields) < 6 {
			malformed++
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(fields[5]), "L") {
			continue
		}
		id, err := parseID(fields[1])
		if err != nil {
			malformed++
			continue
		}
		call := strings.ToUpper(strings.TrimSpace(fields[4]))
		if call == "" {
			malformed++
			if _, err := mismatched.ExecContext(ctx, id); err != nil {
				return err
			}
			continue
		}
		state, uncertain := parseMailingState(fields)
		if uncertain != 0 {
			malformed++
		}

		result, err := matched.ExecContext(ctx, state, uncertain, id, call)
		if err != nil {
			return fmt.Errorf("fcc uls: join EN: %w", err)
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count > 0 {
			usable++
			if _, err := coverage.ExecContext(ctx, id, call); err != nil {
				return err
			}
		}
		if count == 0 {
			if _, err := mismatched.ExecContext(ctx, id); err != nil {
				return err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("fcc uls: scan EN: %w", err)
	}
	if usable == 0 {
		return fmt.Errorf("fcc uls: EN has no usable licensee identities")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE state_candidates SET uncertain = 1 WHERE EXISTS (SELECT 1 FROM AM LEFT JOIN state_coverage ON AM.unique_system_identifier = state_coverage.unique_system_identifier AND AM.call_sign = state_coverage.call_sign WHERE AM.call_sign = state_candidates.call_sign AND state_coverage.unique_system_identifier IS NULL);`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE AM SET state = COALESCE((SELECT CASE WHEN uncertain = 0 THEN state ELSE '' END FROM state_candidates WHERE state_candidates.call_sign = AM.call_sign), '');`); err != nil {
		return err
	}
	var known, uncertain int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM AM WHERE state != '';").Scan(&known); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM state_candidates WHERE uncertain != 0;").Scan(&uncertain); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DROP TABLE state_candidates;"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DROP TABLE state_coverage;"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	log.Printf("FCC ULS import: active=%d known_state=%d unknown_state=%d uncertain_calls=%d malformed_EN=%d schema=%d", active, known, active-known, uncertain, malformed, CurrentSchemaVersion)
	return nil
}

// parseMailingState normalizes only at the FCC boundary. Uncertain evidence is
// sticky in the transaction, whereas a genuine blank is a valid unknown value.
func parseMailingState(fields []string) (string, int) {
	if len(fields) < 18 {
		return "", 1
	}
	state := strings.ToUpper(strings.TrimSpace(fields[17]))
	if state != "" && !spot.IsFCCState(state) {
		return "", 1
	}
	return state, 0
}
