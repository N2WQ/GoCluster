// File role: Builds the Canadian snapshot from ISED's literal semicolon records.
// Names, street addresses and event descriptions never enter the projection.
package uls

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"dxcluster/internal/fsutil"
	"dxcluster/spot"
)

const (
	CanadianSchemaVersion       = 1
	maxISEDMainBytes      int64 = 64 << 20
	maxISEDSpecialBytes   int64 = 8 << 20
	maxISEDLineBytes            = 64 << 10
	maxISEDMainRows             = 1_000_000
	maxISEDSpecialRows          = 100_000
)

var isedMainHeader = []string{"callsign", "first_name", "surname", "address_line", "city", "prov_cd", "postal_code", "qual_a", "qual_b", "qual_c", "qual_d", "qual_e", "club_name", "club_name_2", "club_address", "club_city", "club_prov_cd", "club_postal_code"}
var isedSpecialHeader = []string{"special_call_sign", "beg_date", "end_date", "event_en", "event_fr", "call_sign_request", "use_by"}

const canadianSchema = `CREATE TABLE CA(call_sign TEXT PRIMARY KEY, state TEXT NOT NULL);
CREATE TABLE Events(id INTEGER PRIMARY KEY, special TEXT NOT NULL, start_day TEXT NOT NULL, end_day TEXT NOT NULL, trustee TEXT NOT NULL, use_by TEXT NOT NULL, uncertain INTEGER NOT NULL, prefix_kind INTEGER NOT NULL);
CREATE INDEX idx_Events_special ON Events(special);
CREATE TABLE SourceMeta(id INTEGER PRIMARY KEY CHECK(id=1), main_sha TEXT NOT NULL, special_sha TEXT NOT NULL);`

type isedImportCounts struct{ rows, malformed, uncertain int }

// buildCanadianDatabase owns its scratch database until both sources commit.
// The final file stays in the publication directory so the shared rename keeps
// the last-good snapshot intact on a failed swap. No runtime side table exists.
func buildCanadianDatabase(ctx context.Context, mainPath, specialPath, dbPath, mainSHA, specialSHA string) (tmpPath string, err error) {
	if err := fsutil.EnsureParentDir(dbPath, "ised: create db directory"); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(filepath.Dir(dbPath), "ised-*.dbtmp")
	if err != nil {
		return "", fmt.Errorf("ised: create temporary database: %w", err)
	}
	tmpPath = f.Name()
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}
	success := false
	ownedPath := tmpPath
	defer func() {
		if !success {
			_ = os.Remove(ownedPath)
		}
	}()
	db, err := sql.Open("sqlite", tmpPath+"?_pragma=journal_mode(OFF)&_pragma=synchronous(OFF)")
	if err != nil {
		return "", err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	// This streaming projection requires no SQLite TEMP tables or sorts. Do not
	// set temp_store_directory: it is process-global and would redirect FCC work.
	if _, err := db.ExecContext(ctx, canadianSchema); err != nil {
		return "", err
	}
	if err := importCanadianFiles(ctx, db, mainPath, specialPath); err != nil {
		return "", err
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO SourceMeta(id,main_sha,special_sha) VALUES(1,?,?);", mainSHA, specialSHA); err != nil {
		return "", err
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version=%d;", CanadianSchemaVersion)); err != nil {
		return "", err
	}
	if err := probeCanadianDatabase(ctx, db); err != nil {
		return "", err
	}
	if err := db.Close(); err != nil {
		return "", err
	}
	success = true
	return tmpPath, nil
}

func importCanadianFiles(ctx context.Context, db *sql.DB, mainPath, specialPath string) error {
	main, err := os.Open(mainPath) // #nosec G703 -- Fixed member under caller-owned extraction directory or trusted local fixture; never a source-record path.
	if err != nil {
		return fmt.Errorf("ised: open assigned calls: %w", err)
	}
	defer main.Close()
	special, err := os.Open(specialPath) // #nosec G703 -- Fixed member under caller-owned extraction directory or trusted local fixture; never a source-record path.
	if err != nil {
		return fmt.Errorf("ised: open special calls: %w", err)
	}
	defer special.Close()
	return importCanadianReaders(ctx, db, main, special)
}

// importCanadianReaders streams into SQLite, with transaction lifetime and all
// cardinality/line bounds owned by this build. A malformed structural record
// rejects the pair; uncertain event evidence poisons only matching candidates.
func importCanadianReaders(ctx context.Context, db *sql.DB, main, special io.Reader) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, rollbackErr)
		}
	}()
	mainCounts, err := importCanadianMain(ctx, tx, main)
	if err != nil {
		return err
	}
	eventCounts, err := importCanadianEvents(ctx, tx, special)
	if err != nil {
		return err
	}
	var total, known int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(SUM(state != ''),0) FROM CA;").Scan(&total, &known); err != nil {
		return err
	}
	if total == 0 {
		return errors.New("ised: empty assigned-call projection")
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	log.Printf("ISED import: assigned=%d known_province=%d unknown_province=%d main_rows=%d malformed_main=%d event_rows=%d uncertain_events=%d malformed_events=%d schema=%d", total, known, total-known, mainCounts.rows, mainCounts.malformed, eventCounts.rows, eventCounts.uncertain, eventCounts.malformed, CanadianSchemaVersion)
	return nil
}

func importCanadianMain(ctx context.Context, tx *sql.Tx, source io.Reader) (counts isedImportCounts, err error) {
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO CA(call_sign,state) VALUES(?,?) ON CONFLICT(call_sign) DO UPDATE SET state=CASE WHEN CA.state=excluded.state THEN CA.state ELSE '' END;`)
	if err != nil {
		return counts, err
	}
	defer stmt.Close()
	err = readISEDRecords(ctx, source, isedMainHeader, maxISEDMainBytes, maxISEDMainRows, func(fields []string, indexes map[string]int) error {
		call := isedIdentity(fields[indexes["callsign"]])
		if call == "" {
			return errors.New("ised: invalid assigned callsign")
		}
		state, bad := canadianProvince(fields, indexes)
		counts.rows++
		if bad {
			counts.malformed++
		}
		_, err := stmt.ExecContext(ctx, call, state)
		return err
	})
	return counts, err
}

func importCanadianEvents(ctx context.Context, tx *sql.Tx, source io.Reader) (counts isedImportCounts, err error) {
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO Events(special,start_day,end_day,trustee,use_by,uncertain,prefix_kind) VALUES(?,?,?,?,?,?,?);`)
	if err != nil {
		return counts, err
	}
	defer stmt.Close()
	err = readISEDRecords(ctx, source, isedSpecialHeader, maxISEDSpecialBytes, maxISEDSpecialRows, func(fields []string, indexes map[string]int) error {
		special := isedIdentity(fields[indexes["special_call_sign"]])
		if special == "" {
			return errors.New("ised: invalid special callsign")
		}
		start, end := strings.TrimSpace(fields[indexes["beg_date"]]), strings.TrimSpace(fields[indexes["end_date"]])
		uncertain := !isedDate(start) || !isedDate(end) || start > end
		trustee := isedIdentity(fields[indexes["call_sign_request"]])
		useBy := strings.ToUpper(strings.TrimSpace(fields[indexes["use_by"]]))
		prefixKind := len(special) <= 3
		if prefixKind && (!validISEDSpecialPrefix(special) || !validISEDUseBy(useBy)) {
			uncertain = true
		}
		// Malformed fields need only an uncertainty marker, not their free text.
		// Exact assignments do not depend on use_by, so omit that evidence too.
		if !isedDate(start) {
			start = ""
		}
		if !isedDate(end) {
			end = ""
		}
		if !prefixKind || !validISEDUseBy(useBy) {
			useBy = ""
		}
		counts.rows++
		if uncertain {
			counts.uncertain++
			counts.malformed++
		}
		_, err := stmt.ExecContext(ctx, special, start, end, trustee, useBy, boolInt(uncertain), boolInt(prefixKind))
		return err
	})
	return counts, err
}

// readISEDRecords deliberately has no CSV quote semantics: ISED embeds literal
// bare and leading quotes. Exact width, named headers, UTF-8 and final newlines
// distinguish complete records from truncation; discarded PII is never logged.
func readISEDRecords(ctx context.Context, source io.Reader, header []string, byteLimit int64, rowLimit int, accept func([]string, map[string]int) error) error {
	limited := &io.LimitedReader{R: source, N: byteLimit + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 4096), maxISEDLineBytes)
	scanner.Split(isedLines)
	var indexes map[string]int
	var total int64
	rows := 0
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := scanner.Text()
		total += int64(len(line)) + 1
		if total > byteLimit {
			return errors.New("ised: plaintext exceeds byte limit")
		}
		if !utf8.ValidString(line) || strings.ContainsRune(line, 0) {
			return errors.New("ised: invalid record encoding")
		}
		fields := strings.Split(line, ";")
		if len(fields) != len(header) {
			return fmt.Errorf("ised: record width %d, expected %d", len(fields), len(header))
		}
		if indexes == nil {
			indexes = make(map[string]int, len(header))
			for i, field := range fields {
				key := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(field, "\ufeff")))
				if _, exists := indexes[key]; exists {
					return errors.New("ised: duplicate header")
				}
				indexes[key] = i
			}
			for _, name := range header {
				if _, exists := indexes[name]; !exists {
					return fmt.Errorf("ised: missing header %s", name)
				}
			}
			continue
		}
		rows++
		if rows > rowLimit {
			return errors.New("ised: row limit exceeded")
		}
		if err := accept(fields, indexes); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("ised: read records: %w", err)
	}
	if limited.N == 0 {
		return errors.New("ised: plaintext exceeds byte limit")
	}
	if indexes == nil {
		return errors.New("ised: missing header")
	}
	return nil
}

func isedLines(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, bytes.TrimSuffix(data[:i], []byte{'\r'}), nil
	}
	if atEOF && len(data) != 0 {
		return 0, nil, errors.New("unterminated final record")
	}
	return 0, nil, nil
}

func isedIdentity(raw string) string {
	value := strings.ToUpper(strings.TrimSpace(raw))
	if len(value) < 2 || len(value) > 16 {
		return ""
	}
	for _, ch := range value {
		if (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') {
			return ""
		}
	}
	return value
}

func canadianProvince(fields []string, indexes map[string]int) (string, bool) {
	club := false
	for _, name := range []string{"club_name", "club_name_2", "club_address", "club_city", "club_prov_cd", "club_postal_code"} {
		if strings.TrimSpace(fields[indexes[name]]) != "" {
			club = true
			break
		}
	}
	key := "prov_cd"
	if club {
		key = "club_prov_cd"
	}
	state := strings.ToUpper(strings.TrimSpace(fields[indexes[key]]))
	if state != "" && !isCanadianProvince(state) {
		return "", true
	}
	return state, false
}

func isCanadianProvince(state string) bool {
	return spot.IsCanadianProvince(state)
}

func isedDate(value string) bool {
	_, err := time.Parse("2006-01-02", value)
	return err == nil && len(value) == 10
}
func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
