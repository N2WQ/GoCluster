package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
)

type thresholdFaultState struct {
	nextErr, closeErr              error
	scanFault                      bool
	queries, nextCalls, rowsClosed atomic.Int32
	connClosed                     atomic.Int32
}

// OpenDB confines the fault to this test-owned connection; no global driver or
// production database factory is replaced.
type thresholdFaultConnector struct{ state *thresholdFaultState }

func (c thresholdFaultConnector) Connect(context.Context) (driver.Conn, error) {
	return thresholdFaultConn(c), nil
}
func (thresholdFaultConnector) Driver() driver.Driver { return thresholdFaultDriver{} }

type thresholdFaultDriver struct{}

func (thresholdFaultDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("fixture requires its isolated connector")
}

type thresholdFaultConn struct{ state *thresholdFaultState }

func (thresholdFaultConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fixture does not prepare statements")
}
func (c thresholdFaultConn) Close() error {
	c.state.connClosed.Add(1)
	return nil
}
func (thresholdFaultConn) Begin() (driver.Tx, error) {
	return nil, errors.New("fixture does not begin transactions")
}
func (c thresholdFaultConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(query, "FROM decisions WHERE distance > 0 AND distance <= 3") {
		return nil, errors.New("fixture did not reach selected historical loader query")
	}
	c.state.queries.Add(1)
	return &thresholdFaultRows{state: c.state}, nil
}

type thresholdFaultRows struct {
	state *thresholdFaultState
	step  int
}

func (*thresholdFaultRows) Columns() []string {
	return []string{"id", "subject", "winner", "distance", "winner_confidence", "winner_support", "subject_support", "total_reporters",
		"min_reports", "min_advantage", "min_confidence", "decision", "reason"}
}
func (r *thresholdFaultRows) Close() error {
	r.state.rowsClosed.Add(1)
	return r.state.closeErr
}
func (r *thresholdFaultRows) Next(values []driver.Value) error {
	r.state.nextCalls.Add(1)
	r.step++
	if r.step == 1 {
		copy(values, []driver.Value{int64(1), " good ", " w1xyz ", int64(1), int64(55), int64(3), int64(1), int64(4),
			int64(3), int64(1), int64(60), "applied", ""})
		return nil
	}
	if r.state.nextErr != nil {
		return r.state.nextErr
	}
	if r.step == 2 && r.state.scanFault {
		copy(values, []driver.Value{int64(2), "BAD", "W1XYZ", int64(1), int64(55), "bad-support", int64(1), int64(4),
			int64(3), int64(1), int64(60), "applied", ""})
		return nil
	}
	return io.EOF
}

func TestThresholdLoaderNextAndCloseFailuresRetireRows(t *testing.T) {
	nextFault := errors.New("fixture Next failure after valid row")
	closeFault := errors.New("fixture row Close failure")
	for _, tc := range []struct {
		name  string
		state *thresholdFaultState
		cause error
		wants []string
	}{
		{"next", &thresholdFaultState{nextErr: nextFault}, nextFault, []string{"iterate threshold decisions"}},
		// database/sql exposes automatic EOF Close failure through Rows.Err.
		{"eof-close", &thresholdFaultState{closeErr: closeFault}, closeFault, []string{"iterate threshold decisions"}},
		{"scan-and-close", &thresholdFaultState{scanFault: true, closeErr: closeFault}, closeFault, []string{"scan threshold decision", "close threshold rows", "bad-support"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sql.OpenDB(thresholdFaultConnector{tc.state})
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Errorf("fixture connection close: %v", err)
				}
			})
			rows, err := loadAllDecisions(db)
			if !errors.Is(err, tc.cause) || rows != nil || tc.state.queries.Load() != 1 || tc.state.nextCalls.Load() != 2 || tc.state.rowsClosed.Load() != 1 {
				t.Fatalf("failure/retirement lost: rows=%v err=%v queries=%d next=%d close=%d", rows, err, tc.state.queries.Load(), tc.state.nextCalls.Load(), tc.state.rowsClosed.Load())
			}
			thresholdRequireText(t, err.Error(), tc.wants...)
			if err := db.Close(); err != nil || tc.state.connClosed.Load() != 1 {
				t.Fatalf("connection retirement: close=%d err=%v", tc.state.connClosed.Load(), err)
			}
		})
	}
}

func TestThresholdLoaderFaultConnectorPositiveControl(t *testing.T) {
	state := &thresholdFaultState{}
	db := sql.OpenDB(thresholdFaultConnector{state})
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("fixture connection close: %v", err)
		}
	})
	rows, err := loadAllDecisions(db)
	want := decisionRecord{id: 1, subject: "GOOD", winner: "W1XYZ", distance: 1, winnerConfidence: 55,
		winnerSupport: 3, subjectSupport: 1, totalReporters: 4, minReports: 3, minAdvantage: 1, minConfidence: 60,
		decision: sql.NullString{String: "applied", Valid: true}}
	if err != nil || len(rows) != 1 || rows[0] != want || state.queries.Load() != 1 || state.nextCalls.Load() != 2 || state.rowsClosed.Load() != 1 {
		t.Fatalf("connector positive witness: %+v err=%v queries=%d next=%d close=%d", rows, err, state.queries.Load(), state.nextCalls.Load(), state.rowsClosed.Load())
	}
	if err := db.Close(); err != nil || state.connClosed.Load() != 1 {
		t.Fatalf("positive connection retirement: close=%d err=%v", state.connClosed.Load(), err)
	}
}
