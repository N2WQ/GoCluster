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

type oneCFaultState struct {
	nextErr, closeErr              error
	scanFault                      bool
	queries, nextCalls, rowsClosed atomic.Int32
	connClosed                     atomic.Int32
}

// The isolated Connector exercises database/sql row retirement without a
// registered driver or a replaced global production factory.
type oneCFaultConnector struct{ state *oneCFaultState }

func (c oneCFaultConnector) Connect(context.Context) (driver.Conn, error) {
	return oneCFaultConn(c), nil
}
func (oneCFaultConnector) Driver() driver.Driver { return oneCFaultDriver{} }

type oneCFaultDriver struct{}

func (oneCFaultDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("fixture requires its isolated connector")
}

type oneCFaultConn struct{ state *oneCFaultState }

func (oneCFaultConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fixture does not prepare statements")
}
func (c oneCFaultConn) Close() error {
	c.state.connClosed.Add(1)
	return nil
}
func (oneCFaultConn) Begin() (driver.Tx, error) {
	return nil, errors.New("fixture does not begin transactions")
}
func (c oneCFaultConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(query, "FROM decisions") {
		return nil, errors.New("fixture did not reach historical decisions loader query")
	}
	c.state.queries.Add(1)
	return &oneCFaultRows{state: c.state}, nil
}

type oneCFaultRows struct {
	state *oneCFaultState
	step  int
}

func (*oneCFaultRows) Columns() []string {
	return []string{"distance", "decision", "winner_confidence", "reason"}
}
func (r *oneCFaultRows) Close() error {
	r.state.rowsClosed.Add(1)
	return r.state.closeErr
}
func (r *oneCFaultRows) Next(values []driver.Value) error {
	r.state.nextCalls.Add(1)
	r.step++
	if r.step == 1 {
		copy(values, []driver.Value{int64(1), "APPLIED", int64(40), nil})
		return nil
	}
	if r.state.nextErr != nil {
		return r.state.nextErr
	}
	if r.step == 2 && r.state.scanFault {
		copy(values, []driver.Value{int64(2), "pending", "bad-score", nil})
		return nil
	}
	return io.EOF
}

func TestDistanceAnalysisLoaderNextAndCloseFailuresRetireRows(t *testing.T) {
	nextFault := errors.New("fixture Next failure after valid row")
	closeFault := errors.New("fixture row Close failure")
	for _, tc := range []struct {
		name  string
		state *oneCFaultState
		cause error
		wants []string
	}{
		{"next", &oneCFaultState{nextErr: nextFault}, nextFault, []string{"iterate distance summaries"}},
		// database/sql records automatic EOF Close failure in Rows.Err.
		{"eof-close", &oneCFaultState{closeErr: closeFault}, closeFault, []string{"iterate distance summaries"}},
		{"scan-and-close", &oneCFaultState{scanFault: true, closeErr: closeFault}, closeFault, []string{"scan distance-summary row", "close distance-summary rows", "bad-score"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sql.OpenDB(oneCFaultConnector{tc.state})
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Errorf("fixture connection close: %v", err)
				}
			})
			a, err := loadDistanceAnalysis(db)
			if !errors.Is(err, tc.cause) || a != nil || tc.state.queries.Load() != 1 || tc.state.nextCalls.Load() != 2 || tc.state.rowsClosed.Load() != 1 {
				t.Fatalf("failure/retirement lost: analysis=%+v err=%v query%d next%d close%d", a, err, tc.state.queries.Load(), tc.state.nextCalls.Load(), tc.state.rowsClosed.Load())
			}
			oneCRequireText(t, err.Error(), tc.wants...)
			if err := db.Close(); err != nil || tc.state.connClosed.Load() != 1 {
				t.Fatalf("connection retirement: close%d err=%v", tc.state.connClosed.Load(), err)
			}
		})
	}
}

func TestDistanceAnalysisLoaderFaultConnectorPositiveControl(t *testing.T) {
	state := &oneCFaultState{}
	db := sql.OpenDB(oneCFaultConnector{state})
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("fixture connection close: %v", err)
		}
	})
	a, err := loadDistanceAnalysis(db)
	if err != nil || a == nil || a.totalDecisions != 1 || a.appliedCount != 1 || a.rejectedCount != 0 || a.unknownCount != 0 || len(a.byDistance) != 1 || len(a.unknownOutcomes) != 0 || state.queries.Load() != 1 || state.nextCalls.Load() != 2 || state.rowsClosed.Load() != 1 {
		t.Fatalf("connector positive witness: analysis%+v err=%v query%d next%d close%d", a, err, state.queries.Load(), state.nextCalls.Load(), state.rowsClosed.Load())
	}
	oneCRequireGroup(t, a, 1, 1, 1, 0, 0, []int{40}, nil)
	if err := db.Close(); err != nil || state.connClosed.Load() != 1 {
		t.Fatalf("positive connection retirement: close%d err=%v", state.connClosed.Load(), err)
	}
}
