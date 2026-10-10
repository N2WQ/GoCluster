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

type distance3FaultState struct {
	nextErr, closeErr              error
	scanFault                      bool
	queries, nextCalls, rowsClosed atomic.Int32
	connClosed                     atomic.Int32
}

type distance3FaultConnector struct{ state *distance3FaultState }

func (c distance3FaultConnector) Connect(context.Context) (driver.Conn, error) {
	return distance3FaultConn(c), nil
}
func (distance3FaultConnector) Driver() driver.Driver { return distance3FaultDriver{} }

type distance3FaultDriver struct{}

func (distance3FaultDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("fixture requires its isolated connector")
}

type distance3FaultConn struct{ state *distance3FaultState }

func (distance3FaultConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fixture does not prepare statements")
}
func (c distance3FaultConn) Close() error {
	c.state.connClosed.Add(1)
	return nil
}
func (distance3FaultConn) Begin() (driver.Tx, error) {
	return nil, errors.New("fixture does not begin transactions")
}
func (c distance3FaultConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(query, "FROM decisions WHERE distance = 3") {
		return nil, errors.New("fixture did not reach distance-3 loader query")
	}
	c.state.queries.Add(1)
	return &distance3FaultRows{state: c.state}, nil
}

type distance3FaultRows struct {
	state *distance3FaultState
	step  int
}

func (*distance3FaultRows) Columns() []string {
	return []string{"subject", "winner", "winner_support", "subject_support", "total_reporters", "winner_confidence", "decision", "reason"}
}
func (r *distance3FaultRows) Close() error {
	r.state.rowsClosed.Add(1)
	return r.state.closeErr
}
func (r *distance3FaultRows) Next(values []driver.Value) error {
	r.state.nextCalls.Add(1)
	r.step++
	if r.step == 1 {
		copy(values, []driver.Value{"GOOD", "W1XYZ", int64(3), int64(1), int64(4), int64(65), "applied", ""})
		return nil
	}
	if r.state.nextErr != nil {
		return r.state.nextErr
	}
	if r.step == 2 && r.state.scanFault {
		copy(values, []driver.Value{"BAD", "W1XYZ", "bad-support", int64(1), int64(4), int64(65), "applied", ""})
		return nil
	}
	return io.EOF
}

func TestDistance3LoaderNextAndCloseFailuresRetireRows(t *testing.T) {
	nextFault := errors.New("fixture Next failure after valid row")
	closeFault := errors.New("fixture row Close failure")
	for _, tc := range []struct {
		name  string
		state *distance3FaultState
		cause error
		wants []string
	}{
		{"next", &distance3FaultState{nextErr: nextFault}, nextFault, []string{"iterate distance-3 decisions"}},
		// database/sql records an automatic EOF Close failure in Rows.Err.
		{"eof-close", &distance3FaultState{closeErr: closeFault}, closeFault, []string{"iterate distance-3 decisions"}},
		{"scan-and-close", &distance3FaultState{scanFault: true, closeErr: closeFault}, closeFault, []string{"scan distance-3 decision", "close distance-3 rows", "bad-support"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sql.OpenDB(distance3FaultConnector{tc.state})
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Errorf("fixture connection close: %v", err)
				}
			})
			cases, err := loadDistance3Cases(db)
			if !errors.Is(err, tc.cause) || cases != nil || tc.state.queries.Load() != 1 || tc.state.nextCalls.Load() != 2 || tc.state.rowsClosed.Load() != 1 {
				t.Fatalf("failure/retirement lost: cases=%v err=%v queries=%d next=%d close=%d", cases, err, tc.state.queries.Load(), tc.state.nextCalls.Load(), tc.state.rowsClosed.Load())
			}
			for _, want := range tc.wants {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("causal error %q missing: %v", want, err)
				}
			}
			if err := db.Close(); err != nil || tc.state.connClosed.Load() != 1 {
				t.Fatalf("connection retirement: close=%d err=%v", tc.state.connClosed.Load(), err)
			}
		})
	}
}

func TestDistance3LoaderFaultConnectorPositiveControl(t *testing.T) {
	state := &distance3FaultState{}
	db := sql.OpenDB(distance3FaultConnector{state})
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("fixture connection close: %v", err)
		}
	})
	cases, err := loadDistance3Cases(db)
	if err != nil || len(cases) != 1 || cases[0].subject != "GOOD" || cases[0].winnerSupport != 3 || cases[0].decision != (sql.NullString{String: "applied", Valid: true}) || state.queries.Load() != 1 || state.nextCalls.Load() != 2 || state.rowsClosed.Load() != 1 {
		t.Fatalf("connector positive witness: %+v %v queries=%d next=%d close=%d", cases, err, state.queries.Load(), state.nextCalls.Load(), state.rowsClosed.Load())
	}
}
