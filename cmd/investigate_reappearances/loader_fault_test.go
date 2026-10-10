package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
)

type reappearanceFaultState struct {
	failOwner, kind                                  string
	cause                                            error
	outerQueries, innerQueries, connects, connClosed atomic.Int32
	outerNext, outerClosed                           atomic.Int32
	innerNext, innerClosed                           [2]atomic.Int32
}

// Each OpenDB owns its Connector; no driver registration or global production
// factory is replaced. Nested queries use a second connection while outer rows
// remain owned, and all created connections must retire on DB.Close.
type reappearanceFaultConnector struct{ state *reappearanceFaultState }

func (c reappearanceFaultConnector) Connect(context.Context) (driver.Conn, error) {
	c.state.connects.Add(1)
	return reappearanceFaultConn(c), nil
}
func (reappearanceFaultConnector) Driver() driver.Driver { return reappearanceFaultDriver{} }

type reappearanceFaultDriver struct{}

func (reappearanceFaultDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("fixture requires its isolated Connector")
}

type reappearanceFaultConn struct{ state *reappearanceFaultState }

func (reappearanceFaultConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fixture does not prepare statements")
}
func (c reappearanceFaultConn) Close() error { c.state.connClosed.Add(1); return nil }
func (reappearanceFaultConn) Begin() (driver.Tx, error) {
	return nil, errors.New("fixture preserves independent selections, not a snapshot transaction")
}

func (c reappearanceFaultConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(query, "FROM decisions") {
		return nil, errors.New("fixture missed historical decision selection")
	}
	if len(args) == 0 {
		if c.state.outerQueries.Add(1) != 1 {
			return nil, errors.New("unexpected second applied seed query")
		}
		if c.state.failOwner == "outer" && c.state.kind == "query" {
			return nil, c.state.cause
		}
		return &reappearanceFaultOuterRows{state: c.state}, nil
	}
	index := int(c.state.innerQueries.Add(1)) - 1
	if index >= 2 || len(args) != 4 || args[0].Value != int64(1000) || args[1].Value != int64(87400) || args[2].Value != fmt.Sprintf("S%d", index+1) || args[3].Value != fmt.Sprintf("W%d", index+1) {
		return nil, fmt.Errorf("unexpected normalized follow-up arguments at%d: %v", index, args)
	}
	if c.state.failOwner == "followup" && index == 1 && c.state.kind == "query" {
		return nil, c.state.cause
	}
	return &reappearanceFaultInnerRows{state: c.state, index: index}, nil
}

type reappearanceFaultOuterRows struct {
	state *reappearanceFaultState
	step  int
}

func (*reappearanceFaultOuterRows) Columns() []string {
	return []string{"id", "ts", "subject", "winner", "freq_khz", "distance", "winner_confidence"}
}
func (r *reappearanceFaultOuterRows) Close() error {
	r.state.outerClosed.Add(1)
	if r.state.failOwner == "outer" && (r.state.kind == "eof-close" || r.state.kind == "scan-and-close") {
		return r.state.cause
	}
	return nil
}
func (r *reappearanceFaultOuterRows) Next(values []driver.Value) error {
	r.state.outerNext.Add(1)
	r.step++
	if r.step == 2 && r.state.failOwner == "outer" {
		switch r.state.kind {
		case "next":
			return r.state.cause
		case "scan", "scan-and-close":
			copy(values, []driver.Value{int64(2), int64(1000), "S2", "W2", "bad-seed-frequency", int64(3), int64(65)})
			return nil
		default:
			return io.EOF
		}
	}
	if r.step > 2 {
		return io.EOF
	}
	copy(values, []driver.Value{int64(r.step), int64(1000), fmt.Sprintf(" s%d ", r.step), fmt.Sprintf(" w%d ", r.step), float64(14074), int64(3), int64(65)})
	return nil
}

type reappearanceFaultInnerRows struct {
	state       *reappearanceFaultState
	index, step int
}

func (*reappearanceFaultInnerRows) Columns() []string { return []string{"subject", "freq_khz"} }
func (r *reappearanceFaultInnerRows) Close() error {
	r.state.innerClosed[r.index].Add(1)
	if r.state.failOwner == "followup" && r.index == 1 && (r.state.kind == "eof-close" || r.state.kind == "scan-and-close") {
		return r.state.cause
	}
	return nil
}
func (r *reappearanceFaultInnerRows) Next(values []driver.Value) error {
	r.state.innerNext[r.index].Add(1)
	r.step++
	if r.step <= 2 {
		name, freq := fmt.Sprintf("s%d", r.index+1), float64(7000)
		if r.step == 2 {
			name, freq = fmt.Sprintf("w%d", r.index+1), 7000.5
		}
		copy(values, []driver.Value{name, freq})
		return nil
	}
	if r.state.failOwner == "followup" && r.index == 1 {
		if r.state.kind == "next" {
			return r.state.cause
		}
		if r.step == 3 && (r.state.kind == "scan" || r.state.kind == "scan-and-close") {
			copy(values, []driver.Value{"s2", "bad-followup-frequency"})
			return nil
		}
	}
	return io.EOF
}

func reappearanceAssertFaultRetirement(t *testing.T, state *reappearanceFaultState, outerNext int32, innerNext [2]int32) {
	t.Helper()
	wantOuterClosed := int32(1)
	if state.failOwner == "outer" && state.kind == "query" {
		wantOuterClosed = 0
	}
	if state.outerNext.Load() != outerNext || state.outerClosed.Load() != wantOuterClosed {
		t.Errorf("outer ownership next%d close%d want%d/%d", state.outerNext.Load(), state.outerClosed.Load(), outerNext, wantOuterClosed)
	}
	for i := range 2 {
		wantClose := int32(0)
		if innerNext[i] > 0 {
			wantClose = 1
		}
		if state.innerNext[i].Load() != innerNext[i] || state.innerClosed[i].Load() != wantClose {
			t.Errorf("nested%d next%d close%d want%d/%d", i+1, state.innerNext[i].Load(), state.innerClosed[i].Load(), innerNext[i], wantClose)
		}
	}
}

func TestReappearanceOuterAndLateNestedFailuresRefuseAllPartialCases(t *testing.T) {
	for _, owner := range []string{"outer", "followup"} {
		for _, kind := range []string{"query", "scan", "next", "eof-close", "scan-and-close"} {
			t.Run(owner+"-"+kind, func(t *testing.T) {
				cause := errors.New("fixture causal " + owner + " " + kind + " failure")
				state := &reappearanceFaultState{failOwner: owner, kind: kind, cause: cause}
				db := sql.OpenDB(reappearanceFaultConnector{state: state})
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Errorf("fixture database close: %v", err)
					}
				})
				cases, err := analyzeBothReappearances(db, 24)
				if cases != nil || err == nil || state.outerQueries.Load() != 1 {
					t.Fatalf("failure returned partialcases: %+v err=%v outerqueries%d", cases, err, state.outerQueries.Load())
				}
				if kind != "scan" && !errors.Is(err, cause) {
					t.Errorf("causal error lost: %v", err)
				}
				prefix := "iterate "
				switch kind {
				case "query", "scan":
					prefix = kind + " "
				case "scan-and-close":
					prefix = "scan "
				}
				label, closeLabel, badCell := "applied corrections", "applied-correction rows", "bad-seed-frequency"
				wantOuterNext, wantInnerQueries, wantInnerNext := int32(2), int32(1), [2]int32{3, 0}
				if owner == "followup" {
					label, closeLabel, badCell = "follow-up observations", "follow-up rows", "bad-followup-frequency"
					wantInnerQueries, wantInnerNext = 2, [2]int32{3, 3}
					reappearanceRequireText(t, err.Error(), "correction 2 follow-up")
					if kind == "query" {
						wantInnerNext[1] = 0
					}
				} else if kind == "query" {
					wantOuterNext, wantInnerQueries, wantInnerNext = 0, 0, [2]int32{0, 0}
				}
				reappearanceRequireText(t, err.Error(), prefix+label)
				if kind == "scan" || kind == "scan-and-close" {
					reappearanceRequireText(t, err.Error(), badCell)
				}
				if kind == "scan-and-close" {
					reappearanceRequireText(t, err.Error(), "close "+closeLabel)
				}
				if state.innerQueries.Load() != wantInnerQueries {
					t.Errorf("nested querycount%d want%d", state.innerQueries.Load(), wantInnerQueries)
				}
				reappearanceAssertFaultRetirement(t, state, wantOuterNext, wantInnerNext)
				if err := db.Close(); err != nil || state.connClosed.Load() != state.connects.Load() || state.connects.Load() == 0 {
					t.Fatalf("connection retirement: opened%d closed%d err=%v", state.connects.Load(), state.connClosed.Load(), err)
				}
			})
		}
	}
}

func TestReappearanceNestedConnectorPositiveControl(t *testing.T) {
	state := &reappearanceFaultState{}
	db := sql.OpenDB(reappearanceFaultConnector{state: state})
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("fixture database close: %v", err)
		}
	})
	cases, err := analyzeBothReappearances(db, 24)
	if err != nil || len(cases) != 2 || state.outerQueries.Load() != 1 || state.innerQueries.Load() != 2 {
		t.Fatalf("nested goodrow positivecontrol: %+v err=%v outer%d inner%d", cases, err, state.outerQueries.Load(), state.innerQueries.Load())
	}
	for i, c := range cases {
		if c.correctionID != int64(i+1) || c.subject != fmt.Sprintf("S%d", i+1) || c.winner != fmt.Sprintf("W%d", i+1) || c.subjectFreqKHz != 7000 || c.winnerFreqKHz != 7000.5 || c.freqSeparationKHz != 0.5 {
			t.Errorf("literal nested case%d: %+v", i+1, c)
		}
	}
	reappearanceAssertFaultRetirement(t, state, 3, [2]int32{3, 3})
	if err := db.Close(); err != nil || state.connClosed.Load() != state.connects.Load() || state.connects.Load() != 2 {
		t.Fatalf("nested connection ownership: opened%d closed%d err=%v", state.connects.Load(), state.connClosed.Load(), err)
	}
}
