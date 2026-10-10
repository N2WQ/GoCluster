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

type inspectionFaultState struct {
	failStage             int
	kind                  string
	cause                 error
	largeCount            bool
	queries, connClosed   atomic.Int32
	nextCalls, rowsClosed [4]atomic.Int32
}

type inspectionFaultConnector struct{ state *inspectionFaultState }

func (c inspectionFaultConnector) Connect(context.Context) (driver.Conn, error) {
	return inspectionFaultConn(c), nil
}
func (inspectionFaultConnector) Driver() driver.Driver { return inspectionFaultDriver{} }

type inspectionFaultDriver struct{}

func (inspectionFaultDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("fixture requires its isolated connector")
}

type inspectionFaultConn struct{ state *inspectionFaultState }

func (inspectionFaultConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fixture does not prepare statements")
}
func (c inspectionFaultConn) Close() error {
	c.state.connClosed.Add(1)
	return nil
}
func (inspectionFaultConn) Begin() (driver.Tx, error) {
	return nil, errors.New("fixture preserves four independent queries, not a snapshot transaction")
}
func (c inspectionFaultConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	stage := int(c.state.queries.Add(1)) - 1
	if stage >= 4 || !strings.Contains(query, "FROM decisions") {
		return nil, errors.New("fixture did not reach the four historical selections")
	}
	if stage == c.state.failStage && c.state.kind == "query" {
		return nil, c.state.cause
	}
	return &inspectionFaultRows{state: c.state, stage: stage}, nil
}

type inspectionFaultRows struct {
	state       *inspectionFaultState
	stage, step int
}

func (r *inspectionFaultRows) Columns() []string {
	switch r.stage {
	case 0:
		return []string{"decision", "count"}
	case 1:
		return []string{"subject", "winner", "distance", "winner_confidence", "total_reporters", "decision"}
	case 2:
		return []string{"subject", "winner", "distance", "winner_confidence", "winner_support", "total_reporters", "min_reports", "min_advantage", "min_confidence", "decision", "reason"}
	default:
		return []string{"subject", "winner", "distance", "winner_confidence", "winner_support", "total_reporters", "decision", "reason"}
	}
}

func (r *inspectionFaultRows) Close() error {
	r.state.rowsClosed[r.stage].Add(1)
	if r.stage == r.state.failStage && (r.state.kind == "eof-close" || r.state.kind == "scan-and-close") {
		return r.state.cause
	}
	return nil
}

func (r *inspectionFaultRows) validRow() []driver.Value {
	switch r.stage {
	case 0:
		count := int64(3)
		if r.state.largeCount {
			count = (int64(1) << 31) + 17
		}
		return []driver.Value{"APPLIED", count}
	case 1:
		return []driver.Value{"APPLIED_GOOD", "W1XYZ", int64(2), int64(55), int64(4), "Applied"}
	case 2:
		return []driver.Value{"D1_GOOD", "W1XYZ", int64(1), int64(70), int64(3), int64(4), int64(3), int64(1), int64(60), "Rejected", nil}
	default:
		return []driver.Value{"HIGH_GOOD", "W1XYZ", int64(2), int64(71), int64(3), int64(4), "rejected", ""}
	}
}

func (r *inspectionFaultRows) Next(values []driver.Value) error {
	r.state.nextCalls[r.stage].Add(1)
	r.step++
	if r.step == 1 {
		copy(values, r.validRow())
		return nil
	}
	if r.stage == r.state.failStage {
		if r.state.kind == "next" {
			return r.state.cause
		}
		if r.step == 2 && (r.state.kind == "scan" || r.state.kind == "scan-and-close") {
			bad := r.validRow()
			index := []int{1, 4, 6, 4}[r.stage]
			bad[index] = "bad-selected-integer"
			copy(values, bad)
			return nil
		}
	}
	return io.EOF
}

// Every stage first yields a good row before Scan/Next/Close faults. Earlier
// stages must finish and later stages must remain untouched; a returned partial
// aggregate or a logged-and-continued failure cannot satisfy these assertions.
func TestInspectionEverySQLStageFailureRetiresRowsAndRefusesPartialData(t *testing.T) {
	stages := []string{"outcome breakdown", "applied samples", "distance-1 rejection samples", "high-score rejection samples"}
	for stage, name := range stages {
		for _, kind := range []string{"query", "scan", "next", "eof-close", "scan-and-close"} {
			t.Run(fmt.Sprintf("stage%d-%s", stage+1, kind), func(t *testing.T) {
				cause := errors.New("fixture causal " + kind + " failure")
				state := &inspectionFaultState{failStage: stage, kind: kind, cause: cause}
				db := sql.OpenDB(inspectionFaultConnector{state})
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Errorf("fixture connection close: %v", err)
					}
				})
				data, err := loadInspection(db)
				if data != nil || err == nil || state.queries.Load() != int32(stage+1) {
					t.Fatalf("stage refusal lost: data%+v err=%v queries%d", data, err, state.queries.Load())
				}
				if kind != "scan" && !errors.Is(err, cause) {
					t.Errorf("stage causal error lost: %v", err)
				}
				prefix := "iterate "
				switch kind {
				case "query", "scan":
					prefix = kind + " "
				case "scan-and-close":
					prefix = "scan "
					inspectionRequireText(t, err.Error(), "close "+name+" rows", "bad-selected-integer")
				}
				inspectionRequireText(t, err.Error(), prefix+name)
				if kind == "scan" {
					inspectionRequireText(t, err.Error(), "bad-selected-integer")
				}
				for i := range 4 {
					wantNext, wantClose := int32(0), int32(0)
					if i < stage || (i == stage && kind != "query") {
						wantNext, wantClose = 2, 1
					}
					if state.nextCalls[i].Load() != wantNext || state.rowsClosed[i].Load() != wantClose {
						t.Errorf("stage%d retirement next%d/close%d want%d/%d", i+1, state.nextCalls[i].Load(), state.rowsClosed[i].Load(), wantNext, wantClose)
					}
				}
				if err := db.Close(); err != nil || state.connClosed.Load() != 1 {
					t.Fatalf("connection retirement: close%d err=%v", state.connClosed.Load(), err)
				}
			})
		}
	}
}

func TestInspectionFaultConnectorPositiveControlAndCountWidth(t *testing.T) {
	state := &inspectionFaultState{failStage: -1, largeCount: true}
	db := sql.OpenDB(inspectionFaultConnector{state})
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("fixture connection close: %v", err)
		}
	})
	data, err := loadInspection(db)
	wantCount := (int64(1) << 31) + 17
	if err != nil || data == nil || data.total != wantCount || data.applied != wantCount || data.rejected != 0 || data.unknown != 0 || len(data.outcomes) != 1 || len(data.appliedSamples) != 1 || len(data.distance1Samples) != 1 || len(data.highScoreSamples) != 1 || state.queries.Load() != 4 {
		t.Fatalf("four-selection positive/count-width witness: data%+v err=%v queries%d", data, err, state.queries.Load())
	}
	if data.appliedSamples[0].subject != "APPLIED_GOOD" || data.distance1Samples[0].reports != 3 || data.distance1Samples[0].reason.Valid || data.highScoreSamples[0].score != 71 || !data.highScoreSamples[0].reason.Valid || data.highScoreSamples[0].reason.String != "" {
		t.Errorf("positive selected-field witnesses: %+v", data)
	}
	for i := range 4 {
		if state.nextCalls[i].Load() != 2 || state.rowsClosed[i].Load() != 1 {
			t.Errorf("positive stage%d next%d/close%d", i+1, state.nextCalls[i].Load(), state.rowsClosed[i].Load())
		}
	}
	if err := db.Close(); err != nil || state.connClosed.Load() != 1 {
		t.Fatalf("positive connection retirement: close%d err=%v", state.connClosed.Load(), err)
	}
}
