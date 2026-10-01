//go:build qualification && !windows

package cluster

import (
	"fmt"
	"time"
)

type qualificationSharedInputs struct {
	inputs []qualificationInput
	bytes  int
}

func openQualificationSharedInputs(string, int, bool) (*qualificationSharedInputs, error) {
	return nil, fmt.Errorf("split runtime qualification currently requires Windows QPC and file mappings")
}

func (*qualificationSharedInputs) close() error { return nil }

func qualificationCounterClock() (func() time.Time, int64, error) {
	return nil, 0, fmt.Errorf("split runtime qualification currently requires Windows QPC and file mappings")
}
