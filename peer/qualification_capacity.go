//go:build qualification

package peer

import (
	"fmt"
	"strings"
	"time"
)

// QualificationStagingPlan describes wire input to authenticated candidates.
// It has no access to manager state: the caller must use ordinary sockets and
// independently observe ownership, admission, cleanup and the authority oracle.
type QualificationStagingPlan struct {
	Name                           string
	Records                        [128]int
	WireBytes                      int
	TailBytes                      int
	ExpectedRecords, ExpectedBytes int
	OverflowCandidate              int
	CompleteRace, AwaitDeadline    bool
}

// QualificationStagingPlans uses allocator-boundary oracles rather than
// deriving expected totals from the production accounting helper. Go1.26
// allocates the fixed256-string candidate backing in the4864-byte class.
func QualificationStagingPlans() []QualificationStagingPlan {
	plans := []QualificationStagingPlan{
		{Name: "global-records", WireBytes: 128, ExpectedRecords: 8192, ExpectedBytes: 32 * (4864 + 256*128), OverflowCandidate: 32},
		{Name: "global-bytes", WireBytes: 2016, ExpectedRecords: 7888, ExpectedBytes: 16 << 20, OverflowCandidate: 127},
		{Name: "individual-records", WireBytes: 128, ExpectedRecords: 256, ExpectedBytes: 4864 + 256*128, OverflowCandidate: 0},
		{Name: "individual-bytes", WireBytes: 2016, TailBytes: 1280, ExpectedRecords: 254, ExpectedBytes: 512 << 10, OverflowCandidate: 0},
		{Name: "deadline", WireBytes: 128, ExpectedRecords: 128, ExpectedBytes: 128 * (4864 + 128), OverflowCandidate: -1, AwaitDeadline: true},
		{Name: "winner-race", WireBytes: 128, ExpectedRecords: 128, ExpectedBytes: 128 * (4864 + 128), OverflowCandidate: -1, CompleteRace: true},
	}
	for i := range 32 {
		plans[0].Records[i] = 256
	}
	for i := range 128 {
		plans[1].Records[i] = 61
		if i < 80 {
			plans[1].Records[i]++
		}
		plans[4].Records[i] = 1
		plans[5].Records[i] = 1
	}
	plans[2].Records[0], plans[3].Records[0] = 256, 253
	return plans
}

// QualificationStagingWire is a valid K for an existing topology-fixture
// origin. Candidate copies must remain private until exactly one owner wins.
// Numeric padding exercises wire/owned-byte limits without changing grammar.
func QualificationStagingWire(now time.Time, size int) (string, error) {
	stamp := qualificationStamp(now)
	prefix := "PC92^N0AAAA^" + stamp + "^K^5N0AAAA:"
	suffix := "^1^0^H1^"
	padding := size - len(prefix) - len(suffix)
	if padding < 1 || size > MaxPeerFrameBytes {
		return "", fmt.Errorf("invalid staging wire size%d", size)
	}
	wire := prefix + strings.Repeat("1", padding) + suffix
	f, err := ParseFrame(wire)
	if err != nil {
		return "", err
	}
	if _, err := DecodePC92(f); err != nil {
		return "", err
	}
	return wire, nil
}

// QualificationCapacityChecks separates actual simultaneous observations from
// conservative limits. Maxima are reported, never relabelled as heap bytes.
type QualificationCapacityChecks struct {
	Samples                                                                      int
	MaxOwned, MaxOwnerReservations, MaxPending, MaxStagedRecords, MaxStagedBytes int
	MaxControlBytes, MaxDataBytes, MaxActiveBytes                                int
	MaxReaderBackingBytes, MaxReaderRawLineBytes, MaxParseBytes                  int64
	MaxSpotKeys, MaxPC92Keys, MaxPC93Keys, MaxBulletinKeys                       int
	MaxGraphChargedBytes                                                         int
}

func (c *QualificationCapacityChecks) Observe(s QualificationState) error {
	c.Samples++
	c.MaxOwned = max(c.MaxOwned, s.OwnedSessions)
	c.MaxOwnerReservations = max(c.MaxOwnerReservations, s.OwnerReservations)
	c.MaxPending = max(c.MaxPending, s.Pending)
	c.MaxStagedRecords = max(c.MaxStagedRecords, s.StagedRecords)
	c.MaxStagedBytes = max(c.MaxStagedBytes, s.StagedBytes)
	c.MaxControlBytes = max(c.MaxControlBytes, s.ControlBytes)
	c.MaxDataBytes = max(c.MaxDataBytes, s.DataBytes)
	c.MaxActiveBytes = max(c.MaxActiveBytes, s.ActiveBytes)
	c.MaxReaderBackingBytes = max(c.MaxReaderBackingBytes, s.ReaderBackingBytes)
	c.MaxReaderRawLineBytes = max(c.MaxReaderRawLineBytes, s.ReaderRawLineBytes)
	c.MaxParseBytes = max(c.MaxParseBytes, s.ParseScratchBytes)
	c.MaxSpotKeys = max(c.MaxSpotKeys, s.SpotKeys)
	c.MaxPC92Keys = max(c.MaxPC92Keys, s.PC92Keys)
	c.MaxPC93Keys = max(c.MaxPC93Keys, s.PC93Keys)
	c.MaxBulletinKeys = max(c.MaxBulletinKeys, s.BulletinKeys)
	c.MaxGraphChargedBytes = max(c.MaxGraphChargedBytes, s.GraphChargedBytes)
	if s.OwnedSessions > 192 || s.OwnerReservations > 192 || s.Established > 64 || s.Pending > 128 || s.StagedRecords > 8192 || s.StagedBytes > 16<<20 || s.GraphChargedBytes > 96<<20 || s.ParseScratchBytes > 8<<20 || s.ProjectionReservedBytes > 48<<20 {
		return fmt.Errorf("capacity reservation exceeded: %+v", s)
	}
	return nil
}
