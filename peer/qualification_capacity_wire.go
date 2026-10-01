//go:build qualification

package peer

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// QualificationCapacityGenerator owns only driver timestamps/sequence state.
// Every returned record still enters ordinary authenticated peer socket paths.
type QualificationCapacityGenerator struct {
	topology *QualificationTopology
	messages [4096]TimestampGenerator
}

func NewQualificationCapacityGenerator(topology *QualificationTopology) *QualificationCapacityGenerator {
	return &QualificationCapacityGenerator{topology: topology}
}

// Wire uses existing graph/message origins so capacity pressure cannot invent
// additional authority slots. Size is a wire target for PC26/PC92, an exact
// canonical-key target for PC93/bulletins, or zero for their ordinary fixture.
func (g *QualificationCapacityGenerator) Wire(class string, index, size, hop int) (string, error) {
	now := g.topology.Now()
	switch class {
	case "spot":
		call := qualificationCall("K0", index%(26*26*26*26)) + "ABCDEFGHI"
		// Ordinary spot construction multiplies by100 while rounding. Keep
		// that intermediate finite so the retained key contains all digits.
		prefix := "PC26^" + strconv.FormatFloat(math.MaxFloat64/128, 'g', -1, 64) + "^" + call + "^" + time.Now().UTC().Format("02-Jan-2006^1504Z") + "^"
		suffix := fmt.Sprintf("^W1ABCDEFGHIJKLM^N1REM^^H%d^", hop)
		padding := max(1, size-len(prefix)-len(suffix))
		return prefix + strings.Repeat("x", padding) + suffix, nil
	case "pc92":
		g.topology.mu.Lock()
		stamp, err := g.topology.stamps[index%len(g.topology.stamps)].NextAt(now)
		g.topology.mu.Unlock()
		if err != nil {
			return "", err
		}
		origin := qualificationCall("N0", index%4096)
		prefix := fmt.Sprintf("PC92^%s^%s^K^5%s^0^0^^", origin, stamp, origin)
		suffix := fmt.Sprintf("^H%d^", hop)
		padding := max(1, size-len(prefix)-len(suffix))
		return prefix + strings.Repeat("x", padding) + suffix, nil
	case "pc93":
		stamp, err := g.messages[index%len(g.messages)].NextAt(now)
		if err != nil {
			return "", err
		}
		origin := qualificationCall("M0", index%4096)
		prefix := fmt.Sprintf("PC93^%s^%s^K9ABSENT^DL1AAA^*^", origin, stamp)
		suffix := fmt.Sprintf("^H%d^", hop)
		wire := prefix + "x" + suffix
		frame, err := ParseFrame(wire)
		if err != nil {
			return "", err
		}
		padding := max(1, size-len(pc93Key(frame))+1)
		return prefix + strings.Repeat("x", padding) + suffix, nil
	case "bulletin":
		// Empty logger/origin is accepted by the existing bulletin parser and
		// suppresses display formatting; its canonical payload still occupies
		// the normal cache. Delivery cases are qualified separately in Q1-Q5.
		prefix := fmt.Sprintf("PC23^%s^%02d^100^5^2^%08d", now.Format("02-Jan-2006"), now.Hour(), index)
		suffix := fmt.Sprintf("^^^H%d^", hop)
		frame, err := ParseFrame(prefix + suffix)
		if err != nil {
			return "", err
		}
		padding := max(0, size-len(wwvKey(frame)))
		return prefix + strings.Repeat("x", padding) + suffix, nil
	default:
		return "", fmt.Errorf("unknown qualification capacity class %q", class)
	}
}
