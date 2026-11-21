// File role: Chronological pair selection and WPM agreement for live CW CQ
// observations. No radio identity, signal power or speed influences pairing.
package correctionflow

type cwBurstPair struct {
	first, last *cwBurstRow
}

func (p cwBurstPair) speedSum() (int, bool) {
	a, b := p.first.wpm, p.last.wpm
	return a + b, a > 0 && b > 0 && a <= b+2 && b <= a+2
}

// For a fixed ending row, only its latest distinct-family predecessor can
// win. It also gives the largest gap from an earlier, separate burst. If the
// immediate predecessor is from the same family, reuse the predecessor of
// that family's current run. Thus dense same-second traffic stays linear.
func (b *cwBurstBucket) pairs(dst []cwBurstPair, indexes []uint16) []cwBurstPair {
	var prev, different *cwBurstRow
	for _, i := range indexes {
		r := &b.rows[i]
		if prev != nil && prev.family != r.family {
			different = prev
		}
		if different != nil && r.at-different.at <= cwBurstWidth {
			dst = append(dst, cwBurstPair{different, r})
		}
		prev = r
	}
	return dst
}

// Fix the latest eligible later pair, then the latest eligible earlier pair.
// The timestamp gap is strictly greater than the inclusive 3-second width.
func latestCWSequence(early, late []cwBurstPair) (first, last cwBurstPair, ok bool) {
	if len(early) == 0 || len(late) == 0 {
		return
	}
	for i := len(late) - 1; i >= 0; i-- {
		endBefore := late[i].first.at - cwBurstWidth
		if early[0].last.at >= endBefore {
			continue
		}
		for j := len(early) - 1; j >= 0; j-- {
			if early[j].last.at < endBefore {
				return early[j], late[i], true
			}
		}
	}
	return
}

func cwBurstRetains(original, replacement []cwBurstPair, focalWPM int) bool {
	early, late, ok := latestCWSequence(replacement, original)
	if !ok {
		return false
	}
	_, reverse, ok := latestCWSequence(original, replacement)
	if ok && (reverse.last.at > late.last.at || (reverse.last.at == late.last.at && reverse.last.order > late.last.order)) {
		return false
	}
	other, otherOK := early.speedSum()
	subject, subjectOK := late.speedSum()
	// Each sum is twice its pair mean. Use integer half-WPM units throughout.
	gap := max(other, subject) - min(other, subject)
	incoming := 2 * focalWPM
	subjectError := max(subject, incoming) - min(subject, incoming)
	otherError := max(other, incoming) - min(other, incoming)
	return otherOK && subjectOK && gap >= 8 && subjectError <= 4 && subjectError+4 <= otherError
}
