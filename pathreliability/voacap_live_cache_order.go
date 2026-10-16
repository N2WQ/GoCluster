// File role: Defines deterministic equal-time eviction for the bounded live maps.
// The fallback mutex owns selection. Greatest typed keys lose ties; numeric
// fields compare numerically. No index, retained metadata, or allocation is added.
package pathreliability

func compareLiveCacheKey(a, b *voacapCacheKey) int {
	if a.userCell != b.userCell {
		if a.userCell < b.userCell {
			return -1
		}
		return 1
	}
	if a.dxCell != b.dxCell {
		if a.dxCell < b.dxCell {
			return -1
		}
		return 1
	}
	if a.band != b.band {
		if a.band < b.band {
			return -1
		}
		return 1
	}
	if a.frequencyKHz != b.frequencyKHz {
		if a.frequencyKHz < b.frequencyKHz {
			return -1
		}
		return 1
	}
	if a.year != b.year {
		if a.year < b.year {
			return -1
		}
		return 1
	}
	if a.month != b.month {
		if a.month < b.month {
			return -1
		}
		return 1
	}
	if a.ssn != b.ssn {
		if a.ssn < b.ssn {
			return -1
		}
		return 1
	}
	if a.direction != b.direction {
		if a.direction < b.direction {
			return -1
		}
		return 1
	}
	return 0
}

func compareLiveDelayKey(a, b *voacapDelayKey) int {
	if a.userCell != b.userCell {
		if a.userCell < b.userCell {
			return -1
		}
		return 1
	}
	if a.dxCell != b.dxCell {
		if a.dxCell < b.dxCell {
			return -1
		}
		return 1
	}
	if a.band != b.band {
		if a.band < b.band {
			return -1
		}
		return 1
	}
	if a.frequencyKHz != b.frequencyKHz {
		if a.frequencyKHz < b.frequencyKHz {
			return -1
		}
		return 1
	}
	if a.direction != b.direction {
		if a.direction < b.direction {
			return -1
		}
		return 1
	}
	return 0
}
