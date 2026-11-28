// File role: Bound growth and reclaim receiver-map backing for live CW keys.
package spot

const (
	// CWResolverReporterLimit is the hard limit for one raw CW callsign.
	CWResolverReporterLimit = 256
	// CWResolverExtraRowsPerKey is shared by memberships above each candidate's
	// base. The live 16-candidate key therefore holds at most 16*64+256 rows.
	CWResolverExtraRowsPerKey = 256
)

func (st *resolverKeyState) reporterAtCapacity(inCandidate, baseLimit int) bool {
	if inCandidate < baseLimit {
		return false
	}
	return st.cwReporterBase == 0 || inCandidate >= CWResolverReporterLimit || st.cwExtraRows >= CWResolverExtraRowsPerKey
}

func (st *resolverKeyState) compactReporterRefs() {
	if st.cwReporterBase > 0 {
		st.reporterRefs = compactResolverReporterMap(st.reporterRefs, &st.reporterRefsPeak, st.cwReporterBase)
	}
}

// Go maps do not shrink on delete. Rebuild at most once per owner prune pass,
// after a population grown beyond the base drops by half. The owner can grant
// freed rows immediately, and old backing never escapes the owner to readers.
func compactResolverReporterMap[V any](entries map[string]V, peak *int, base int) map[string]V {
	count := len(entries)
	if *peak <= base || count > *peak/2 {
		return entries
	}
	*peak = count
	if count == 0 {
		return nil
	}
	compacted := make(map[string]V, count)
	for receiver, value := range entries {
		compacted[receiver] = value
	}
	return compacted
}
