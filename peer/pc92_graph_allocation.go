package peer

// metadataMutationCharge follows commit's ownership order. A replacement must
// reserve the newly cloned strings while the previous entry is still retained;
// final metadata length alone would miss that overlap at a full graph budget.
func (g *protocolGraph) metadataMutationCharge(plan *graphPlan) (peak, final int) {
	final = g.metadataBytes
	for _, entry := range plan.addNodes.All() {
		final += entryBytes(entry)
	}
	peak = final
	retiredCalls := 0
	plannedEntry := func(call string) PC92Entry {
		if n := g.nodes.Value(call); n != nil {
			return n.Entry
		}
		return plan.addNodes.Value(call)
	}
	replace := func(old, incoming PC92Entry) {
		next := entryWithMetadata(old, incoming)
		cloned := 0
		for _, field := range []struct{ old, next string }{
			{old.Call, next.Call}, {old.Version, next.Version}, {old.Build, next.Build},
		} {
			if field.old != field.next {
				cloned += allocationBytes(len(field.next))
			}
		}
		peak = max(peak, final+retiredCalls+cloned)
		final += entryBytes(next) - entryBytes(old)
	}
	r := plan.record
	if !r.SubjectImplicit {
		replace(plannedEntry(r.Subject.Call), r.Subject)
	}
	if plan.external {
		final += entryBytes(r.Subject)
		peak = max(peak, final)
	}
	var existing *boundedIndex[string, PC92Entry]
	if n := g.nodes.Value(r.Subject.Call); n != nil {
		existing = n.Members
	}
	for _, call := range plan.remove {
		final -= entryBytes(existing.Value(call))
		// The plan's remove slice still aliases this owned call after both the
		// membership and last-user index entries have been deleted. Keep that
		// backing alive in the peak until the complete transaction returns.
		retiredCalls += allocationBytes(len(call))
	}
	for _, entry := range plan.add {
		replace(existing.Value(entry.Call), entry)
		if entry.IsNode() {
			replace(plannedEntry(entry.Call), entry)
		}
	}
	return peak, final
}

// Index bucket storage is determined by occupancy, never by hash distribution.
// Retain the high-water reservation after deletion, and compact only outside
// iteration at quarter occupancy. The old/new buckets remain charged until
// Compact returns; entries are relinked without allocating another generation.
func (g *protocolGraph) putMember(n *graphNode, entry PC92Entry) {
	if n.Members == nil {
		n.Members = newBoundedIndex[string, PC92Entry](maxGraphEdges)
	}
	n.Members.Set(entry.Call, entry)
	if size := n.Members.Len(); size > n.memberHigh {
		g.memberHigh += size - n.memberHigh
		n.memberHigh = size
	}
}

func (g *protocolGraph) compactMembers(n *graphNode) {
	previous := n.memberHigh
	compactGraphIndex(n.Members, &n.memberHigh)
	g.memberHigh += n.memberHigh - previous
}

func compactGraphIndex[K comparable, V any](current *boundedIndex[K, V], high *int) {
	size := current.Len()
	*high = max(*high, size)
	if size == 0 {
		current.Compact()
		*high = 0
		return
	}
	if *high < 32 || size > *high/4 {
		return
	}
	current.Compact()
	*high = size
}

func (g *protocolGraph) compactIndexes() {
	compactGraphIndex(g.nodes, &g.nodesHigh)
	compactGraphIndex(g.users, &g.usersHigh)
	compactGraphIndex(g.ingress, &g.ingressHigh)
	compactGraphIndex(g.freshness, &g.freshnessHigh)
}
