package peer

import (
	"strings"
	"time"
)

const (
	maxGraphNodes          = 4096
	maxGraphUsers          = 65536
	maxGraphEdges          = 131072
	maxIngressObservations = 262144
	maxFreshnessOrigins    = 16384
	maxMessageOrigins      = 4096
	// The controller owns at most one decoded record and transactional plan.
	// Keep that temporary storage inside the graph's 96 MiB partition rather
	// than treating the reader semaphore as a second, implicit allowance.
	graphMutationScratchBytes = 5 << 20
)

type graphNode struct {
	memberHigh   int
	Entry        PC92Entry
	Members      *boundedIndex[memberKey, PC92Entry]
	Complete     bool
	Observations int
	Seen         time.Time
}

// Node and user routes are independent relationships in the receiver, even
// when their canonical callsigns coincide. Here/external flags remain metadata.
type memberKey struct {
	Call string
	Node bool
}

func membershipKey(e PC92Entry) memberKey { return memberKey{e.Call, e.IsNode()} }

type ingressKey struct{ Origin, Ingress string }
type ingressObservation struct {
	Seen time.Time
	Hop  int
}
type originWatermark struct {
	Value       float64
	Accepted    time.Time
	MessageOnly bool
}

// protocolGraph is exclusively owned by the manager controller. Membership,
// ingress and replay protection have independent bounds: deleting an edge never
// deletes a still-valid watermark. Persistence is only a copy of this state.
type protocolGraph struct {
	memberHigh, nodesHigh, usersHigh, ingressHigh, freshnessHigh int
	ingressBytes                                                 int
	metadataBytes                                                int
	nodes                                                        *boundedIndex[string, *graphNode]
	users                                                        *boundedIndex[string, int]
	ingress                                                      *boundedIndex[ingressKey, ingressObservation]
	freshness                                                    *boundedIndex[string, originWatermark]
	edges, messageOrigins                                        int
	nextObservation                                              time.Time
}

func newProtocolGraph(now time.Time) *protocolGraph {
	return &protocolGraph{nodes: newBoundedIndex[string, *graphNode](maxGraphNodes), users: newBoundedIndex[string, int](maxGraphUsers),
		ingress: newBoundedIndex[ingressKey, ingressObservation](maxIngressObservations), freshness: newBoundedIndex[string, originWatermark](maxFreshnessOrigins),
		nextObservation: now.Add(time.Hour)}
}

// freshTime follows the reference's strict 900-second UTC window and asymmetric
// midnight ordering rule. It does not mutate; admission commits the watermark
// only after every graph/cache resource has been reserved successfully.
func freshTime(value float64, now time.Time, old originWatermark, exists bool) bool {
	utc := now.UTC()
	sec := float64(utc.Hour()*3600 + utc.Minute()*60 + utc.Second())
	diff := sec - value
	if diff < 0 {
		diff = -diff
	}
	if value < 0 || value >= 86400 || !(diff < 900 || 86400-diff < 900) {
		return false
	}
	if !exists {
		return true
	}
	if value == old.Value {
		return false
	}
	if value < old.Value {
		return value+86400-old.Value <= 8220
	}
	return old.Value+86400-value >= 8220
}
func (g *protocolGraph) canWatermark(origin string, message bool) bool {
	if _, ok := g.freshness.Get(origin); ok {
		return true
	}
	return g.freshness.Len() < maxFreshnessOrigins && (!message || g.messageOrigins < maxMessageOrigins)
}
func (g *protocolGraph) commitWatermark(origin string, value float64, now time.Time, message bool) {
	old, ok := g.freshness.Get(origin)
	if ok && old.MessageOnly && !message {
		g.messageOrigins--
	}
	if !ok && message {
		g.messageOrigins++
	}
	g.freshness.Set(strings.Clone(origin), originWatermark{value, now, message && (!ok || old.MessageOnly)})
	g.freshnessHigh = max(g.freshnessHigh, g.freshness.Len())
}
func (g *protocolGraph) canObserve(origin, ingress string) bool {
	if ingress == "" {
		return true
	}
	_, ok := g.ingress.Get(ingressKey{origin, ingress})
	return ok || g.ingress.Len() < maxIngressObservations
}
func (g *protocolGraph) observe(origin, ingress string, hop int, now time.Time, duplicate bool) {
	if ingress == "" {
		return
	}
	key := ingressKey{origin, ingress}
	if _, ok := g.ingress.Get(key); ok && duplicate {
		return
	}
	if g.canObserve(origin, ingress) {
		if _, ok := g.ingress.Get(key); !ok {
			g.ingressBytes += ingressEntryBytes(origin, ingress)
		}
		key = ingressKey{strings.Clone(origin), strings.Clone(ingress)}
		g.ingress.Set(key, ingressObservation{now, hop})
		g.ingressHigh = max(g.ingressHigh, g.ingress.Len())
	}
}

func entryWithMetadata(old, entry PC92Entry) PC92Entry {
	if entry.Version == "" {
		entry.Version = old.Version
	}
	if entry.Build == "" {
		entry.Build = old.Build
	}
	if !entry.IP.IsValid() {
		entry.IP = old.IP
	}
	return entry
}

func mergeEntry(old, entry PC92Entry) PC92Entry {
	entry = entryWithMetadata(old, entry)
	// Preserve already-owned unchanged strings. Apart from reducing churn,
	// this avoids manufacturing overlapping copies of large absent metadata.
	for _, field := range []struct {
		next *string
		old  string
	}{
		{&entry.Call, old.Call}, {&entry.Version, old.Version}, {&entry.Build, old.Build},
	} {
		if *field.next == field.old {
			*field.next = field.old
		} else {
			*field.next = strings.Clone(*field.next)
		}
	}
	return entry
}
func (g *protocolGraph) commit(p *graphPlan, now time.Time) {
	r := p.record
	for call, e := range p.addNodes.All() {
		call = strings.Clone(call)
		e = mergeEntry(PC92Entry{}, e)
		g.metadataBytes += entryBytes(e)
		g.nodes.Set(call, &graphNode{Entry: e, Observations: 3, Seen: now})
	}
	n := g.nodes.Value(p.subject)
	if !r.SubjectImplicit {
		g.setNodeEntry(n, r.Subject)
	}
	if p.external {
		parent := g.nodes.Value(r.Origin)
		g.putMember(parent, mergeEntry(PC92Entry{}, r.Subject))
		g.metadataBytes += entryBytes(r.Subject)
		g.edges++
	}
	for key := range p.removals(n.Members) {
		g.removeEdge(n, key)
	}
	for _, e := range p.add {
		incoming := e
		old, exists := n.Members.Get(membershipKey(e))
		e = mergeEntry(old, e)
		if !exists {
			g.edges++
			if !e.IsNode() {
				g.users.Set(e.Call, g.users.Value(e.Call)+1)
			}
		}
		g.metadataBytes += entryBytes(e) - entryBytes(old)
		g.putMember(n, e)
		if e.IsNode() {
			child := g.nodes.Value(e.Call)
			// An omitted IP cannot reassert an older address borrowed from this
			// parent edge over a newer address learned through another parent.
			g.setNodeEntry(child, incoming)
		}
	}
	if r.Action == "C" || r.Action == "K" {
		n.Observations = 3
		n.Seen = now
	}
	if r.Action == "C" {
		n.Complete = true
	}
	g.compactMembers(n)
	g.compactIndexes()
}
func (g *protocolGraph) removeEdge(n *graphNode, key memberKey) {
	e, ok := n.Members.Get(key)
	if !ok {
		return
	}
	g.metadataBytes -= entryBytes(e)
	n.Members.Delete(key)
	g.edges--
	if !e.IsNode() {
		g.users.Set(key.Call, g.users.Value(key.Call)-1)
		if g.users.Value(key.Call) == 0 {
			g.users.Delete(key.Call)
		}
	}
}
func (g *protocolGraph) loseIngress(ingress string) {
	for key := range g.ingress.All() {
		if key.Ingress == ingress {
			if n := g.nodes.Value(key.Origin); n != nil {
				n.Complete = false
			}
			g.ingressBytes -= ingressEntryBytes(key.Origin, key.Ingress)
			g.ingress.Delete(key)
		}
	}
	g.compactIndexes()
}
func (g *protocolGraph) expire(now time.Time, clockSafe bool, direct *boundedIndex[string, bool]) {
	if !now.Before(g.nextObservation) {
		steps := int(now.Sub(g.nextObservation)/time.Hour) + 1
		g.nextObservation = g.nextObservation.Add(time.Duration(steps) * time.Hour)
		expired := newBoundedIndex[string, bool](g.nodes.Len())
		for call, n := range g.nodes.All() {
			if direct.Value(call) {
				continue
			}
			n.Observations -= steps
			if n.Observations <= 0 {
				expired.Set(call, true)
			}
		}
		for _, n := range g.nodes.All() {
			for call, e := range n.Members.All() {
				if e.IsNode() && expired.Value(call.Call) {
					g.removeEdge(n, call)
				}
			}
			g.compactMembers(n)
		}
		for call := range expired.All() {
			n := g.nodes.Value(call)
			for member := range n.Members.All() {
				g.removeEdge(n, member)
			}
			g.compactMembers(n)
			g.metadataBytes -= entryBytes(n.Entry)
			g.nodes.Delete(call)
		}
		for key := range g.ingress.All() {
			if expired.Value(key.Origin) {
				g.ingressBytes -= ingressEntryBytes(key.Origin, key.Ingress)
				g.ingress.Delete(key)
			}
		}
	}
	if !clockSafe {
		g.compactIndexes()
		return
	}
	for call, wm := range g.freshness.All() {
		if g.nodes.Value(call) == nil && now.Sub(wm.Accepted) > 1800*time.Second {
			if wm.MessageOnly {
				g.messageOrigins--
			}
			g.freshness.Delete(call)
		}
	}
	g.compactIndexes()
}

// Index charges include entries, bucket backing and old/new bucket overlap.
// Explicit compaction retains the old high-water charge until relinking ends.
// Variable strings are charged separately and cannot retain an input frame.
func entryBytes(e PC92Entry) int {
	return allocationBytes(len(e.Call)) + allocationBytes(len(e.Version)) + allocationBytes(len(e.Build))
}
func (g *protocolGraph) setNodeEntry(n *graphNode, e PC92Entry) {
	next := mergeEntry(n.Entry, e)
	g.metadataBytes += entryBytes(next) - entryBytes(n.Entry)
	n.Entry = next
}
func (g *protocolGraph) retainedCharge() int {
	return graphMutationScratchBytes + max(g.nodesHigh, g.nodes.Len())*512 + max(g.usersHigh, g.users.Len())*132 +
		max(g.memberHigh, g.edges)*256 + max(g.ingressHigh, g.ingress.Len())*160 +
		g.ingressBytes + max(g.freshnessHigh, g.freshness.Len())*196 + g.metadataBytes
}
func (g *protocolGraph) projectedCharge(p *graphPlan) int {
	charge := g.retainedCharge()
	charge += max(0, g.nodes.Len()+p.addNodes.Len()-max(g.nodesHigh, g.nodes.Len())) * 512
	for _, e := range p.addNodes.All() {
		charge += entryBytes(e)
	}
	r := p.record
	n := g.nodes.Value(p.subject)
	var existing *boundedIndex[memberKey, PC92Entry]
	if n != nil {
		existing = n.Members
		if !r.SubjectImplicit {
			charge += entryBytes(entryWithMetadata(n.Entry, r.Subject)) - entryBytes(n.Entry)
		}
	}
	newMembers, newUsers := existing.Len()-p.removed, g.users.Len()
	for key, e := range p.removals(existing) {
		charge -= entryBytes(e)
		if !e.IsNode() && g.users.Value(key.Call) == 1 {
			newUsers--
		}
	}
	for _, e := range p.add {
		old, ok := existing.Get(membershipKey(e))
		if !ok {
			newMembers++
			if !e.IsNode() && g.users.Value(e.Call) == 0 {
				newUsers++
			}
		}
		charge += entryBytes(entryWithMetadata(old, e)) - entryBytes(old)
		if e.IsNode() {
			if child := g.nodes.Value(e.Call); child != nil {
				charge += entryBytes(entryWithMetadata(child.Entry, e)) - entryBytes(child.Entry)
			}
		}
	}
	oldMembers := 0
	if n != nil {
		oldMembers = max(n.memberHigh, existing.Len())
	}
	charge += max(0, newMembers-oldMembers) * 256
	charge += max(0, newUsers-max(g.usersHigh, g.users.Len())) * 132
	if p.external {
		parent := g.nodes.Value(r.Origin)
		if parent == nil || parent.Members.Len() == max(parent.memberHigh, parent.Members.Len()) {
			charge += 256
		}
		charge += entryBytes(r.Subject)
	}
	// Two fresh origins and two ingress observations may be needed for an
	// external subject. Reserve them before committing any graph mutation.
	peakMetadata, finalMetadata := g.metadataMutationCharge(p)
	return charge + peakMetadata - finalMetadata
}
