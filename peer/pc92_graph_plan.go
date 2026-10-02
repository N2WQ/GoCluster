package peer

import (
	"fmt"
	"iter"
)

// graphPlan owns only input-sized scratch. C scans the unchanged subject twice:
// prepare computes removal effects; commit deletes only the yielded missing key,
// then inserts after traversal. No population-sized removal list is retained.
type graphPlan struct {
	record   *PC92Record
	subject  string
	addNodes *boundedIndex[string, PC92Entry]
	desired  *boundedIndex[memberKey, plannedMember]
	external bool
	removed  int
	add      []PC92Entry
}

type plannedMember struct {
	entry PC92Entry
	// Receiver C processes all entries for a call when either kind is new or
	// repeated in the input. This is distinct from membership-set replacement.
	changed bool
}

// effectiveNodeSubject keeps K's numeric replacement separate from A/C/D's
// preserve-absent merge. The decoder represents both omitted and zero numeric
// fields as empty strings; DXSpider's K handler writes zero in either case.
// Only node authority and its allocation preflight use this view. The decoded
// record, distinct origin and relationship-edge metadata retain their wire form.
func effectiveNodeSubject(record *PC92Record) PC92Entry {
	entry := record.Subject
	if record.Action == "K" {
		if entry.Version == "" {
			entry.Version = "0"
		}
		if entry.Build == "" {
			entry.Build = "0"
		}
	}
	return entry
}

// Only C traverses the current population. D is bounded by its input and A/K
// have no removal work. Commit may delete the yielded existing key, but cannot
// insert or compact while this iterator is active.
func (p *graphPlan) removals(existing *boundedIndex[memberKey, PC92Entry]) iter.Seq2[memberKey, PC92Entry] {
	return func(yield func(memberKey, PC92Entry) bool) {
		switch p.record.Action {
		case "C":
			for key, entry := range existing.All() {
				if _, wanted := p.desired.Get(key); !wanted && !yield(key, entry) {
					return
				}
			}
		case "D":
			for key := range p.desired.All() {
				if entry, exists := existing.Get(key); exists && !yield(key, entry) {
					return
				}
			}
		}
	}
}

func (g *protocolGraph) prepare(r *PC92Record, local, ingress string, direct *boundedIndex[string, bool]) (*graphPlan, error) {
	subject := r.Subject.Call
	if subject == "" {
		subject = r.Origin
	}
	if r.Origin == local || subject == local || (direct.Value(subject) && subject != ingress) {
		return nil, nil //nolint:nilnil // Protected authority intentionally has no mutation plan.
	}
	if r.Action == "D" && g.nodes.Value(r.Origin) == nil {
		return nil, nil //nolint:nilnil // Unknown origins have no withdrawal authority.
	}
	p := &graphPlan{record: r, subject: subject, addNodes: newBoundedIndex[string, PC92Entry](maxGraphNodes - g.nodes.Len()), desired: newBoundedIndex[memberKey, plannedMember](len(r.Members))}
	addNode := func(call string, entry PC92Entry) bool {
		if g.nodes.Value(call) != nil {
			return true
		}
		if _, exists := p.addNodes.Get(call); !exists && g.nodes.Len()+p.addNodes.Len() >= maxGraphNodes {
			return false
		}
		p.addNodes.Set(call, entry)
		return true
	}
	subjectEntry := effectiveNodeSubject(r)
	subjectEntry.Call = subject
	if !addNode(r.Origin, PC92Entry{Call: r.Origin, Flags: 5}) || !addNode(subject, subjectEntry) {
		return nil, fmt.Errorf("node capacity")
	}
	var existing *boundedIndex[memberKey, PC92Entry]
	if node := g.nodes.Value(subject); node != nil {
		existing = node.Members
	}
	for _, e := range r.Members {
		if e.Call == local || e.Call == subject || (direct.Value(e.Call) && !e.IsNode()) {
			continue
		}
		key := membershipKey(e)
		prior, repeated := p.desired.Get(key)
		_, exists := existing.Get(key)
		if repeated {
			if e.IsNode() {
				// Only the first addition can construct the receiver's node.
				// Later occurrences update explicit IP, not its numeric version.
				e.Version = prior.entry.Version
			}
			e = entryWithMetadata(prior.entry, e)
		}
		p.desired.Set(key, plannedMember{e, repeated || !exists || prior.changed})
	}
	if r.Action != "D" && r.Action != "K" {
		p.add = make([]PC92Entry, 0, p.desired.Len())
		for key, entry := range p.desired.All() {
			other := p.desired.Value(memberKey{key.Call, !key.Node})
			if r.Action == "C" && !entry.changed && !other.changed {
				continue
			}
			incoming := g.memberEntry(entry.entry)
			p.add = append(p.add, incoming)
			if incoming.IsNode() && !addNode(key.Call, incoming) {
				return nil, fmt.Errorf("node capacity")
			}
		}
	}
	newUsers := g.users.Len()
	for key, e := range p.removals(existing) {
		p.removed++
		if !e.IsNode() && g.users.Value(key.Call) == 1 {
			newUsers--
		}
	}
	edges := g.edges - p.removed
	if subject != r.Origin && r.Subject.IsExternal() {
		origin := g.nodes.Value(r.Origin)
		p.external = origin == nil
		if origin != nil {
			_, exists := origin.Members.Get(memberKey{subject, true})
			p.external = !exists
		}
		if p.external {
			edges++
		}
	}
	for _, e := range p.add {
		if _, exists := existing.Get(membershipKey(e)); !exists {
			edges++
			if !e.IsNode() && g.users.Value(e.Call) == 0 {
				newUsers++
			}
		}
	}
	if edges > maxGraphEdges || newUsers > maxGraphUsers {
		return nil, fmt.Errorf("membership capacity")
	}
	if g.projectedCharge(p) > 96<<20 {
		return nil, fmt.Errorf("graph retained-byte capacity")
	}
	return p, nil
}

// Route::{User,Node} constructors default Here to true even for a clear input
// bit, and adding an existing member does not update its Here flag. Within this
// PC92 authority graph only an explicit node subject can subsequently change
// node Here. Node::add also leaves an existing version/build alone: a new node
// takes its first version (or5401), while build is established by its explicit
// subject publication, not a member entry. Legacy PC24 authority is outside the
// selected bridge profile.
func (g *protocolGraph) memberEntry(entry PC92Entry) PC92Entry {
	here := true
	if entry.IsNode() {
		if existing := g.nodes.Value(entry.Call); existing != nil {
			here = existing.Entry.Here()
			entry.Version, entry.Build = existing.Entry.Version, existing.Entry.Build
		} else {
			if entry.Version == "" || entry.Version == "0" {
				entry.Version = "5401"
			}
			entry.Build = ""
		}
	} else {
		// Numeric slots are accepted by the shared wire decoder but are not
		// attributes of a receiver Route::User. Do not retain ignored metadata.
		entry.Version, entry.Build = "", ""
	}
	entry.Flags &^= 1
	if here {
		entry.Flags |= 1
	}
	return entry
}
