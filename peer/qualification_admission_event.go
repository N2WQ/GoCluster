package peer

import "time"

// qualificationAdmissionEvent carries facts from existing ownership boundaries.
// No recovery healthy-since value is exposed: qualification derives its timing
// obligation from capacity and gate transitions. Normal builds discard events.
// Strings are borrowed for the synchronous callback and have protocol bounds.
type qualificationAdmissionEvent struct {
	Kind, Call, Cause, Origin, Timestamp, Key string
	Direction                                 string
	Generation, Sequence                      uint64
	At, AdmissionAt, LogicalExpiry            time.Time
	Nodes, Users, Edges, Ingress, Freshness   int
	PC92Keys, PC92KeyBytes                    int
	InputPC92, InputPC92Bytes                 int
}
