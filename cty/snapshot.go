package cty

// SnapshotIdentity identifies one immutable CTY database publication. It contains
// no reference to the database or its data. Nonzero size ensures distinct live
// tokens cannot share the address permitted for zero-size Go allocations.
type SnapshotIdentity struct {
	_ byte
}

// SnapshotIdentity returns one stable token, including for directly constructed
// databases. Lazy atomic initialization preserves concurrent read-only lookup;
// losing first-use allocations are ordinary garbage, not a generation registry.
func (db *CTYDatabase) SnapshotIdentity() *SnapshotIdentity {
	if db == nil {
		return nil
	}
	if identity := db.identity.Load(); identity != nil {
		return identity
	}
	identity := new(SnapshotIdentity)
	if db.identity.CompareAndSwap(nil, identity) {
		return identity
	}
	return db.identity.Load()
}
