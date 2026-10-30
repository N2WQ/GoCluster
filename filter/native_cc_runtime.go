// File role: Adapts admitted native CC programs to the common delivery snapshot.
// Immutable criteria are shared; six controls are detached for caller-owned
// mutation. Live/history own-call exceptions remain with the delivery owners.
package filter

// NewCCFilter prepares native criteria without interpreting public GO rules.
// The returned canonical definitions and runtime controls share no mutable state.
func NewCCFilter(configuration CCConfiguration) (CCConfiguration, Filter, error) {
	canonical, plan, err := PrepareCCConfiguration(configuration)
	if err != nil {
		return CCConfiguration{}, Filter{}, err
	}
	return canonical, ccFilterFromPrepared(plan, canonical), nil
}

func ccFilterFromPrepared(plan *PreparedCCFilter, controls CCConfiguration) Filter {
	return Filter{
		preparedCC:     plan,
		IncludeBeacons: controls.IncludeBeacons.Pointer(), AllowWWV: controls.AllowWWV.Pointer(),
		AllowWCY: controls.AllowWCY.Pointer(), AllowAnnounce: controls.AllowAnnounce.Pointer(),
		AllowSelf: controls.AllowSelf.Pointer(), AllowToxic: controls.AllowToxic.Pointer(),
	}
}

// IsCC distinguishes program ownership without interpreting a mutable selector.
func (f *Filter) IsCC() bool { return f != nil && f.preparedCC != nil }

// NativeCCConfiguration captures current controls as well as immutable criteria.
// The caller synchronizes controls; the returned DTO owns all mutable collections.
func (f *Filter) NativeCCConfiguration() (CCConfiguration, bool) {
	if !f.IsCC() {
		return CCConfiguration{}, false
	}
	c := f.preparedCC.CanonicalConfig()
	c.IncludeBeacons = DefaultBoolFromPointer(f.IncludeBeacons)
	c.AllowWWV = DefaultBoolFromPointer(f.AllowWWV)
	c.AllowWCY = DefaultBoolFromPointer(f.AllowWCY)
	c.AllowAnnounce = DefaultBoolFromPointer(f.AllowAnnounce)
	c.AllowSelf = DefaultBoolFromPointer(f.AllowSelf)
	c.AllowToxic = DefaultBoolFromPointer(f.AllowToxic)
	return c, true
}

// nativeCC borrows only immutable admitted collections. Callers must not mutate
// the returned collections; captured controls replace the plan's original ones.
func (f FilterConfiguration) nativeCC() CCConfiguration {
	c := f.preparedCC.configuration
	c.IncludeBeacons, c.AllowWWV, c.AllowWCY = f.IncludeBeacons, f.AllowWWV, f.AllowWCY
	c.AllowAnnounce, c.AllowSelf, c.AllowToxic = f.AllowAnnounce, f.AllowSelf, f.AllowToxic
	return c
}
