// File role: Stages complete both-bank human updates under the existing lease.
// Admission precedes publication. A late disk failure preserves both unsaved
// banks in memory; a later complete save can persist all pending changes.
package telnet

import (
	"errors"
	"fmt"

	"dxcluster/filter"
)

func (s *Server) runV7HumanPreferenceOwned(c *Client, line string, stage func(*Client, string) humanCommandOutcome) (string, bool) {
	reservation, err := c.reserveConfigurationOutput()
	if err != nil {
		return "Configuration preparation is busy; try again. No changes applied.\n", true
	}
	defer reservation.Release()
	staged := newHumanPreferenceCandidate(c)
	outcome := stage(staged, line)
	if !outcome.publish {
		return outcome.response, outcome.handled
	}
	next, err := staged.captureV7Configuration()
	if err != nil {
		return fmt.Sprintf("Configuration command failed: %v\n", err), true
	}
	canonical, goFilter, ccFilter, err := filter.PrepareV7Configuration(next)
	if err != nil {
		return fmt.Sprintf("Configuration command failed: %v\n", err), true
	}
	staged.filterBanks = &canonical.Filters
	if staged.dialect == DialectCC {
		*staged.filter = ccFilter
	} else {
		*staged.filter = goFilter
	}
	if err := c.admitTemporaryV7Configuration(canonical); err != nil {
		return fmt.Sprintf("Configuration command failed: %v\n", err), true
	}
	if err := s.v7ConfigurationReadbackFits(c, canonical, staged, c.v7PresetReferences); err != nil {
		return fmt.Sprintf("Configuration command failed: %v\n", err), true
	}
	prepared, prepareErr := s.prepareV7ConfigurationRecord(c, canonical, c.v7PresetReferences)
	if prepareErr != nil && !errors.Is(prepareErr, filter.ErrCurrentUserRecord) && errors.Is(prepareErr, filter.ErrStorageAdmission) {
		return fmt.Sprintf("Configuration command failed: %v\n", prepareErr), true
	}
	s.publishHumanPreference(c, staged, outcome.forceSolar)
	c.configurationUnsaved = true
	if prepareErr != nil {
		return outcome.persistenceFailure(c, prepareErr), outcome.handled
	}
	if err := s.commitConfigurationRecord(prepared); err != nil {
		return outcome.persistenceFailure(c, err), outcome.handled
	}
	c.configurationUnsaved = false
	if outcome.warning == humanWarningFilter {
		return outcome.response + "Saved and active.\r\n", outcome.handled
	}
	return outcome.response, outcome.handled
}
