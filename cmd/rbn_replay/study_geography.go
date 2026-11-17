// File role: Supply the explicit frozen CTY receiver-location study scenario.
// It is applied only to focal A inputs, before learning, and owns no cache or
// receiver registry. A derived cell is not evidence of a historical station site.
package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/spot"
	"dxcluster/strutil"
)

const studyReceiverGeography = "cty_representative_v1"

type studyInputScenario struct {
	ReceiverGeography string `json:"receiver_geography"`
	CTYSHA256         string `json:"cty_sha256"`
	H3Res1SHA256      string `json:"h3_res1_sha256"`
	H3Res2SHA256      string `json:"h3_res2_sha256"`
}

type studyReceiverLocation struct {
	LookupCall string `json:"lookup_call"`
	CTYPrefix  string `json:"cty_prefix"`
	Grid       string `json:"grid"`
	Known      bool   `json:"known"`
	Derived    bool   `json:"derived"`
	Reason     string `json:"reason"`
}

type studyInputCoverage struct {
	FocalRows          int `json:"all_focal_a_rows"`
	Known              int `json:"known_derived_grid"`
	UnknownNoMatch     int `json:"unknown_cty_match"`
	UnknownCoordinates int `json:"unknown_invalid_coordinates"`
	UnknownReceiver    int `json:"unknown_empty_receiver"`
}

func studyValidateInputScenario(s studyInputScenario, arms []studyArm) error {
	if s.ReceiverGeography != studyReceiverGeography || !studyValidSHA(s.CTYSHA256) ||
		!studyValidSHA(s.H3Res1SHA256) || !studyValidSHA(s.H3Res2SHA256) {
		return errors.New("study schema 2 requires explicit cty_representative_v1 and frozen CTY/res1/res2 SHA256 roles")
	}
	for _, arm := range arms {
		for _, sum := range []string{s.CTYSHA256, s.H3Res1SHA256, s.H3Res2SHA256} {
			found := false
			for _, asset := range arm.Assets {
				found = found || strings.EqualFold(asset.SHA256, sum)
			}
			if !found {
				return fmt.Errorf("study input scenario asset is absent from %s manifest", arm.Name)
			}
		}
	}
	return nil
}

// Bind semantic roles to the actual configuration paths, not an unordered set
// of content hashes. This runs before any archive content is inspected.
func studyValidateGeographyAssets(s studyInputScenario, arm studyArm, cfg *config.Config) error {
	if err := studyValidateInputScenario(s, []studyArm{arm}); err != nil {
		return err
	}
	if !cfg.CTY.Enabled || strings.TrimSpace(cfg.CTY.File) == "" || strings.TrimSpace(cfg.H3TablePath) == "" {
		return errors.New("study CTY scenario requires enabled CTY and explicit frozen CTY/H3 paths")
	}
	roles := []studyFile{{Path: cfg.CTY.File, SHA256: s.CTYSHA256},
		{Path: filepath.Join(cfg.H3TablePath, "res1.bin"), SHA256: s.H3Res1SHA256},
		{Path: filepath.Join(cfg.H3TablePath, "res2.bin"), SHA256: s.H3Res2SHA256}}
	for _, role := range roles {
		path, err := filepath.Abs(role.Path)
		if err != nil {
			return err
		}
		found := false
		for _, asset := range arm.Assets {
			assetPath, err := filepath.Abs(asset.Path)
			if err != nil {
				return err
			}
			found = found || strings.EqualFold(path, assetPath) && strings.EqualFold(role.SHA256, asset.SHA256)
		}
		if !found {
			return fmt.Errorf("study input scenario CTY/H3 role binding differs: %s", role.Path)
		}
	}
	return nil
}

// The immutable image is owned by the runner; the observer only borrows it.
// Per-row additions have a <=64-byte raw receiver lookup, <=128-byte prefix,
// 4-byte grid and fixed reason vocabulary. Pending ownership retains no extra
// receivers beyond the existing temporal MaxPending+1 decision contexts.
func studyValidateCTYImage(db *cty.CTYDatabase) error {
	if db == nil || len(db.Data) == 0 {
		return errors.New("study CTY scenario requires a nonempty loaded frozen CTY image")
	}
	for _, info := range db.Data {
		if len(info.Prefix) > 128 {
			return errors.New("study CTY prefix exceeds bounded 128-byte decision provenance")
		}
	}
	return nil
}

// Match normalizeCallForMetadata in internal/cluster/bootstrap.go: preserve
// slash location segments and remove a hyphen suffix only in the final segment.
// CorrectionBaseKey is deliberately unsuitable: K1ABC/DL-# must look up DL.
func studyReceiverLookupCall(call string) string {
	call = strutil.NormalizeUpper(call)
	start := strings.LastIndexByte(call, '/') + 1
	if suffix := strings.IndexByte(call[start:], '-'); suffix >= 0 && start+suffix > 0 {
		call = call[:start+suffix]
	}
	return strings.TrimSpace(call)
}

func studyDeriveReceiverLocation(db *cty.CTYDatabase, receiver string) studyReceiverLocation {
	location := studyReceiverLocation{LookupCall: studyReceiverLookupCall(receiver), Reason: "cty_no_match"}
	if location.LookupCall == "" {
		location.Reason = "empty_receiver"
		return location
	}
	info, ok := db.LookupCallsignPortable(location.LookupCall)
	if !ok || info == nil {
		return location
	}
	location.CTYPrefix = info.Prefix
	grid, ok := cty.Grid4FromPrefixInfo(info)
	if !ok {
		location.Reason = "invalid_coordinates"
		return location
	}
	location.Grid, location.Known, location.Derived, location.Reason = grid, true, true, "cty_representative"
	return location
}

func studyApplyReceiverLocation(s *spot.Spot, location studyReceiverLocation) {
	s.DEMetadata.Grid, s.DEMetadata.GridDerived = location.Grid, location.Derived
	s.InvalidateMetadataCache()
	s.EnsureNormalized()
}

func (c *studyInputCoverage) observe(location studyReceiverLocation) {
	c.FocalRows++
	switch location.Reason {
	case "cty_representative":
		c.Known++
	case "cty_no_match":
		c.UnknownNoMatch++
	case "invalid_coordinates":
		c.UnknownCoordinates++
	case "empty_receiver":
		c.UnknownReceiver++
	}
}

func (c studyInputCoverage) complete(expected int) bool {
	return c.FocalRows == expected && c.Known >= 0 && c.UnknownNoMatch >= 0 && c.UnknownCoordinates >= 0 && c.UnknownReceiver >= 0 &&
		c.Known+c.UnknownNoMatch+c.UnknownCoordinates+c.UnknownReceiver == expected
}

func studyReceiverLocationBinding(location studyReceiverLocation, receiver string) bool {
	if location.LookupCall != studyReceiverLookupCall(receiver) || len(location.CTYPrefix) > 128 {
		return false
	}
	if location.Known {
		return location.Derived && location.Reason == "cty_representative" && len(location.Grid) == 4 &&
			location.Grid[0] >= 'A' && location.Grid[0] <= 'R' && location.Grid[1] >= 'A' && location.Grid[1] <= 'R' &&
			location.Grid[2] >= '0' && location.Grid[2] <= '9' && location.Grid[3] >= '0' && location.Grid[3] <= '9'
	}
	if location.Derived || location.Grid != "" {
		return false
	}
	return location.Reason == "cty_no_match" && location.LookupCall != "" && location.CTYPrefix == "" ||
		location.Reason == "invalid_coordinates" && location.LookupCall != "" ||
		location.Reason == "empty_receiver" && location.LookupCall == "" && location.CTYPrefix == ""
}
