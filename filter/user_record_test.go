package filter

import (
	"reflect"
	"testing"
	"time"
)

func TestUpdateRecentIPs(t *testing.T) {
	recent := []string{"1.1.1.1", "2.2.2.2"}
	for _, ip := range []string{"3.3.3.3", "1.1.1.1", "4.4.4.4", "5.5.5.5", "6.6.6.6"} {
		recent = UpdateRecentIPs(recent, ip)
	}
	want := []string{"6.6.6.6", "5.5.5.5", "4.4.4.4", "1.1.1.1", "3.3.3.3"}
	if !reflect.DeepEqual(recent, want) {
		t.Fatalf("recent IP order/cap=%v, want %v", recent, want)
	}
}

func TestMergeRecentIPs(t *testing.T) {
	got := MergeRecentIPs([]string{"198.51.100.1", "203.0.113.5"},
		[]string{"203.0.113.5", "192.0.2.9", "198.51.100.1", "203.0.113.6"})
	want := []string{"198.51.100.1", "203.0.113.5", "192.0.2.9", "203.0.113.6"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("merged IP order=%v, want %v", got, want)
	}
}

func TestTouchUserRecordIP(t *testing.T) {
	usePresetTestDir(t)
	record, created, err := TouchUserRecordIP("k3to", "203.0.113.9")
	if err != nil || !created || record.DedupePolicy != DedupePolicyMed || record.PreparedRules() == nil {
		t.Fatal("new login did not use configured new-user factory", err)
	}
	record, created, err = TouchUserRecordIP("k3to", "198.51.100.10")
	if err != nil || created || !reflect.DeepEqual(record.RecentIPs, []string{"198.51.100.10", "203.0.113.9"}) {
		t.Fatal("IP update lost metadata", err)
	}
}

func TestTouchUserRecordLogin(t *testing.T) {
	usePresetTestDir(t)
	first := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	record, created, previous, ip, err := TouchUserRecordLoginWithDefaultDedupe("k3to", "203.0.113.9", first, DedupePolicySlow)
	if err != nil || !created || !previous.IsZero() || ip != "" || record.DedupePolicy != DedupePolicySlow {
		t.Fatal("first login metadata/default policy changed", err)
	}
	second := first.Add(2 * time.Hour)
	record, created, previous, ip, err = TouchUserRecordLoginWithDefaultDedupe("k3to", "198.51.100.10", second, DedupePolicyMed)
	if err != nil || created || !previous.Equal(first) || ip != "203.0.113.9" ||
		!record.LastLoginUTC.Equal(second) || record.DedupePolicy != DedupePolicySlow {
		t.Fatal("saved metadata/policy was overwritten by new-user defaults", err)
	}
}

func TestUserRecordRoundTripAndFullSSIDOwnership(t *testing.T) {
	usePresetTestDir(t)
	f := nativeRuleFilter(t, RuleDefinition{0, RulePass, "MODE=CW AND EVENT=POTA"})
	f.NearbyEnabled = true
	record := &UserRecord{Filter: *f, RecentIPs: []string{"203.0.113.10", "198.51.100.42"},
		DedupePolicy: DedupePolicySlow, Grid: "FN31", NoiseClass: "URBAN", PathMinObservationCount: 42}
	if err := SaveUserRecord("K3TO-1", record); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadUserRecord("K3TO-1")
	if err != nil || !recordConfiguration(record).Equal(recordConfiguration(loaded)) ||
		!reflect.DeepEqual(loaded.RecentIPs, record.RecentIPs) {
		t.Fatal("exact rules/settings/metadata changed", err)
	}
	if err := SaveUserRecord("K3TO-2", &UserRecord{Grid: "IO91"}); err != nil {
		t.Fatal(err)
	}
	loaded, err = LoadUserRecord("K3TO-1")
	if err != nil || loaded.Grid != "FN31" {
		t.Fatal("record state leaked between SSIDs", err)
	}
}

func TestSaveUserFilterPreservesRecentIPs(t *testing.T) {
	usePresetTestDir(t)
	login := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	if err := SaveUserRecord("K3TO", &UserRecord{RecentIPs: []string{"192.0.2.1"}, LastLoginUTC: login}); err != nil {
		t.Fatal(err)
	}
	f := nativeRuleFilter(t, RuleDefinition{0, RuleReject, "MODE=FT8"})
	if err := SaveUserFilter("K3TO", f); err != nil {
		t.Fatal(err)
	}
	record, err := LoadUserRecord("K3TO")
	if err != nil || !record.LastLoginUTC.Equal(login) || record.RecentIPs[0] != "192.0.2.1" ||
		!record.PreparedRules().Equal(f.PreparedRules()) {
		t.Fatal("filter save lost rules or login metadata", err)
	}
}
