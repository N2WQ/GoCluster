package spot

import (
	"slices"
	"strings"
	"testing"
)

func TestFCCStateVocabulary(t *testing.T) {
	// This literal is the approved vocabulary, independent of the getter.
	want := strings.Fields("AA AE AK AL AP AR AS AZ CA CO CT DC DE FL GA GU HI IA ID IL IN KS KY LA MA MD ME MI MN MO MP MS MT NC ND NE NH NJ NM NV NY OH OK OR PA PR RI SC SD TN TX UM UT VA VI VT WA WI WV WY")
	got := FCCStateCodes()
	if len(got) != 60 || !slices.Equal(got, want) {
		t.Fatalf("state vocabulary = %v", got)
	}
	for _, code := range want {
		if !IsFCCState(code) {
			t.Errorf("rejected approved code %s", code)
		}
	}
	for _, code := range []string{"", "ca", " CA", "CAN", "ON", "QC", "XX"} {
		if IsFCCState(code) {
			t.Errorf("accepted noncanonical/unsupported code %q", code)
		}
	}
	got[0] = "XX"
	if FCCStateCodes()[0] != "AA" {
		t.Fatal("caller changed shared vocabulary")
	}
}

func TestFCCJurisdictionVocabulary(t *testing.T) {
	want := []int{6, 9, 20, 43, 103, 110, 123, 138, 166, 174, 182, 197, 202, 285, 291, 297, 515}
	for adif := 0; adif <= 600; adif++ {
		if IsFCCJurisdiction(adif) != slices.Contains(want, adif) {
			t.Errorf("jurisdiction mismatch for ADIF %d", adif)
		}
	}
}
