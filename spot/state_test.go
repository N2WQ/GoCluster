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

func TestCanadianProvinceVocabulary(t *testing.T) {
	// Independent literal oracle includes all ten provinces and three territories.
	want := strings.Fields("AB BC MB NB NL NS NT NU ON PE QC SK YT")
	got := CanadianProvinceCodes()
	if !slices.Equal(got, want) {
		t.Fatalf("province vocabulary = %v", got)
	}
	for _, code := range want {
		if !IsCanadianProvince(code) || !IsState(code) || IsFCCState(code) {
			t.Errorf("province %s has incorrect shared/source membership", code)
		}
	}
	for _, code := range []string{"", "on", " ON", "ON ", "CA", "ZZ"} {
		if IsCanadianProvince(code) {
			t.Errorf("accepted noncanonical/unsupported province %q", code)
		}
	}
	got[0] = "ZZ"
	if CanadianProvinceCodes()[0] != "AB" {
		t.Fatal("caller changed shared province vocabulary")
	}
}

func TestSharedStateVocabulary(t *testing.T) {
	want := strings.Fields("AA AB AE AK AL AP AR AS AZ BC CA CO CT DC DE FL GA GU HI IA ID IL IN KS KY LA MA MB MD ME MI MN MO MP MS MT NB NC ND NE NH NJ NL NM NS NT NU NV NY OH OK ON OR PA PE PR QC RI SC SD SK TN TX UM UT VA VI VT WA WI WV WY YT")
	got := StateCodes()
	if len(got) != 73 || !slices.Equal(got, want) || !slices.IsSorted(got) {
		t.Fatalf("shared vocabulary = %v", got)
	}
	for first := byte('A'); first <= 'Z'; first++ {
		for second := byte('A'); second <= 'Z'; second++ {
			code := string([]byte{first, second})
			if IsState(code) != slices.Contains(want, code) {
				t.Errorf("shared vocabulary membership mismatch for %s", code)
			}
		}
	}
	for _, code := range []string{"", "on", " ON", "ON ", "CAN"} {
		if IsState(code) {
			t.Errorf("accepted noncanonical state %q", code)
		}
	}
	got[0] = "ZZ"
	if StateCodes()[0] != "AA" {
		t.Fatal("caller changed shared state vocabulary")
	}
}

func TestCanadianJurisdictionVocabulary(t *testing.T) {
	want := []int{1, 211, 252}
	for adif := 0; adif <= 600; adif++ {
		if IsCanadianJurisdiction(adif) != slices.Contains(want, adif) {
			t.Errorf("Canadian jurisdiction mismatch for ADIF %d", adif)
		}
	}
}
