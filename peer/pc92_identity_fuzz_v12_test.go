package peer

import (
	"fmt"
	"strings"
	"testing"
)

func FuzzPC92V12RawIdentity(f *testing.F) {
	for i := uint8(0); i < 5; i++ {
		f.Add("", uint16(1), i, i)
	}
	f.Add("\x00\u00a0^H99^", uint16(65535), uint8(2), uint8(1))
	f.Fuzz(func(t *testing.T, noise string, number uint16, selector, mutation uint8) {
		if len(noise) > 128 {
			noise = noise[:128]
		}
		call := fmt.Sprintf("K%dABC", number)
		role := []string{"origin", "subject", "member"}[int(selector)%3]
		action := []string{"A", "C", "D", "K"}[(int(selector)/3)%4]
		if role == "member" && action == "K" {
			action = "C"
		}
		// Each construction is invalid independently of the production matcher:
		// a missing slash component, >2 SSID digits, lowercase, or punctuation.
		// Put forbidden punctuation before noise: a colon in noise starts a
		// metadata slot, where arbitrary characters are historically numeric-
		// normalized and are outside this raw-identity rejection contract.
		bad := []string{call + "/", "/" + call, call + "-000" + noise, strings.ToLower(call) + noise, "!" + call + noise}[int(mutation)%5]
		if r, err := DecodePC92(v12IdentityFrame(bad, role, action)); err == nil || r != nil {
			t.Fatalf("guaranteed-invalid identity accepted: %s %s %q %+v", action, role, bad, r)
		}
		if r, err := DecodePC92(v12IdentityFrame(call, role, action)); err != nil || r == nil {
			t.Fatalf("valid control rejected: %s %s %q %v", action, role, call, err)
		}
	})
}
