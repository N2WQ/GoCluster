package main

import (
	"strings"

	"dxcluster/strutil"
	"dxcluster/uls"
)

func shouldRejectCTYCall(call string) bool {
	base := strings.TrimSpace(uls.NormalizeForLicense(call))
	if base == "" {
		return false
	}
	if uls.AllowlistMatchAny(base) {
		return false
	}
	return hasLeadingLettersBeforeDigit(base, 3)
}

func hasLeadingLettersBeforeDigit(call string, threshold int) bool {
	if threshold <= 0 {
		return false
	}
	call = strutil.NormalizeUpper(call)
	if call == "" {
		return false
	}
	count := 0
	for i := 0; i < len(call); i++ {
		ch := call[i]
		if ch >= '0' && ch <= '9' {
			return count >= threshold
		}
		if ch >= 'A' && ch <= 'Z' {
			count++
			if count >= threshold {
				return true
			}
			continue
		}
		break
	}
	return count >= threshold
}
