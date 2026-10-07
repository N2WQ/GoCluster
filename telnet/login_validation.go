// File role: Admits login identities using syntax, portable CTY validity and
// base-call FCC/ISED authority. Missing reference data fails open; structured
// rejection reasons are stable operator diagnostics, independent of spot gates.
package telnet

import (
	"log"
	"strings"

	"dxcluster/spot"
	"dxcluster/uls"
)

// loginValidationReason is the support-facing reason vocabulary for login
// admission decisions. These labels are written to event logs and rate-limited
// console logs, so keep them stable for troubleshooting.
type loginValidationReason string

const (
	loginValidationReasonSyntaxInvalid  loginValidationReason = "syntax_invalid"
	loginValidationReasonCTYUnavailable loginValidationReason = "cty_unavailable"
	loginValidationReasonCTYUnknown     loginValidationReason = "cty_unknown"
	loginValidationReasonUSUnlicensed   loginValidationReason = "us_unlicensed"
	loginValidationReasonCAUnlicensed   loginValidationReason = "ca_unlicensed"
)

// loginValidationResult separates hard rejection from fail-open admission. A
// fail-open result means the validator could not prove the login was bad, so
// availability wins and the skip is still reported for operators.
type loginValidationResult struct {
	valid    bool
	failOpen bool
	reason   loginValidationReason
}

// validateLoginCallsign protects the cluster from obvious bad, unknown, or
// unlicensed logins without making external reference data a single point of
// failure. CTY/license snapshot outages fail open and are logged as skipped validation.
func (s *Server) validateLoginCallsign(call string) loginValidationResult {
	if !isValidLoginCallsign(call) {
		return loginValidationResult{reason: loginValidationReasonSyntaxInvalid}
	}
	if s == nil || s.ctyLookup == nil {
		return loginValidationResult{valid: true}
	}
	db := s.ctyLookup()
	if db == nil {
		return loginValidationResult{valid: true, failOpen: true, reason: loginValidationReasonCTYUnavailable}
	}

	lookupCall := call
	testBase, isTestCall := loginTestCallBase(call)
	if isTestCall {
		lookupCall = testBase
	}
	_, ok := db.LookupCallsignPortable(lookupCall)
	if !ok {
		return loginValidationResult{reason: loginValidationReasonCTYUnknown}
	}
	if !isTestCall {
		licenseCall := strings.TrimSpace(uls.NormalizeForLicense(call))
		baseInfo, baseOK := db.LookupCallsignPortable(licenseCall)
		if !baseOK {
			return loginValidationResult{valid: true, failOpen: true, reason: loginValidationReasonCTYUnknown}
		}
		var check func(string) bool
		var reason loginValidationReason
		switch {
		case baseInfo.ADIF == 291:
			check, reason = s.usLicenseCheck, loginValidationReasonUSUnlicensed
		case spot.IsCanadianJurisdiction(baseInfo.ADIF):
			check, reason = s.canadianLicenseCheck, loginValidationReasonCAUnlicensed
		default:
			return loginValidationResult{valid: true}
		}
		if uls.AllowlistMatch(baseInfo.ADIF, licenseCall) {
			return loginValidationResult{valid: true}
		}
		if check == nil {
			return loginValidationResult{valid: true, failOpen: true, reason: loginValidationReasonCTYUnavailable}
		}
		if !check(licenseCall) {
			return loginValidationResult{reason: reason}
		}
	}
	return loginValidationResult{valid: true}
}

// isValidLoginCallsign applies the cheap syntax gate before CTY/ULS lookups so
// invalid input cannot force expensive reference-data work.
func isValidLoginCallsign(call string) bool {
	call = strings.TrimSpace(call)
	return spot.IsValidNormalizedCallsign(call) && containsASCIILetter(call) && !strings.Contains(call, "#")
}

// containsASCIILetter keeps numeric-only tokens from being treated as valid
// calls while staying ASCII-only for telnet login input.
func containsASCIILetter(s string) bool {
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch >= 'A' && ch <= 'Z' {
			return true
		}
	}
	return false
}

// loginTestCallBase lets documented TEST identities exercise cluster paths
// while still proving the base TEST call through CTY.
func loginTestCallBase(call string) (string, bool) {
	call = strings.TrimSpace(call)
	if call == "" || strings.Contains(call, "/") {
		return "", false
	}
	if strings.HasSuffix(call, "TEST") {
		return call, true
	}
	idx := strings.LastIndexByte(call, '-')
	if idx <= 0 || idx >= len(call)-1 {
		return "", false
	}
	for _, ch := range call[idx+1:] {
		if ch < '0' || ch > '9' {
			return "", false
		}
	}
	base := call[:idx]
	if strings.HasSuffix(base, "TEST") {
		return base, true
	}
	return "", false
}

// logLoginValidation emits bounded operator evidence for rejected or skipped
// validation. Accepted logins stay quiet so normal traffic does not dominate
// the support signal.
func (s *Server) logLoginValidation(action string, reason loginValidationReason, call, address string) {
	if s == nil || reason == "" {
		return
	}
	if action != "skipped" {
		s.reportLoginAttempt(action, string(reason), call, address, "")
	}
	total, ok := s.loginValidationLog.Inc()
	if !ok {
		return
	}
	call = strings.TrimSpace(call)
	if call == "" {
		call = "(empty)"
	}
	log.Printf("Login callsign validation %s: reason=%s call=%s addr=%s total=%d", action, reason, call, address, total)
}

// reportLoginAttempt sends the structured file-only event used by support
// tooling. Callers do not need to branch when event logging is disabled.
func (s *Server) reportLoginAttempt(action, reason, call, address, detail string) {
	if s == nil || s.loginAttemptReporter == nil {
		return
	}
	s.loginAttemptReporter(LoginAttemptEvent{
		Action:  action,
		Reason:  reason,
		Call:    call,
		Address: address,
		Detail:  detail,
	})
}

// reportConnection records connection lifecycle events separately from login
// validation so operators can distinguish transport churn from callsign policy.
func (s *Server) reportConnection(action, reason, call, address string) {
	if s == nil || s.connectionReporter == nil {
		return
	}
	s.connectionReporter(ConnectionEvent{
		Action:  action,
		Reason:  reason,
		Call:    call,
		Address: address,
	})
}
