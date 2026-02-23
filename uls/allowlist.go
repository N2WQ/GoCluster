package uls

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"

	"dxcluster/strutil"
)

const defaultAllowlistJurisdiction = "US"

type Allowlist struct {
	byJurisdiction map[string]*compiledAllowlist
}

type compiledAllowlist struct {
	entries []allowlistEntry
}

type allowlistEntry struct {
	pattern string
	re      *regexp.Regexp
	literal string
	prefix  string
	suffix  string
	minLen  int
	maxLen  int
}

var allowlist atomic.Pointer[Allowlist]

func SetAllowlistPath(path string) {
	path = strings.TrimSpace(path)
	if path == "" {
		allowlist.Store(nil)
		return
	}
	al, err := LoadAllowlistFromFile(path)
	if err != nil {
		log.Printf("FCC ULS: allowlist load failed from %s: %v", path, err)
		allowlist.Store(nil)
		return
	}
	allowlist.Store(al)
}

func AllowlistMatch(adif int, call string) bool {
	al := allowlist.Load()
	if al == nil {
		return false
	}
	for _, key := range jurisdictionKeys(adif) {
		if al.match(key, call) {
			return true
		}
	}
	return false
}

// AllowlistMatchAny matches a call against all configured jurisdictions.
// It is intended for cases where jurisdiction is unknown but allowlist should override drops.
func AllowlistMatchAny(call string) bool {
	al := allowlist.Load()
	if al == nil || call == "" {
		return false
	}
	for key := range al.byJurisdiction {
		if al.match(key, call) {
			return true
		}
	}
	return false
}

func LoadAllowlistFromFile(path string) (*Allowlist, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return LoadAllowlist(f)
}

func LoadAllowlist(r io.Reader) (*Allowlist, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024), 16*1024)
	compiled := &Allowlist{byJurisdiction: make(map[string]*compiledAllowlist)}
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" || strings.HasPrefix(raw, "#") {
			continue
		}
		jKey, pattern := splitAllowlistLine(raw)
		if pattern == "" {
			continue
		}
		entry, err := compileAllowlistEntry(pattern)
		if err != nil {
			return nil, fmt.Errorf("allowlist line %d: %w", lineNum, err)
		}
		if compiled.byJurisdiction[jKey] == nil {
			compiled.byJurisdiction[jKey] = &compiledAllowlist{}
		}
		compiled.byJurisdiction[jKey].entries = append(compiled.byJurisdiction[jKey].entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return compiled, nil
}

func (a *Allowlist) match(jurisdiction, call string) bool {
	if a == nil || jurisdiction == "" || call == "" {
		return false
	}
	c := a.byJurisdiction[jurisdiction]
	if c == nil {
		return false
	}
	for _, entry := range c.entries {
		if entry.minLen > 0 && len(call) < entry.minLen {
			continue
		}
		if entry.maxLen > 0 && len(call) > entry.maxLen {
			continue
		}
		if entry.literal != "" {
			if call == entry.literal {
				return true
			}
			continue
		}
		if entry.prefix != "" && !strings.HasPrefix(call, entry.prefix) {
			continue
		}
		if entry.suffix != "" && !strings.HasSuffix(call, entry.suffix) {
			continue
		}
		if entry.re != nil && entry.re.MatchString(call) {
			return true
		}
	}
	return false
}

func splitAllowlistLine(line string) (string, string) {
	colon := strings.IndexByte(line, ':')
	if colon <= 0 {
		return defaultAllowlistJurisdiction, line
	}
	prefix := strings.TrimSpace(line[:colon])
	if prefix == "" || !isJurisdictionToken(prefix) {
		return defaultAllowlistJurisdiction, line
	}
	pattern := strings.TrimSpace(line[colon+1:])
	if pattern == "" {
		return strings.ToUpper(prefix), ""
	}
	return normalizeJurisdictionKey(prefix), pattern
}

func normalizeJurisdictionKey(key string) string {
	key = strutil.NormalizeUpper(key)
	if key == "" {
		return defaultAllowlistJurisdiction
	}
	if strutil.IsAllDigitsASCII(key) {
		return "ADIF" + key
	}
	if strings.HasPrefix(key, "ADIF") && strutil.IsAllDigitsASCII(strings.TrimPrefix(key, "ADIF")) {
		return key
	}
	return key
}

func jurisdictionKeys(adif int) []string {
	if adif <= 0 {
		return nil
	}
	if adif == 291 {
		return []string{"US", "ADIF291"}
	}
	return []string{"ADIF" + strconv.Itoa(adif)}
}

func compileAllowlistEntry(pattern string) (allowlistEntry, error) {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return allowlistEntry{}, fmt.Errorf("empty allowlist pattern")
	}
	entry := allowlistEntry{pattern: pattern}
	if isLiteralPattern(pattern) {
		entry.literal = strings.ToUpper(pattern)
		entry.minLen = len(entry.literal)
		entry.maxLen = len(entry.literal)
		return entry, nil
	}
	if pref, suf, ok := anchoredLiteral(pattern); ok {
		entry.prefix = strings.ToUpper(pref)
		entry.suffix = strings.ToUpper(suf)
		if entry.prefix != "" || entry.suffix != "" {
			entry.minLen = len(entry.prefix) + len(entry.suffix)
		}
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return allowlistEntry{}, err
	}
	entry.re = re
	return entry, nil
}

func anchoredLiteral(pattern string) (string, string, bool) {
	if strings.HasPrefix(pattern, "^") && strings.HasSuffix(pattern, "$") {
		body := strings.TrimSuffix(strings.TrimPrefix(pattern, "^"), "$")
		if isLiteralPattern(body) {
			return body, "", true
		}
	}
	if strings.HasPrefix(pattern, "^") {
		body := strings.TrimPrefix(pattern, "^")
		if isLiteralPattern(body) {
			return body, "", true
		}
	}
	if strings.HasSuffix(pattern, "$") {
		body := strings.TrimSuffix(pattern, "$")
		if isLiteralPattern(body) {
			return "", body, true
		}
	}
	return "", "", false
}

func isLiteralPattern(pattern string) bool {
	if pattern == "" {
		return false
	}
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '.', '*', '+', '?', '|', '(', ')', '[', ']', '{', '}', '\\', '^', '$':
			return false
		}
	}
	return true
}

func isJurisdictionToken(token string) bool {
	if token == "" {
		return false
	}
	for i := 0; i < len(token); i++ {
		ch := token[i]
		if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' {
			continue
		}
		return false
	}
	return true
}
