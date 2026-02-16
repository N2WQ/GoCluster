package reputation

import (
	"net/netip"
	"strings"
	"unicode"
)

var countryAliasCode = map[string]string{
	"UNITEDSTATES":                     "US",
	"UNITEDSTATESOFAMERICA":            "US",
	"USA":                              "US",
	"UNITEDKINGDOM":                    "GB",
	"GREATBRITAIN":                     "GB",
	"ENGLAND":                          "GB",
	"SCOTLAND":                         "GB",
	"WALES":                            "GB",
	"NORTHERNIRELAND":                  "GB",
	"RUSSIA":                           "RU",
	"RUSSIANFEDERATION":                "RU",
	"SOUTHKOREA":                       "KR",
	"REPUBLICOFKOREA":                  "KR",
	"KOREA":                            "KR",
	"NORTHKOREA":                       "KP",
	"DEMOCRATICPEOPLESREPUBLICOFKOREA": "KP",
	"VIETNAM":                          "VN",
	"IRAN":                             "IR",
	"SYRIA":                            "SY",
	"TANZANIA":                         "TZ",
	"BOLIVIA":                          "BO",
	"VENEZUELA":                        "VE",
}

func countryKeyFromCode(code string) string {
	code = strings.ToUpper(strings.TrimSpace(code))
	if len(code) == 2 {
		return code
	}
	return ""
}

func countryKeyFromName(name string) string {
	name = normalizeCountryName(name)
	if name == "" {
		return ""
	}
	if code, ok := countryAliasCode[name]; ok {
		return code
	}
	return name
}

func normalizeCountryName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range name {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			b.WriteRune(unicode.ToUpper(r))
		}
	}
	return b.String()
}

func continentKey(continent string) string {
	continent = strings.ToUpper(strings.TrimSpace(continent))
	if len(continent) == 2 {
		return continent
	}
	return ""
}

func prefixKeyFromIP(addr netip.Addr) string {
	if !addr.IsValid() {
		return ""
	}
	if addr.Is4() {
		prefix := netip.PrefixFrom(addr, 24).Masked()
		return prefix.String()
	}
	if addr.Is6() {
		prefix := netip.PrefixFrom(addr, 48).Masked()
		return prefix.String()
	}
	return ""
}
