package reputation

import (
	"context"
	"dxcluster/strutil"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"
)

type ipinfoClient struct {
	enabled bool
	token   string
	baseURL string
	client  *http.Client
}

func newIPInfoClient(enabled bool, token, baseURL string, timeout time.Duration) *ipinfoClient {
	if !enabled {
		return nil
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return nil
	}
	if baseURL == "" {
		baseURL = "https://ipinfo.io"
	}
	if timeout <= 0 {
		timeout = 250 * time.Millisecond
	}
	return &ipinfoClient{
		enabled: true,
		token:   token,
		baseURL: strings.TrimRight(baseURL, "/"),
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   timeout,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				TLSHandshakeTimeout:   timeout,
				ExpectContinueTimeout: timeout,
			},
		},
	}
}

func (c *ipinfoClient) lookup(addr netip.Addr, now time.Time) (LookupResult, bool) {
	if c == nil || !c.enabled || !addr.IsValid() {
		return LookupResult{}, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.client.Timeout)
	defer cancel()
	url := fmt.Sprintf("%s/%s/json?token=%s", c.baseURL, addr.String(), c.token)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return LookupResult{}, false
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return LookupResult{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			return LookupResult{}, false
		}
		return LookupResult{}, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return LookupResult{}, false
	}
	var parsed ipinfoResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return LookupResult{}, false
	}
	asn := normalizeASN(parsed.ASN())
	country := strutil.NormalizeUpper(parsed.Country)
	if asn == "" && country == "" {
		return LookupResult{}, false
	}
	return LookupResult{
		ASN:         asn,
		CountryCode: country,
		Source:      ipinfoSource,
		FetchedAt:   now,
	}, true
}

type ipinfoResponse struct {
	IP      string `json:"ip"`
	Org     string `json:"org"`
	Country string `json:"country"`
	ASNInfo struct {
		ASN string `json:"asn"`
	} `json:"asn"`
}

func (r ipinfoResponse) ASN() string {
	if strings.TrimSpace(r.ASNInfo.ASN) != "" {
		return r.ASNInfo.ASN
	}
	parts := strings.Fields(r.Org)
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}
