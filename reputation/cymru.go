package reputation

import (
	"context"
	"dxcluster/strutil"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"
)

const (
	cymruSource = "cymru"
)

type cymruResolver struct {
	enabled    bool
	timeout    time.Duration
	cache      *ttlCache
	pendingTTL time.Duration
	queue      chan cymruRequest
	workers    int
	pendingMu  sync.Mutex
	pending    map[string]time.Time
	resolver   *net.Resolver
}

type cymruRequest struct {
	ip  netip.Addr
	key string
}

func newCymruResolver(enabled bool, timeout, cacheTTL, negativeTTL time.Duration, maxEntries, workers int) *cymruResolver {
	if workers <= 0 {
		workers = 2
	}
	if timeout <= 0 {
		timeout = 250 * time.Millisecond
	}
	if cacheTTL <= 0 {
		cacheTTL = time.Hour
	}
	if negativeTTL <= 0 {
		negativeTTL = 5 * time.Minute
	}
	return &cymruResolver{
		enabled:    enabled,
		timeout:    timeout,
		cache:      newTTLCache(16, cacheTTL, negativeTTL, maxEntries),
		pendingTTL: 5 * time.Minute,
		queue:      make(chan cymruRequest, workers*8),
		workers:    workers,
		pending:    make(map[string]time.Time),
		resolver:   net.DefaultResolver,
	}
}

func (c *cymruResolver) start(ctx context.Context) {
	if c == nil || !c.enabled {
		return
	}
	for i := 0; i < c.workers; i++ {
		go c.worker(ctx)
	}
}

func (c *cymruResolver) lookup(addr netip.Addr, now time.Time) (LookupResult, bool, bool) {
	if c == nil || !c.enabled {
		return LookupResult{}, false, false
	}
	key := addr.String()
	if value, ok, negative := c.cache.get(key, now); ok {
		return value, true, negative
	}
	c.enqueue(addr, key, now)
	return LookupResult{}, false, false
}

func (c *cymruResolver) enqueue(addr netip.Addr, key string, now time.Time) {
	if c == nil || !c.enabled {
		return
	}
	c.pendingMu.Lock()
	if t, ok := c.pending[key]; ok {
		if now.Sub(t) < c.pendingTTL {
			c.pendingMu.Unlock()
			return
		}
	}
	c.pending[key] = now
	c.pendingMu.Unlock()

	select {
	case c.queue <- cymruRequest{ip: addr, key: key}:
	default:
		// Drop when queue is full to keep ingress non-blocking.
	}
}

func (c *cymruResolver) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-c.queue:
			now := time.Now().UTC()
			result, ok := c.lookupDNS(ctx, req.ip)
			if ok {
				c.cache.set(req.key, result, now, false)
			} else {
				c.cache.set(req.key, LookupResult{}, now, true)
			}
			c.pendingMu.Lock()
			delete(c.pending, req.key)
			c.pendingMu.Unlock()
		}
	}
}

func (c *cymruResolver) lookupDNS(ctx context.Context, addr netip.Addr) (LookupResult, bool) {
	if c == nil || !c.enabled {
		return LookupResult{}, false
	}
	query, ok := cymruQuery(addr)
	if !ok {
		return LookupResult{}, false
	}
	lookupCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	txts, err := c.resolver.LookupTXT(lookupCtx, query)
	if err != nil || len(txts) == 0 {
		return LookupResult{}, false
	}
	asn, country := parseCymruTXT(txts)
	if asn == "" && country == "" {
		return LookupResult{}, false
	}
	return LookupResult{
		ASN:         asn,
		CountryCode: country,
		Source:      cymruSource,
		FetchedAt:   time.Now().UTC(),
	}, true
}

func cymruQuery(addr netip.Addr) (string, bool) {
	if !addr.IsValid() {
		return "", false
	}
	if addr.Is4() {
		ip := addr.As4()
		return fmt.Sprintf("%d.%d.%d.%d.origin.asn.cymru.com", ip[3], ip[2], ip[1], ip[0]), true
	}
	if addr.Is6() {
		return reverseIPv6(addr) + ".origin6.asn.cymru.com", true
	}
	return "", false
}

func reverseIPv6(addr netip.Addr) string {
	ip := addr.As16()
	var b strings.Builder
	b.Grow(len(ip) * 4)
	for i := len(ip) - 1; i >= 0; i-- {
		lo := ip[i] & 0x0f
		hi := ip[i] >> 4
		b.WriteString(fmt.Sprintf("%x.%x.", lo, hi))
	}
	out := b.String()
	return strings.TrimSuffix(out, ".")
}

func parseCymruTXT(txts []string) (string, string) {
	for _, txt := range txts {
		parts := strings.Split(txt, "|")
		if len(parts) < 3 {
			continue
		}
		asn := normalizeASN(parts[0])
		cc := strutil.NormalizeUpper(parts[2])
		if asn == "" && cc == "" {
			continue
		}
		return asn, cc
	}
	return "", ""
}
