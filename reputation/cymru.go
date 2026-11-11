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
	// Pending identities belong only to queued or executing requests. The
	// request-held pointer distinguishes generations even if wall time repeats.
	pending  map[string]*cymruPending
	resolver *net.Resolver
}

type cymruPending struct {
	at time.Time
}

type cymruRequest struct {
	ip      netip.Addr
	key     string
	pending *cymruPending
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
		pending:    make(map[string]*cymruPending),
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
	defer c.pendingMu.Unlock()
	if pending := c.pending[key]; pending != nil && now.Sub(pending.at) < c.pendingTTL {
		return
	}
	// All producers hold pendingMu. A full queue is a valid nonblocking refusal
	// point and avoids allocating an ownership object for every rejected IP.
	if len(c.queue) == cap(c.queue) {
		return
	}
	pending := &cymruPending{at: now}

	select {
	case c.queue <- cymruRequest{ip: addr, key: key, pending: pending}:
		// A receiver may already be executing; its completion takes pendingMu
		// too, so ownership is published before it can remove this reservation.
		c.pending[key] = pending
	default:
		// No new owner exists when admission fails. Keep any older admitted
		// reservation unchanged, including when this was a stale retry.
	}
}

func (c *cymruResolver) worker(ctx context.Context) {
	c.runWorker(ctx, c.lookupDNS)
}

// runWorker keeps the production queue/cache/retirement loop in one place.
// Its fixed lookup operation is supplied before launch, so tests can control
// completion ordering without DNS traffic or mutable package-global hooks.
func (c *cymruResolver) runWorker(ctx context.Context, lookup func(context.Context, netip.Addr) (LookupResult, bool)) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-c.queue:
			now := time.Now().UTC()
			result, ok := lookup(ctx, req.ip)
			if ok {
				c.cache.set(req.key, result, now, false)
			} else {
				c.cache.set(req.key, LookupResult{}, now, true)
			}
			c.pendingMu.Lock()
			if c.pending[req.key] == req.pending {
				delete(c.pending, req.key)
			}
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
		fmt.Fprintf(&b, "%x.%x.", lo, hi)
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
