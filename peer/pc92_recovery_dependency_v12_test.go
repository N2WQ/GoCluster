package peer

import (
	"testing"
	"time"
)

// V14 requires graph-independent retry service while preserving the ordinary
// authority/liveness effects of every K transition.
func TestPC92V14RetryServicePreservesKAuthority(t *testing.T) {
	for _, change := range []string{"none", "metadata", "missing_ingress", "missing_freshness"} {
		t.Run(change, func(t *testing.T) {
			p, source, _, advance, base := recoveryV12Owner(t)
			receiveControllerWire(t, p, source, "PC92^N2AAA^43200^C^5N2AAA:5457:633^H1^", base)
			p.serviceAdmissionRecovery(base)
			suffix := ":5457:633"
			switch change {
			case "metadata":
				suffix = ":5457"
			case "missing_ingress":
				p.graph.loseIngress(source.remoteCall)
			case "missing_freshness":
				p.graph.freshness.Delete("N2AAA")
			}
			advance(time.Second)
			receiveControllerWire(t, p, source, "PC92^N2AAA^43201^K^5N2AAA"+suffix+"^0^0^H1^", p.elapsedNow())
			p.serviceAdmissionRecovery(p.elapsedNow())
			if p.graph.freshness.Value("N2AAA").Value != 43201 || !p.graph.nodes.Value("N2AAA").Seen.Equal(base.Add(time.Second)) {
				t.Fatal("retry service changed K authority/liveness commitment")
			}
			if _, ok := p.graph.ingress.Get(ingressKey{"N2AAA", source.remoteCall}); !ok {
				t.Fatal("K lost ingress observation")
			}
		})
	}
}
