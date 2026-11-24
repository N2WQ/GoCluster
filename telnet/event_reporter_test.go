package telnet

import "testing"

func TestReportLoginAttemptUsesConfiguredCallback(t *testing.T) {
	var got LoginAttemptEvent
	s := &Server{
		loginAttemptReporter: func(ev LoginAttemptEvent) {
			got = ev
		},
	}
	s.reportLoginAttempt("rejected", "cty_unknown", "ZZ9ABC", "203.0.113.1:1234", "")
	if got.Action != "rejected" || got.Reason != "cty_unknown" || got.Call != "ZZ9ABC" || got.Address != "203.0.113.1:1234" {
		t.Fatalf("unexpected event: %+v", got)
	}
}

func TestClientCloseReportsDisconnect(t *testing.T) {
	var got ConnectionEvent
	s := &Server{
		connectionReporter: func(ev ConnectionEvent) {
			got = ev
		},
	}
	c := &Client{sessionProfile: &sessionProfile{},
		server:   s,
		callsign: "K1ABC",
		address:  "203.0.113.1:1234",
		done:     make(chan struct{}),
	}
	c.close("test")
	if got.Action != "disconnect" || got.Reason != "test" || got.Call != "K1ABC" || got.Address != "203.0.113.1:1234" {
		t.Fatalf("unexpected event: %+v", got)
	}
}
