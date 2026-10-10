package main

import (
	"strings"
	"testing"
)

func TestProbePhaseBudgetsPrecedeAndBoundEveryHandshakeWriter(t *testing.T) {
	for _, producer := range []string{"login", "password", "PC18", "PC16", "PC17", "PC19", "PC21", "ack", "raw"} {
		t.Run(producer, func(t *testing.T) {
			cfg := phaseProbeConfig()
			cfg.autoLogin = producer == "login" || producer == "password"
			f := newProbeFixture(t, cfg, nil, nil)
			var prefix string
			initWriter := strings.HasPrefix(producer, "PC")
			switch producer {
			case "login":
				prefix = "N2WQ-33\r\n"
			case "password":
				requireProbeWrite(t, f, "N2WQ-33\r\n")
				f.line(t, "N2WQ-33\r\n")
				prefix = "secret\r\n"
			case "ack":
				phaseFirstRead(t, f)
				f.send(t, "PC51^N2WQ-33^REMOTE^1^\r\n")
				prefix = "PC51^REMOTE^N2WQ-33^0^"
			case "raw":
				phaseFirstRead(t, f)
				f.send(t, string([]byte{255, 251, 1})+"PC51^N2WQ-33^AFTER^1^\r\n")
				prefix = string([]byte{255, 254, 1})
			default:
				phaseFirstRead(t, f)
				f.send(t, producer+"^remote^legacy^\r\n")
				prefix = "PC19^1^N2WQ-33^0^5401^H99^"
			}
			requireProbeWrite(t, f, prefix)
			initial := phaseEvent(t, f.conn, "phase", 0)
			requirePhaseDuration(t, initial, cfg.loginTimeout+cfg.initTimeout)
			active := initial
			if initWriter {
				active = phaseEvent(t, f.conn, "phase", 1)
				requirePhaseDuration(t, active, cfg.initTimeout)
				if active.when.Equal(initial.when) {
					t.Fatal("first init retained combined initial deadline")
				}
			}
			requirePhaseBeforeWrite(t, f, active, prefix)
			requireNaturalPhaseTimeout(t, f, prefix)
			wantPhases := 1
			if initWriter {
				wantPhases++
			}
			if phaseCount(f.conn, "phase") != wantPhases || phaseCount(f.conn, "clear") != 0 {
				t.Fatalf("unexpected phase renewal/establishment: %+v", f.conn.deadlineTrace())
			}
			for _, output := range f.conn.writes() {
				if strings.Contains(output, "AFTER") || output == "BYE\r\n" {
					t.Fatalf("dispatch after phase write failure: %q", output)
				}
			}
		})
	}
}

func TestProbeRepeatedCompatibilityMarkersDoNotRenewFirstInit(t *testing.T) {
	f := newProbeFixture(t, phaseProbeConfig(), nil, nil)
	phaseFirstRead(t, f)
	f.initialize(t, false)
	first := phaseEvent(t, f.conn, "phase", 1)
	// Eleven further small inputs/response pairs stay below all fixture caps.
	for i := 0; i < 5; i++ {
		f.initialize(t, false)
		f.send(t, []string{"PC16^", "PC17^", "PC19^", "PC21^", "PC16^"}[i]+"repeat^\r\n")
	}
	f.syncReader(t)
	f.readerAfterSync(t)
	if phaseCount(f.conn, "phase") != 2 || !phaseEvent(t, f.conn, "phase", 1).when.Equal(first.when) {
		t.Fatalf("repeated marker renewed first init: %+v", f.conn.deadlineTrace())
	}
	requireNaturalPhaseTimeout(t, f, "")
	for _, read := range f.conn.deadlineTrace() {
		if read.kind == "read" && read.order > first.order && !read.when.Equal(first.when) {
			t.Fatalf("post-init read renewed fixed budget: %+v", read)
		}
	}
}
