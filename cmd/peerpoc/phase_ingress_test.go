package main

import (
	"io"
	"testing"
	"time"
)

func TestProbeEveryIngressCategoryRetainsAbsolutePhaseBudget(t *testing.T) {
	for _, traffic := range []string{"silent", "partial", "blank", "wwv", "prompt", "negotiation"} {
		t.Run(traffic, func(t *testing.T) {
			f := newProbeFixture(t, phaseProbeConfig(), nil, nil)
			initial := phaseFirstRead(t, f)
			for i := 0; i < 6; i++ {
				switch traffic {
				case "silent":
				case "partial":
					f.send(t, "x")
				case "blank":
					f.send(t, "\r\n")
				case "wwv":
					f.send(t, "PC23^solar^\r\n")
				case "prompt":
					f.send(t, "login please\r\n")
					if i == 0 {
						f.line(t, "N2WQ-33\r\n")
					}
				case "negotiation":
					f.send(t, string([]byte{255, 251, 1}))
					if err := f.peer.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
						t.Fatal(err)
					}
					reply := make([]byte, 3)
					if _, err := io.ReadFull(f.wire, reply); err != nil || string(reply) != string([]byte{255, 254, 1}) {
						t.Fatalf("drained actual Telnet reply = %v / %v", reply, err)
					}
				}
			}
			requireNaturalPhaseTimeout(t, f, "")
			if phaseCount(f.conn, "phase") != 1 {
				t.Fatal("ingress traffic renewed write phase")
			}
			for _, event := range f.conn.deadlineTrace() {
				if event.kind == "read" && !event.when.Equal(initial.when) {
					t.Fatalf("%s ingress refreshed fixed phase deadline: %+v", traffic, event)
				}
			}
		})
	}
}

func TestProbeReadsUseEarlierIdleOrAbsolutePhaseBound(t *testing.T) {
	for _, partial := range []bool{false, true} {
		for _, idleFirst := range []bool{false, true} {
			name := "phase"
			if idleFirst {
				name = "idle"
			}
			if partial {
				name += "/partial"
			} else {
				name += "/silent"
			}
			t.Run(name, func(t *testing.T) {
				cfg := phaseProbeConfig()
				if idleFirst {
					cfg.idleTimeout = 150 * time.Millisecond
				}
				f := newProbeFixture(t, cfg, nil, nil)
				phase := phaseFirstRead(t, f)
				if partial {
					f.send(t, "unfinished")
				}
				requireNaturalPhaseTimeout(t, f, "")
				read := phaseEvent(t, f.conn, "read", 0)
				if idleFirst {
					if !read.when.Before(phase.when) {
						t.Fatalf("read did not choose earlier idle bound: %+v / %+v", read, phase)
					}
					requirePhaseDuration(t, read, cfg.idleTimeout)
				} else if !read.when.Equal(phase.when) {
					t.Fatalf("read did not choose fixed phase bound: %+v / %+v", read, phase)
				}
			})
		}
	}
}
