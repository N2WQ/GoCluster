package telnet

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestYAMLSelectedUploadLimitsCountDecodedBodyBytes(t *testing.T) {
	for _, limit := range []int{65_536, 262_144} {
		for _, size := range []int{limit, limit + 1} {
			for _, ending := range []string{"\n", "\r\n"} {
				for _, transport := range []string{"native", "ziutek"} {
					name := fmt.Sprintf("%s/%d/%d/%s", transport, limit, size, fmtEndingName(ending))
					t.Run(name, func(t *testing.T) {
						iac := string([]byte{IAC})
						body := "value: " + strings.Repeat("a", size-len("value: ")-1-len(ending)) + iac + ending
						wireBody := body
						if transport == "native" {
							wireBody = string([]byte{IAC, WILL, 1}) + strings.ReplaceAll(body, iac, string([]byte{IAC, IAC}))
						}
						c, conn := newYAMLTestClient("---" + ending + wireBody + "..." + ending + "RESUME\n")
						c.server = &Server{useZiutek: transport == "ziutek"}
						hooks, timer := fixedYAMLTestHooks()
						got, err := c.receiveYAMLBodyLimitWithHooks(hooks.now().Add(yamlUploadTimeout), limit, hooks)
						if size == limit {
							if err != nil || string(got) != body || cap(got) != limit || conn.closed.Load() {
								t.Fatalf("selected exact limit failed: len=%d cap=%d err=%v closed=%v", len(got), cap(got), err, conn.closed.Load())
							}
							if tail, err := c.readCommandLine(128); err != nil || tail != "RESUME" {
								t.Fatalf("complete upload lost command boundary: tail=%q err=%v", tail, err)
							}
						} else {
							wantError := fmt.Sprintf("YAML body exceeds %d-byte limit", limit)
							if !errors.Is(err, errYAMLUploadTooLarge) || err.Error() != wantError || got != nil || !conn.closed.Load() {
								t.Fatalf("selected limit did not fail terminally: len=%d err=%v closed=%v", len(got), err, conn.closed.Load())
							}
						}
						if !timer.isStopped() {
							t.Fatal("selected-limit receiver retained its watchdog")
						}
					})
				}
			}
		}
	}
}

func TestYAMLLegacyReceiverDoesNotInferSchemaFromBody(t *testing.T) {
	body := "schema_version: 7\nvalue: " + strings.Repeat("a", 70_000) + "\n"
	c, conn := newYAMLTestClient("---\n" + body + "...\nRESUME\n")
	hooks, _ := fixedYAMLTestHooks()
	got, err := c.receiveYAMLBodyWithHooks(hooks.now().Add(yamlUploadTimeout), hooks)
	if !errors.Is(err, errYAMLUploadTooLarge) || got != nil || !conn.closed.Load() {
		t.Fatalf("legacy receiver inferred a larger limit: len=%d err=%v closed=%v", len(got), err, conn.closed.Load())
	}
	c, conn = newYAMLTestClient("---\n" + body + "...\nRESUME\n")
	hooks, _ = fixedYAMLTestHooks()
	got, err = c.receiveYAMLBodyLimitWithHooks(hooks.now().Add(yamlUploadTimeout), 262_144, hooks)
	if err != nil || string(got) != body || conn.closed.Load() {
		t.Fatalf("explicit larger receiver rejected admitted body: len=%d err=%v closed=%v", len(got), err, conn.closed.Load())
	}
}
