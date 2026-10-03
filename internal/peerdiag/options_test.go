package peerdiag

import (
	"bytes"
	"encoding/binary"
	"math"
	"net"
	"strings"
	"testing"
	"time"
)

func TestV15HelperOptionsIPC(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		want := Options{Enabled: enabled, Directory: "peer-" + strings.Repeat("\u6771", 512), OverlongPath: "relative/overlong.log", RetentionDays: 0, DedupeWindow: 0}
		parent, helper := net.Pipe()
		errC := make(chan error, 1)
		go func() { defer parent.Close(); errC <- sendOptions(parent, want) }()
		got, err := receiveOptions(helper)
		_ = helper.Close()
		if err != nil || got != want {
			t.Fatalf("options=%+v err=%v", got, err)
		}
		if err = <-errC; err != nil {
			t.Fatal(err)
		}
	}
}

func TestV15HelperOptionsRejectBeforePayload(t *testing.T) {
	for _, fault := range []string{"magic", "directory", "overlong", "flags", "retention", "dedupe"} {
		t.Run(fault, func(t *testing.T) {
			var header [optionsHeaderBytes]byte
			binary.LittleEndian.PutUint32(header[:4], optionsMagic)
			switch fault {
			case "magic":
				header[0] = 0
			case "directory":
				binary.LittleEndian.PutUint32(header[4:8], math.MaxUint32)
			case "overlong":
				binary.LittleEndian.PutUint32(header[8:12], math.MaxUint32)
			case "flags":
				binary.LittleEndian.PutUint32(header[12:16], 2)
			case "retention":
				binary.LittleEndian.PutUint64(header[16:24], math.MaxUint64)
			case "dedupe":
				binary.LittleEndian.PutUint64(header[24:32], math.MaxUint64)
			}
			input := bytes.NewBuffer(append(header[:], "unread-payload"...))
			if _, err := receiveOptions(input); err == nil {
				t.Fatal("invalid declaration accepted")
			}
			if input.String() != "unread-payload" {
				t.Fatal("invalid header consumed payload")
			}
		})
	}
}

func TestV15HelperReservationArithmetic(t *testing.T) {
	if !parentLaunchFits(300, 300, 100) || !helperOptionsFit(200, 200, 300) {
		t.Fatal("ordinary bounded paths refused")
	}
	if parentLaunchFits(math.MaxInt, math.MaxInt, math.MaxInt) || helperOptionsFit(math.MaxUint64, math.MaxUint64, math.MaxUint64) {
		t.Fatal("overflow bypassed admission")
	}
	if validOptions(Options{RetentionDays: -1}) || validOptions(Options{DedupeWindow: -time.Second}) {
		t.Fatal("negative scalar accepted")
	}
	if !validOptions(Options{RetentionDays: 0, DedupeWindow: 0}) {
		t.Fatal("explicit zero changed")
	}
}

func FuzzV15HelperOptionsHeader(f *testing.F) {
	var header [optionsHeaderBytes]byte
	binary.LittleEndian.PutUint32(header[:4], optionsMagic)
	f.Add(header[:])
	f.Add(bytes.Repeat([]byte{255}, optionsHeaderBytes))
	f.Fuzz(func(t *testing.T, input []byte) {
		// No payload is supplied: accepted declarations must remain small and
		// report EOF; a hostile length must never cause an unbounded allocation.
		var wire [optionsHeaderBytes]byte
		copy(wire[:], input)
		stream := bytes.NewBuffer(wire[:])
		got, err := receiveOptions(stream)
		if err == nil && (got.Directory != "" || got.OverlongPath != "" || !validOptions(got)) {
			t.Fatal("invalid options accepted without payload")
		}
	})
}
