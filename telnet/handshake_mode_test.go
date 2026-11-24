package telnet

import (
	"reflect"
	"testing"
)

func TestNegotiateTelnetFullPreservesCurrentSequence(t *testing.T) {
	conn := &recordingConn{}
	s := &Server{handshakeMode: telnetHandshakeFull, echoMode: telnetEchoServer}
	client := &Client{sessionProfile: &sessionProfile{}, conn: conn}

	s.negotiateTelnet(client)

	want := []byte{
		IAC, WILL, 3,
		IAC, DO, 3,
		IAC, WILL, 1,
		IAC, DONT, 1,
	}
	if got := conn.Bytes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected full handshake bytes:\nwant=%v\ngot=%v", want, got)
	}
}

func TestNegotiateTelnetMinimalServerEchoEmitsReducedSequence(t *testing.T) {
	conn := &recordingConn{}
	s := &Server{handshakeMode: telnetHandshakeMinimal, echoMode: telnetEchoServer}
	client := &Client{sessionProfile: &sessionProfile{}, conn: conn}

	s.negotiateTelnet(client)

	want := []byte{
		IAC, WILL, 3,
		IAC, WILL, 1,
	}
	if got := conn.Bytes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected minimal server-echo handshake bytes:\nwant=%v\ngot=%v", want, got)
	}
}

func TestNegotiateTelnetMinimalLocalEchoEmitsReducedSequence(t *testing.T) {
	conn := &recordingConn{}
	s := &Server{handshakeMode: telnetHandshakeMinimal, echoMode: telnetEchoLocal}
	client := &Client{sessionProfile: &sessionProfile{}, conn: conn}

	s.negotiateTelnet(client)

	want := []byte{
		IAC, WILL, 3,
		IAC, WONT, 1,
	}
	if got := conn.Bytes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected minimal local-echo handshake bytes:\nwant=%v\ngot=%v", want, got)
	}
}

func TestNegotiateTelnetNoneEmitsNoBytes(t *testing.T) {
	conn := &recordingConn{}
	s := &Server{handshakeMode: telnetHandshakeNone, echoMode: telnetEchoServer}
	client := &Client{sessionProfile: &sessionProfile{}, conn: conn}

	s.negotiateTelnet(client)

	if got := conn.Bytes(); len(got) != 0 {
		t.Fatalf("expected no handshake bytes, got %v", got)
	}
}
