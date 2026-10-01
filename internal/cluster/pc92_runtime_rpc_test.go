//go:build qualification

package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/peer"
	"dxcluster/telnet"
)

const qualificationRPCMaxBytes = 2 << 20

type qualificationRequest struct {
	Kind                     string
	Offset, Epoch, Frequency int64
	Used                     int
}

type qualificationReply struct {
	Error                            string
	State                            peer.QualificationState
	Membership                       telnet.PeerMembership
	Config                           *config.Config `json:",omitempty"`
	Clock, Frequency                 int64
	Enqueue                          []qualificationRecipientResult `json:",omitempty"`
	Failures                         uint64
	Examples                         []string `json:",omitempty"`
	HeapAlloc, HeapInuse, ProcessSys uint64
	OracleBytes                      uint64
	GOMAXPROCS                       int
	Profile                          qualificationLoadProfile
}

// Control traffic is serialized and length-bounded. It never carries hot-path
// enqueue events: shared immutable inputs and child-owned bounded accounting
// remove any opportunity to silently drop or delay an observation in transit.
type qualificationService struct {
	conn     net.Conn
	mu       sync.Mutex
	command  *exec.Cmd
	done     chan error
	log      *os.File
	closed   sync.Once
	closeErr error
	ready    qualificationReply
}

func qualificationWritePacket(w io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > qualificationRPCMaxBytes {
		return fmt.Errorf("qualification control record exceeds limit")
	}
	var size [4]byte
	binary.LittleEndian.PutUint32(size[:], uint32(len(data)))
	if _, err := w.Write(size[:]); err != nil {
		return err
	}
	_, err = io.Copy(w, bytes.NewReader(data))
	return err
}

func qualificationReadPacket(r io.Reader, value any) error {
	var size [4]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return err
	}
	n := binary.LittleEndian.Uint32(size[:])
	if n == 0 || n > qualificationRPCMaxBytes {
		return fmt.Errorf("invalid qualification control record length %d", n)
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(value)
}

func startQualificationService(t *testing.T, repo, output, mappedPath string, count int) *qualificationService {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	lc := net.ListenConfig{}
	listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_ = listener.(*net.TCPListener).SetDeadline(time.Now().Add(30 * time.Second))
	log, err := os.OpenFile(filepath.Join(filepath.Dir(output), "service-output.txt"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	s := &qualificationService{done: make(chan error, 1), log: log}
	s.command = exec.CommandContext(t.Context(), executable, "-test.run=^TestPC92RuntimeService$", "-test.timeout=70m", "-test.v")
	s.command.Env = append(os.Environ(), "GOCLUSTER_PC92_RUNTIME_CHILD=1", "GOCLUSTER_PC92_RUNTIME_REPO="+repo, "GOCLUSTER_PC92_RUNTIME_CONTROL="+listener.Addr().String(), "GOCLUSTER_PC92_RUNTIME_INPUTS="+mappedPath, "GOCLUSTER_PC92_RUNTIME_INPUT_COUNT="+strconv.Itoa(count), "GOMAXPROCS=2", "GOGC=50", "GOMEMLIMIT=1536MiB")
	s.command.Stdout, s.command.Stderr = log, log
	if err := s.command.Start(); err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	go func() { s.done <- s.command.Wait() }()
	t.Cleanup(s.close)
	s.conn, err = listener.Accept()
	if err != nil {
		t.Fatalf("service readiness: %v; see service-output.txt", err)
	}
	_ = s.conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := qualificationReadPacket(s.conn, &s.ready); err != nil || s.ready.Error != "" {
		t.Fatalf("service readiness: %v %s", err, s.ready.Error)
	}
	return s
}

func (s *qualificationService) call(ctx context.Context, request qualificationRequest) (qualificationReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var reply qualificationReply
	if err := ctx.Err(); err != nil {
		return reply, err
	}
	deadline := time.Now().Add(10 * time.Second)
	if caller, ok := ctx.Deadline(); ok && caller.Before(deadline) {
		deadline = caller
	}
	if err := s.conn.SetDeadline(deadline); err != nil {
		return reply, err
	}
	if err := qualificationWritePacket(s.conn, request); err != nil {
		return reply, err
	}
	if err := qualificationReadPacket(s.conn, &reply); err != nil {
		return reply, err
	}
	if reply.Error != "" {
		return reply, fmt.Errorf("service: %s", reply.Error)
	}
	return reply, nil
}

func (s *qualificationService) QualificationSnapshot(ctx context.Context) (peer.QualificationState, error) {
	reply, err := s.call(ctx, qualificationRequest{Kind: "snapshot"})
	return reply.State, err
}

func (s *qualificationService) QualificationSetClockOffset(ctx context.Context, offset time.Duration) error {
	_, err := s.call(ctx, qualificationRequest{Kind: "clock", Offset: int64(offset)})
	return err
}

func (s *qualificationService) membership(ctx context.Context) (telnet.PeerMembership, error) {
	reply, err := s.call(ctx, qualificationRequest{Kind: "membership"})
	return reply.Membership, err
}

func (s *qualificationService) close() {
	s.closed.Do(func() {
		if s.conn != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, s.closeErr = s.call(ctx, qualificationRequest{Kind: "stop"})
			cancel()
			_ = s.conn.Close()
		}
		select {
		case err := <-s.done:
			if err != nil {
				s.closeErr = err
			}
		case <-time.After(5 * time.Second):
			s.closeErr = fmt.Errorf("service did not stop within five seconds")
			_ = s.command.Process.Kill()
			<-s.done
		}
		_ = s.log.Close()
	})
}
