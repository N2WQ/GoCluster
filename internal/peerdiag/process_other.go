//go:build !windows

package peerdiag

import (
	"os"
	"syscall"
)

const helperExecutable = "peerdiag"
const nativePathExpansionReservation = 0
const cwdPathFactor = 16

func helperProcessAttributes() *syscall.SysProcAttr { return nil }
func helperEnvironmentRoot() string                 { return "" }
func helperEnvironmentBytes(string) int             { return len("GOMAXPROCS=1") }
func helperEnvironment(string) []string             { return []string{"GOMAXPROCS=1"} }

func helperExecutablePath(int) (string, error) { return os.Executable() }
func parentNativePathReservation() uint64      { return 0 }
func helperCurrentDirectoryBytes(_, _ uint64) (uint64, error) {
	cwd, err := syscall.Getwd()
	return uint64(len(cwd)), err
}
func helperOSPath(path string) (string, error) { return path, nil }

type companionProcess struct{ process *os.Process }

func startCompanion(path string, args []string, attributes *os.ProcAttr) (*companionProcess, error) {
	process, err := os.StartProcess(path, args, attributes)
	if process == nil {
		return nil, err
	}
	return &companionProcess{process: process}, err
}
func (p *companionProcess) started() bool { return p.process != nil }
func (p *companionProcess) Kill() error   { return p.process.Kill() }
func (p *companionProcess) Wait() (bool, error) {
	state, err := p.process.Wait()
	return err == nil && state != nil, err
}
func (p *companionProcess) release() error { return nil } // successful os.Process.Wait owns native release
