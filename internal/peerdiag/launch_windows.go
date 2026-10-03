//go:build windows

package peerdiag

import (
	"errors"
	"os"
	"runtime"
	"strings"
	"syscall"
	"unsafe"
)

const nativeAttributeLimit = 4096
const procThreadAttributeHandleList = 0x00020002
const extendedStartupInfoPresent = 0x00080000

var processKernel = syscall.NewLazyDLL("kernel32.dll")
var initializeAttributes = processKernel.NewProc("InitializeProcThreadAttributeList")
var updateAttributes = processKernel.NewProc("UpdateProcThreadAttribute")
var deleteAttributes = processKernel.NewProc("DeleteProcThreadAttributeList")
var processLocalAlloc = processKernel.NewProc("LocalAlloc")

type processStartupInfo struct {
	syscall.StartupInfo
	attributes uintptr
}

// All native startup resources enter this owner immediately. Go 1.26.4's
// syscall.StartProcess allocates an opaque attribute list without a bound and
// loses that allocation if its second initialization fails. This narrow
// adapter bounds that list, preserves HANDLE_LIST-only inheritance, and retains
// any unconfirmed native release instead of starting overlapping generations.
type companionProcess struct {
	handle, thread                             syscall.Handle
	created                                    bool
	inherited                                  [3]syscall.Handle
	attributes                                 uintptr
	attributeBytes                             uint32
	initialized, deleted                       bool
	attributeCloseAttempted                    bool
	handleCloseAttempted, threadCloseAttempted bool
	inheritedCloseAttempted                    [3]bool
	cleanupFailed                              bool
}

func startCompanion(path string, args []string, attributes *os.ProcAttr) (*companionProcess, error) {
	if len(attributes.Files) != 3 || attributes.Dir != "" {
		return nil, errors.New("invalid diagnostic process attributes")
	}
	commandBytes := 0
	for _, arg := range args {
		if len(arg) > 512-commandBytes {
			return nil, errors.New("diagnostic short argv exceeded")
		}
		commandBytes += len(arg)
	}
	application, err := syscall.UTF16FromString(path)
	if err != nil {
		return nil, err
	}
	var command strings.Builder
	command.Grow(2*commandBytes + len(args))
	for i, arg := range args {
		if i != 0 {
			command.WriteByte(' ')
		}
		command.WriteString(syscall.EscapeArg(arg))
	}
	commandLine, err := syscall.UTF16FromString(command.String())
	if err != nil {
		return nil, err
	}
	environmentBytes := 1
	for _, entry := range attributes.Env {
		if strings.IndexByte(entry, 0) >= 0 || len(entry) > parentReservation-environmentBytes {
			return nil, errors.New("invalid diagnostic environment")
		}
		environmentBytes += len(entry) + 1
	}
	environment := make([]uint16, 0, environmentBytes+1)
	for _, entry := range attributes.Env {
		// Use the OS's WTF-16 conversion, including unpaired surrogates. The
		// temporary and destination buffers are both charged before launch.
		encoded, err := syscall.UTF16FromString(entry)
		if err != nil {
			return nil, err
		}
		environment = append(environment, encoded...)
	}
	environment = append(environment, 0)
	if len(attributes.Env) == 0 {
		environment = append(environment, 0)
	}
	p := &companionProcess{}
	if err = p.allocateAttributes(0); err != nil {
		return p, err
	}
	current, err := syscall.GetCurrentProcess()
	if err != nil {
		return p, err
	}
	for i, file := range attributes.Files {
		if file == nil {
			return p, errors.New("missing diagnostic standard handle")
		}
		if err = syscall.DuplicateHandle(current, syscall.Handle(file.Fd()), current, &p.inherited[i], 0, true, syscall.DUPLICATE_SAME_ACCESS); err != nil {
			return p, err
		}
	}
	ok, _, updateErr := updateAttributes.Call(p.attributes, 0, procThreadAttributeHandleList, uintptr(unsafe.Pointer(&p.inherited[0])), unsafe.Sizeof(p.inherited), 0, 0)
	if ok == 0 {
		return p, updateErr
	}
	startup := processStartupInfo{StartupInfo: syscall.StartupInfo{Flags: syscall.STARTF_USESTDHANDLES | syscall.STARTF_USESHOWWINDOW, ShowWindow: syscall.SW_HIDE, StdInput: p.inherited[0], StdOutput: p.inherited[1], StdErr: p.inherited[2]}, attributes: p.attributes}
	startup.Cb = uint32(unsafe.Sizeof(startup))
	var info syscall.ProcessInformation
	err = syscall.CreateProcess(&application[0], &commandLine[0], nil, nil, true, syscall.CREATE_UNICODE_ENVIRONMENT|extendedStartupInfoPresent, &environment[0], nil, &startup.StartupInfo, &info)
	p.handle, p.thread = info.Process, info.Thread
	p.created = info.Process != 0
	runtime.KeepAlive(p)
	runtime.KeepAlive(attributes)
	if err != nil {
		return p, err
	}
	if err = p.releaseStartup(); err != nil {
		return p, err
	}
	return p, nil
}

func (p *companionProcess) allocateAttributes(flags uint32) error {
	var size uintptr
	_, _, queryErr := initializeAttributes.Call(0, 1, 0, uintptr(unsafe.Pointer(&size)))
	if !errors.Is(queryErr, syscall.ERROR_INSUFFICIENT_BUFFER) {
		return errors.New("diagnostic attribute size query failed")
	}
	if size == 0 || size > nativeAttributeLimit {
		return errors.New("diagnostic native attribute reservation exhausted")
	}
	pointer, _, allocErr := processLocalAlloc.Call(0, size)
	if pointer == 0 {
		return allocErr
	}
	p.attributes, p.attributeBytes = pointer, uint32(size)
	ok, _, initErr := initializeAttributes.Call(pointer, 1, uintptr(flags), uintptr(unsafe.Pointer(&size)))
	if ok == 0 {
		return initErr
	}
	p.initialized = true
	return nil
}

func (p *companionProcess) started() bool { return p.created }
func (p *companionProcess) Kill() error   { return syscall.TerminateProcess(p.handle, 1) }
func (p *companionProcess) Wait() (bool, error) {
	result, err := syscall.WaitForSingleObject(p.handle, syscall.INFINITE)
	if err != nil {
		return false, err
	}
	if result != syscall.WAIT_OBJECT_0 {
		return false, errors.New("diagnostic process wait incomplete")
	}
	var code uint32
	if err = syscall.GetExitCodeProcess(p.handle, &code); err != nil {
		return false, err
	}
	return true, nil
}

func (p *companionProcess) releaseStartup() error {
	for i, handle := range p.inherited {
		if handle != 0 && !p.inheritedCloseAttempted[i] {
			p.inheritedCloseAttempted[i] = true
			if syscall.CloseHandle(handle) != nil {
				p.cleanupFailed = true
			} else {
				p.inherited[i] = 0
			}
		}
	}
	if p.thread != 0 && !p.threadCloseAttempted {
		p.threadCloseAttempted = true
		if syscall.CloseHandle(p.thread) != nil {
			p.cleanupFailed = true
		} else {
			p.thread = 0
		}
	}
	if p.attributes != 0 && !p.attributeCloseAttempted {
		p.attributeCloseAttempted = true
		if p.initialized && !p.deleted {
			deleteAttributes.Call(p.attributes) //nolint:errcheck // Win32 DeleteProcThreadAttributeList is void; LastError is unspecified
			p.deleted = true
		}
		if _, err := syscall.LocalFree(syscall.Handle(p.attributes)); err != nil {
			p.cleanupFailed = true
		} else {
			p.attributes, p.attributeBytes = 0, 0
		}
	}
	if p.cleanupFailed {
		return errors.New("diagnostic native startup release unconfirmed")
	}
	return nil
}

// Called only after a successful process wait, or when no process was created.
func (p *companionProcess) release() error {
	startupErr := p.releaseStartup()
	if p.handle != 0 && !p.handleCloseAttempted {
		p.handleCloseAttempted = true
		if syscall.CloseHandle(p.handle) != nil {
			p.cleanupFailed = true
		} else {
			p.handle = 0
		}
	}
	if p.cleanupFailed {
		return errors.New("diagnostic native process release unconfirmed")
	}
	return startupErr
}
