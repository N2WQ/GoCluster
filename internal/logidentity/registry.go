// Package logidentity tracks bounded ownership of log destinations. Filesystem
// identity is gathered by destination workers before entering this registry;
// no registry operation performs filesystem I/O or waits for a worker.
package logidentity

import (
	"errors"
	"math"
	"sync"
)

// MaxOwners matches the fixed number of process-wide logging streams.
const MaxOwners = 32

var (
	ErrInvalidStream   = errors.New("invalid log stream")
	ErrDuplicateStream = errors.New("duplicate log stream")
	ErrExhausted       = errors.New("log owner generation exhausted")
	ErrInvalidLease    = errors.New("invalid log owner lease")
	ErrInvalidIdentity = errors.New("invalid filesystem identity")
	ErrAlreadyClaimed  = errors.New("log owner already holds a different identity")
	ErrConflict        = errors.New("filesystem identity is already claimed")
	ErrDirectoryAbsent = errors.New("log directory identity has not been claimed")
	ErrFileClaimed     = errors.New("log file identity is still claimed")
	ErrUnsupported     = errors.New("filesystem identity is unsupported")
	ErrWrongKind       = errors.New("unexpected filesystem object kind")
)

// ConflictError captures the other stream while the registry lock still
// protects the conflicting claim. It remains valid even if that owner retires
// before the caller handles the error.
type ConflictError struct {
	Other uint8
}

func (e *ConflictError) Error() string { return ErrConflict.Error() }
func (e *ConflictError) Unwrap() error { return ErrConflict }

// identity is populated only from an opened filesystem handle. Its primitive
// fields can be compared without a syscall, even on Windows.
type identity struct {
	volume uint64
	object uint64
	valid  bool
}

// DirectoryID and FileID cannot be fabricated by callers from path strings.
// The zero value is invalid and never qualifies as evidence of independence.
type DirectoryID struct{ id identity }
type FileID struct{ id identity }

// State describes all registered owners, including previous generations that
// are still retained by an unfinished filesystem operation.
type State uint8

const (
	Pending State = iota
	Ready
	Conflict
)

// Status identifies the two stream numbers involved when State is Conflict.
type Status struct {
	State        State
	First, Other uint8
}

type fileState uint8

const (
	filePending fileState = iota
	fileAbsent
	fileClaimed
)

type slot struct {
	generation uint64
	active     bool
	directory  DirectoryID
	file       FileID
	fileState  fileState
}

// Registry uses one fixed slot per stream across all runtime generations.
// A slot is never recycled until its trusted worker explicitly releases it
// after all owned operations and successful closes have actually returned.
type Registry struct {
	mu    sync.Mutex
	slots [MaxOwners]slot
}

// Lease refers to one stream generation. The zero value is invalid, and a
// stale lease cannot mutate a successor's claims.
type Lease struct {
	registry   *Registry
	generation uint64
	stream     uint8
}

// BeginBatch registers every requested available stream atomically, before any
// new worker can observe readiness. A set bit in busy identifies a stream still
// owned by an earlier generation; that owner remains part of both status gates.
// All inputs are checked before mutation. A stream may occur at most once.
func (r *Registry) BeginBatch(streams []uint8) (leases [MaxOwners]Lease, busy uint32, err error) {
	if r == nil {
		return leases, 0, ErrInvalidLease
	}
	var requested [MaxOwners]bool
	for _, stream := range streams {
		if int(stream) >= MaxOwners {
			return leases, 0, ErrInvalidStream
		}
		if requested[stream] {
			return leases, 0, ErrDuplicateStream
		}
		requested[stream] = true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, stream := range streams {
		s := &r.slots[stream]
		if !s.active && s.generation == math.MaxUint64 {
			return leases, 0, ErrExhausted
		}
	}
	for _, stream := range streams {
		s := &r.slots[stream]
		if s.active {
			busy |= uint32(1) << stream
			continue
		}
		s.generation++
		s.active = true
		s.directory = DirectoryID{}
		s.file = FileID{}
		s.fileState = filePending
		leases[stream] = Lease{registry: r, stream: stream, generation: s.generation}
	}
	return leases, busy, nil
}

// Valid reports whether this value represents a lease returned by BeginBatch.
// A once-valid lease can still become stale after release.
func (l Lease) Valid() bool {
	return l.registry != nil && l.generation != 0 && int(l.stream) < MaxOwners
}

// ClaimDirectory records one handle-derived directory identity. Conflicting
// identities remain visible to DirectoryStatus until their owners retire.
func (l Lease) ClaimDirectory(id DirectoryID) error {
	if !l.Valid() {
		return ErrInvalidLease
	}
	if !id.id.valid {
		return ErrInvalidIdentity
	}
	r := l.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	s := &r.slots[l.stream]
	if !s.active || s.generation != l.generation {
		return ErrInvalidLease
	}
	if s.directory.id.valid && s.directory != id {
		return ErrAlreadyClaimed
	}
	s.directory = id
	for i := range r.slots {
		if i != int(l.stream) && r.slots[i].active && r.slots[i].directory == id {
			return &ConflictError{Other: uint8(i)}
		}
	}
	return nil
}

// CheckDirectory checks an already-open directory's freshly obtained identity
// without changing any claim. A match with another live owner takes priority
// over an unowned retargeted identity, and returns that owner's stream. The
// identity must be obtained by the worker before entering this in-memory check.
func (l Lease) CheckDirectory(id DirectoryID) (uint8, error) {
	if !l.Valid() {
		return 0, ErrInvalidLease
	}
	if !id.id.valid {
		return 0, ErrInvalidIdentity
	}
	r := l.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	s := &r.slots[l.stream]
	if !s.active || s.generation != l.generation {
		return 0, ErrInvalidLease
	}
	for i := range r.slots {
		if i != int(l.stream) && r.slots[i].active && r.slots[i].directory == id {
			return uint8(i), ErrConflict
		}
	}
	if s.directory != id {
		return 0, ErrAlreadyClaimed
	}
	return 0, nil
}

// ClaimFile records the identity of an opened active file. Callers must not
// write before the file gate is Ready and must reject unsafe link counts.
func (l Lease) ClaimFile(id FileID) error {
	return l.claimFile(id, false)
}

// ReplaceFile atomically transfers this owner's file claim to another opened
// file identity without making the file gate Pending. Before calling it, the
// worker must have observed a successful close of any previous file handle and
// the return of every operation using that handle. A previously absent or
// pending file may also be assigned its first identity this way. As with
// ClaimFile, a conflicting new identity stays claimed until the owner retires.
func (l Lease) ReplaceFile(id FileID) error {
	return l.claimFile(id, true)
}

func (l Lease) claimFile(id FileID, replace bool) error {
	if !l.Valid() {
		return ErrInvalidLease
	}
	if !id.id.valid {
		return ErrInvalidIdentity
	}
	r := l.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	s := &r.slots[l.stream]
	if !s.active || s.generation != l.generation {
		return ErrInvalidLease
	}
	if !s.directory.id.valid {
		return ErrDirectoryAbsent
	}
	if !replace && s.fileState == fileClaimed && s.file != id {
		return ErrAlreadyClaimed
	}
	s.file, s.fileState = id, fileClaimed
	for i := range r.slots {
		if i != int(l.stream) && r.slots[i].active && r.slots[i].fileState == fileClaimed && r.slots[i].file == id {
			return &ConflictError{Other: uint8(i)}
		}
	}
	return nil
}

// MarkFileAbsent records a completed absence check. If the file later appears,
// the worker must claim the identity of the actual opened handle before writing.
func (l Lease) MarkFileAbsent() error {
	if !l.Valid() {
		return ErrInvalidLease
	}
	r := l.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	s := &r.slots[l.stream]
	if !s.active || s.generation != l.generation {
		return ErrInvalidLease
	}
	if !s.directory.id.valid {
		return ErrDirectoryAbsent
	}
	if s.fileState == fileClaimed {
		return ErrFileClaimed
	}
	s.fileState = fileAbsent
	return nil
}

// ReleaseFile clears a file claim only after the worker has observed its actual
// successful close and the return of all operations using it. The file gate
// becomes Pending until the worker identifies the next file or its absence.
func (l Lease) ReleaseFile() error {
	if !l.Valid() {
		return ErrInvalidLease
	}
	r := l.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	s := &r.slots[l.stream]
	if !s.active || s.generation != l.generation {
		return ErrInvalidLease
	}
	s.file, s.fileState = FileID{}, filePending
	return nil
}

// Reset atomically clears both identities while keeping the generation active
// and pending for another probe. The trusted worker must call it only after all
// filesystem operations and all claimed handles have returned and closed
// successfully; this registry does not own or close those handles.
func (l Lease) Reset() error {
	if !l.Valid() {
		return ErrInvalidLease
	}
	r := l.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	s := &r.slots[l.stream]
	if !s.active || s.generation != l.generation {
		return ErrInvalidLease
	}
	s.directory, s.file, s.fileState = DirectoryID{}, FileID{}, filePending
	return nil
}

// Release frees a slot only after all worker operations and directory-handle
// cleanup have actually returned. A file claim must first be explicitly cleared.
func (l Lease) Release() error {
	if !l.Valid() {
		return ErrInvalidLease
	}
	r := l.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	s := &r.slots[l.stream]
	if !s.active || s.generation != l.generation {
		return ErrInvalidLease
	}
	if s.fileState == fileClaimed {
		return ErrFileClaimed
	}
	s.active = false
	s.directory, s.file, s.fileState = DirectoryID{}, FileID{}, filePending
	return nil
}

// DirectoryStatus is Ready only when every registered owner has supplied a
// distinct directory identity. Conflict takes precedence over Pending.
func (r *Registry) DirectoryStatus() Status {
	if r == nil {
		return Status{State: Pending}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.directoryStatusLocked()
}

func (r *Registry) directoryStatusLocked() Status {
	pending := false
	for i := range r.slots {
		a := &r.slots[i]
		if !a.active {
			continue
		}
		if !a.directory.id.valid {
			pending = true
			continue
		}
		for j := i + 1; j < len(r.slots); j++ {
			b := &r.slots[j]
			if b.active && b.directory.id.valid && a.directory == b.directory {
				return Status{State: Conflict, First: uint8(i), Other: uint8(j)}
			}
		}
	}
	if pending {
		return Status{State: Pending}
	}
	return Status{State: Ready}
}

// FileStatus is Ready only after the directory gate is Ready and every owner
// has either claimed a distinct active file or established its absence.
func (r *Registry) FileStatus() Status {
	if r == nil {
		return Status{State: Pending}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	directories := r.directoryStatusLocked()
	if directories.State == Conflict {
		return directories
	}
	pending := directories.State == Pending
	for i := range r.slots {
		a := &r.slots[i]
		if !a.active {
			continue
		}
		if a.fileState == filePending {
			pending = true
			continue
		}
		if a.fileState != fileClaimed {
			continue
		}
		for j := i + 1; j < len(r.slots); j++ {
			b := &r.slots[j]
			if b.active && b.fileState == fileClaimed && a.file == b.file {
				return Status{State: Conflict, First: uint8(i), Other: uint8(j)}
			}
		}
	}
	if pending {
		return Status{State: Pending}
	}
	return Status{State: Ready}
}
