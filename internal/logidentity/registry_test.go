package logidentity

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func directoryID(n uint64) DirectoryID {
	return DirectoryID{id: identity{volume: 7, object: n, valid: true}}
}

func fileID(n uint64) FileID {
	return FileID{id: identity{volume: 7, object: n, valid: true}}
}

func TestRegistryHasFixedSmallBacking(t *testing.T) {
	if size := unsafe.Sizeof(Registry{}); size > 4096 {
		t.Fatalf("registry backing grew to %d bytes", size)
	}
}

func TestBatchGatesUntilEveryDirectoryAndFileIsKnown(t *testing.T) {
	var r Registry
	leases, busy, err := r.BeginBatch([]uint8{1, 2, 3})
	if err != nil || busy != 0 {
		t.Fatalf("batch busy=%b err=%v", busy, err)
	}
	if got := r.DirectoryStatus().State; got != Pending {
		t.Fatalf("unidentified batch directory status = %v", got)
	}
	if err := leases[1].ClaimDirectory(directoryID(1)); err != nil {
		t.Fatal(err)
	}
	if err := leases[2].ClaimDirectory(directoryID(2)); err != nil {
		t.Fatal(err)
	}
	if got := r.DirectoryStatus().State; got != Pending {
		t.Fatalf("unidentified third owner was ignored: %v", got)
	}
	if err := leases[3].ClaimDirectory(directoryID(3)); err != nil {
		t.Fatal(err)
	}
	if got := r.DirectoryStatus().State; got != Ready {
		t.Fatalf("directory status = %v", got)
	}
	if err := leases[1].ClaimFile(fileID(10)); err != nil {
		t.Fatal(err)
	}
	if err := leases[2].MarkFileAbsent(); err != nil {
		t.Fatal(err)
	}
	if got := r.FileStatus().State; got != Pending {
		t.Fatalf("unidentified file owner was ignored: %v", got)
	}
	if err := leases[3].ClaimFile(fileID(30)); err != nil {
		t.Fatal(err)
	}
	if got := r.FileStatus().State; got != Ready {
		t.Fatalf("file status = %v", got)
	}
	if err := leases[2].ClaimFile(fileID(10)); !errors.Is(err, ErrConflict) {
		t.Fatalf("newly opened alias claim = %v", err)
	}
	if got := r.FileStatus(); got.State != Conflict || got.First != 1 || got.Other != 2 {
		t.Fatalf("file conflict = %+v", got)
	}
}

func TestDirectoryConflictTakesPriorityOverAnotherPendingOwner(t *testing.T) {
	var r Registry
	leases, _, err := r.BeginBatch([]uint8{0, 1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := leases[0].ClaimDirectory(directoryID(9)); err != nil {
		t.Fatal(err)
	}
	if err := leases[1].ClaimDirectory(directoryID(9)); !errors.Is(err, ErrConflict) {
		t.Fatalf("alias claim = %v", err)
	}
	if got := r.DirectoryStatus(); got.State != Conflict || got.First != 0 || got.Other != 1 {
		t.Fatalf("directory conflict hidden by pending owner: %+v", got)
	}
	if got := r.FileStatus().State; got != Conflict {
		t.Fatalf("file gate ignored directory conflict: %v", got)
	}
}

func TestCheckDirectoryDetectsRetargetingWithoutChangingClaims(t *testing.T) {
	var r Registry
	leases, _, err := r.BeginBatch([]uint8{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := leases[1].ClaimDirectory(directoryID(11)); err != nil {
		t.Fatal(err)
	}
	if err := leases[2].ClaimDirectory(directoryID(22)); err != nil {
		t.Fatal(err)
	}
	if _, err := leases[1].CheckDirectory(directoryID(11)); err != nil {
		t.Fatalf("unchanged directory = %v", err)
	}
	if other, err := leases[1].CheckDirectory(directoryID(22)); !errors.Is(err, ErrConflict) || other != 2 {
		t.Fatalf("retarget to another owner: other=%d err=%v", other, err)
	}
	if _, err := leases[1].CheckDirectory(directoryID(33)); !errors.Is(err, ErrAlreadyClaimed) {
		t.Fatalf("retarget to unowned directory = %v", err)
	}
	if _, err := leases[1].CheckDirectory(DirectoryID{}); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("unknown directory = %v", err)
	}
	if got := r.DirectoryStatus().State; got != Ready {
		t.Fatalf("read-only checks changed registry status: %v", got)
	}
	if _, err := leases[1].CheckDirectory(directoryID(11)); err != nil {
		t.Fatalf("read-only checks replaced original identity: %v", err)
	}
}

func TestCheckDirectoryRejectsStaleLeaseAndReportsExistingConflict(t *testing.T) {
	var r Registry
	leases, _, err := r.BeginBatch([]uint8{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := leases[1].ClaimDirectory(directoryID(11)); err != nil {
		t.Fatal(err)
	}
	if err := leases[2].ClaimDirectory(directoryID(11)); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting claim = %v", err)
	}
	if other, err := leases[1].CheckDirectory(directoryID(11)); !errors.Is(err, ErrConflict) || other != 2 {
		t.Fatalf("existing conflict hidden by own match: other=%d err=%v", other, err)
	}
	if err := leases[1].Release(); err != nil {
		t.Fatal(err)
	}
	next, _, err := r.BeginBatch([]uint8{1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leases[1].CheckDirectory(directoryID(11)); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("stale lease check = %v", err)
	}
	if _, err := next[1].CheckDirectory(directoryID(33)); !errors.Is(err, ErrAlreadyClaimed) {
		t.Fatalf("pending owner check = %v", err)
	}
}

func TestKnownFileConflictTakesPriorityOverPendingDirectory(t *testing.T) {
	var r Registry
	leases, _, err := r.BeginBatch([]uint8{0, 1, 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, stream := range []uint8{0, 1} {
		if err := leases[stream].ClaimDirectory(directoryID(uint64(stream) + 1)); err != nil {
			t.Fatal(err)
		}
	}
	if err := leases[0].ClaimFile(fileID(9)); err != nil {
		t.Fatal(err)
	}
	if err := leases[1].ClaimFile(fileID(9)); !errors.Is(err, ErrConflict) {
		t.Fatalf("alias claim = %v", err)
	}
	if got := r.FileStatus(); got.State != Conflict || got.First != 0 || got.Other != 1 {
		t.Fatalf("file conflict hidden by pending directory: %+v", got)
	}
}

func TestRetainedClaimsAndStaleGenerationCannotBeReleased(t *testing.T) {
	var r Registry
	leases, _, err := r.BeginBatch([]uint8{4})
	if err != nil {
		t.Fatal(err)
	}
	old := leases[4]
	if err := old.ClaimDirectory(directoryID(11)); err != nil {
		t.Fatal(err)
	}
	if err := old.ClaimFile(fileID(21)); err != nil {
		t.Fatal(err)
	}
	if err := old.Release(); !errors.Is(err, ErrFileClaimed) {
		t.Fatalf("released owner with a live file: %v", err)
	}
	next, busy, err := r.BeginBatch([]uint8{4, 5})
	if err != nil || busy != 1<<4 || next[4].Valid() || !next[5].Valid() {
		t.Fatalf("retained owner lost: busy=%b next4=%t next5=%t err=%v", busy, next[4].Valid(), next[5].Valid(), err)
	}
	if err := next[5].ClaimDirectory(directoryID(11)); !errors.Is(err, ErrConflict) {
		t.Fatalf("retained directory accepted: %v", err)
	}
	if err := next[5].Release(); err != nil {
		t.Fatal(err)
	}
	// The worker must make these calls only after the real file and directory
	// operations have returned; the registry itself never closes an OS handle.
	if err := old.ReleaseFile(); err != nil {
		t.Fatal(err)
	}
	if err := old.Release(); err != nil {
		t.Fatal(err)
	}
	next, busy, err = r.BeginBatch([]uint8{4})
	if err != nil || busy != 0 || !next[4].Valid() {
		t.Fatalf("successor busy=%b err=%v", busy, err)
	}
	if err := old.Release(); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("stale owner released successor: %v", err)
	}
	if err := next[4].ClaimDirectory(directoryID(12)); err != nil {
		t.Fatalf("successor lost after stale release: %v", err)
	}
}

func TestResetClearsClaimsButRetainsGeneration(t *testing.T) {
	var r Registry
	leases, _, err := r.BeginBatch([]uint8{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, stream := range []uint8{1, 2} {
		if err := leases[stream].ClaimDirectory(directoryID(uint64(stream))); err != nil {
			t.Fatal(err)
		}
		if err := leases[stream].ClaimFile(fileID(uint64(stream))); err != nil {
			t.Fatal(err)
		}
	}
	if err := leases[1].Reset(); err != nil {
		t.Fatal(err)
	}
	if r.DirectoryStatus().State != Pending || r.FileStatus().State != Pending {
		t.Fatal("reset owner did not make both identity gates pending")
	}
	_, busy, err := r.BeginBatch([]uint8{1})
	if err != nil || busy != 1<<1 {
		t.Fatalf("reset lost active generation: busy=%b err=%v", busy, err)
	}
	if err := leases[1].ClaimDirectory(directoryID(3)); err != nil {
		t.Fatal(err)
	}
	if err := leases[1].MarkFileAbsent(); err != nil {
		t.Fatal(err)
	}
	if r.DirectoryStatus().State != Ready || r.FileStatus().State != Ready {
		t.Fatal("same lease could not restore identity gates")
	}
	if _, err := leases[2].CheckDirectory(directoryID(2)); err != nil {
		t.Fatalf("reset changed another owner's directory: %v", err)
	}
	if r.slots[2].file != fileID(2) || r.slots[2].fileState != fileClaimed {
		t.Fatal("reset changed another owner's file")
	}
}

func TestResetRejectsInvalidAndStaleLeases(t *testing.T) {
	if err := (Lease{}).Reset(); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("zero lease reset = %v", err)
	}
	var r Registry
	owners, _, err := r.BeginBatch([]uint8{1})
	if err != nil {
		t.Fatal(err)
	}
	old := owners[1]
	if err := old.Release(); err != nil {
		t.Fatal(err)
	}
	if err := old.Reset(); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("released lease reset = %v", err)
	}
	owners, _, err = r.BeginBatch([]uint8{1})
	if err != nil {
		t.Fatal(err)
	}
	if err := owners[1].ClaimDirectory(directoryID(2)); err != nil {
		t.Fatal(err)
	}
	if err := owners[1].ClaimFile(fileID(2)); err != nil {
		t.Fatal(err)
	}
	if err := old.Reset(); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("stale lease reset = %v", err)
	}
	if r.DirectoryStatus().State != Ready || r.FileStatus().State != Ready {
		t.Fatal("stale reset changed successor claims")
	}
}

func TestBatchValidationDoesNotPartiallyRegister(t *testing.T) {
	var r Registry
	if _, _, err := r.BeginBatch([]uint8{1, 1}); !errors.Is(err, ErrDuplicateStream) {
		t.Fatalf("duplicate stream = %v", err)
	}
	if _, _, err := r.BeginBatch([]uint8{1, MaxOwners}); !errors.Is(err, ErrInvalidStream) {
		t.Fatalf("invalid stream = %v", err)
	}
	r.slots[3].generation = math.MaxUint64
	if _, _, err := r.BeginBatch([]uint8{1, 3}); !errors.Is(err, ErrExhausted) {
		t.Fatalf("exhausted stream = %v", err)
	}
	if r.slots[1].active || r.slots[3].active {
		t.Fatal("failed batch partially registered owners")
	}
	if got := r.DirectoryStatus().State; got != Ready {
		t.Fatalf("empty registry = %v", got)
	}
}

func TestUnknownAndReplacementIdentitiesAreNotAccepted(t *testing.T) {
	var r Registry
	leases, _, err := r.BeginBatch([]uint8{1})
	if err != nil {
		t.Fatal(err)
	}
	l := leases[1]
	if err := l.ClaimDirectory(DirectoryID{}); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("unknown directory = %v", err)
	}
	if err := l.ClaimFile(fileID(1)); !errors.Is(err, ErrDirectoryAbsent) {
		t.Fatalf("file before directory = %v", err)
	}
	if err := l.ClaimDirectory(directoryID(1)); err != nil {
		t.Fatal(err)
	}
	if err := l.ClaimDirectory(directoryID(2)); !errors.Is(err, ErrAlreadyClaimed) {
		t.Fatalf("directory replacement = %v", err)
	}
	if err := l.ClaimFile(FileID{}); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("unknown file = %v", err)
	}
	if err := l.ClaimFile(fileID(1)); err != nil {
		t.Fatal(err)
	}
	if err := l.ClaimFile(fileID(2)); !errors.Is(err, ErrAlreadyClaimed) {
		t.Fatalf("file replacement while open = %v", err)
	}
	if err := l.MarkFileAbsent(); !errors.Is(err, ErrFileClaimed) {
		t.Fatalf("marked open file absent: %v", err)
	}
}

func TestReplaceFileTransfersClaimAndRetainsConflict(t *testing.T) {
	var r Registry
	leases, _, err := r.BeginBatch([]uint8{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, stream := range []uint8{0, 1} {
		if err := leases[stream].ClaimDirectory(directoryID(uint64(stream) + 1)); err != nil {
			t.Fatal(err)
		}
	}
	if err := leases[0].ClaimFile(fileID(10)); err != nil {
		t.Fatal(err)
	}
	if err := leases[1].MarkFileAbsent(); err != nil {
		t.Fatal(err)
	}
	// The real prior handle must be closed successfully before replacement.
	if err := leases[0].ReplaceFile(fileID(11)); err != nil {
		t.Fatal(err)
	}
	if got := r.FileStatus().State; got != Ready {
		t.Fatalf("replacement made file gate %v", got)
	}
	if err := leases[1].ReplaceFile(fileID(10)); err != nil {
		t.Fatalf("prior identity was not released or absent assignment failed: %v", err)
	}
	if err := leases[0].ReplaceFile(fileID(10)); !errors.Is(err, ErrConflict) {
		t.Fatalf("replacing with an owned identity = %v", err)
	}
	if got := r.FileStatus(); got.State != Conflict || got.First != 0 || got.Other != 1 {
		t.Fatalf("replacement conflict was not retained: %+v", got)
	}
	if r.slots[0].file != fileID(10) || r.slots[0].fileState != fileClaimed {
		t.Fatal("failed replacement did not retain the conflicting identity")
	}
}

func TestReplaceFileRejectsInvalidAndStaleLease(t *testing.T) {
	var r Registry
	leases, _, err := r.BeginBatch([]uint8{0})
	if err != nil {
		t.Fatal(err)
	}
	old := leases[0]
	if err := old.ReplaceFile(fileID(1)); !errors.Is(err, ErrDirectoryAbsent) {
		t.Fatalf("file before directory = %v", err)
	}
	if err := old.ClaimDirectory(directoryID(1)); err != nil {
		t.Fatal(err)
	}
	if err := old.ReplaceFile(fileID(1)); err != nil {
		t.Fatalf("pending first assignment = %v", err)
	}
	if err := old.ReplaceFile(FileID{}); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("unknown replacement = %v", err)
	}
	if got := r.FileStatus().State; got != Ready || r.slots[0].file != fileID(1) {
		t.Fatalf("invalid replacement lost prior claim: %v", got)
	}
	if err := old.ReleaseFile(); err != nil {
		t.Fatal(err)
	}
	if err := old.Release(); err != nil {
		t.Fatal(err)
	}
	next, _, err := r.BeginBatch([]uint8{0})
	if err != nil {
		t.Fatal(err)
	}
	if err := old.ReplaceFile(fileID(2)); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("stale replacement = %v", err)
	}
	if err := next[0].ClaimDirectory(directoryID(2)); err != nil {
		t.Fatal(err)
	}
	if got := r.FileStatus().State; got != Pending {
		t.Fatalf("stale replacement changed successor: %v", got)
	}
}

func TestClaimConflictErrorRetainsOtherStreamAfterRelease(t *testing.T) {
	for _, operation := range []string{"directory", "file", "replace"} {
		t.Run(operation, func(t *testing.T) {
			var registry Registry
			leases, _, err := registry.BeginBatch([]uint8{3, 7})
			if err != nil {
				t.Fatal(err)
			}
			if err := leases[7].ClaimDirectory(directoryID(1)); err != nil {
				t.Fatal(err)
			}
			var conflict error
			if operation == "directory" {
				conflict = leases[3].ClaimDirectory(directoryID(1))
			} else {
				if err := leases[3].ClaimDirectory(directoryID(2)); err != nil {
					t.Fatal(err)
				}
				if err := leases[7].ClaimFile(fileID(42)); err != nil {
					t.Fatal(err)
				}
				if operation == "file" {
					conflict = leases[3].ClaimFile(fileID(42))
				} else {
					conflict = leases[3].ReplaceFile(fileID(42))
				}
				if err := leases[7].ReleaseFile(); err != nil {
					t.Fatal(err)
				}
			}
			if err := leases[7].Release(); err != nil {
				t.Fatal(err)
			}
			if !errors.Is(conflict, ErrConflict) {
				t.Fatalf("conflict error = %v", conflict)
			}
			var detail *ConflictError
			if !errors.As(conflict, &detail) || detail.Other != 7 {
				t.Fatalf("conflict details after release = %#v", detail)
			}
		})
	}
}

func TestReplaceFileNeverExposesPendingBetweenClaims(t *testing.T) {
	var r Registry
	leases, _, err := r.BeginBatch([]uint8{0})
	if err != nil {
		t.Fatal(err)
	}
	owner := leases[0]
	if err := owner.ClaimDirectory(directoryID(1)); err != nil {
		t.Fatal(err)
	}
	if err := owner.ClaimFile(fileID(1)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 10000; i++ {
			if err := owner.ReplaceFile(fileID(uint64(1 + i%2))); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for {
		if got := r.FileStatus().State; got != Ready {
			t.Fatalf("replacement exposed a non-ready state: %v", got)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			return
		default:
		}
	}
}

func TestConcurrentAliasClaimsHaveOneWinner(t *testing.T) {
	var r Registry
	streams := make([]uint8, MaxOwners)
	for i := range streams {
		streams[i] = uint8(i)
	}
	leases, _, err := r.BeginBatch(streams)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, MaxOwners)
	start := make(chan struct{})
	for _, stream := range streams {
		wg.Add(1)
		go func(l Lease) {
			defer wg.Done()
			<-start
			results <- l.ClaimDirectory(directoryID(1))
		}(leases[stream])
	}
	close(start)
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else if !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	if winners != 1 || r.DirectoryStatus().State != Conflict {
		t.Fatalf("winners=%d status=%+v", winners, r.DirectoryStatus())
	}
}

func TestPendingOwnerDoesNotHoldRegistryLock(t *testing.T) {
	var r Registry
	leases, _, err := r.BeginBatch([]uint8{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	// Stream 1 represents a worker that has not returned from filesystem I/O.
	done := make(chan error, 1)
	go func() { done <- leases[2].ClaimDirectory(directoryID(2)) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending owner blocked unrelated in-memory claim")
	}
	if got := r.DirectoryStatus().State; got != Pending {
		t.Fatalf("pending owner ignored: %v", got)
	}
}
