package logidentity

import "os"

type metadata struct {
	id    identity
	mode  os.FileMode
	links uint64
}

// IdentifyDirectory obtains the physical identity of an already-open folder.
// This can block in the operating system and must run on the owning log worker,
// never on startup, a producer, or while holding a Registry lock. To prevent
// identity reuse, the owner must keep the handle open for the entire claim and
// must not close it concurrently with this call.
func IdentifyDirectory(file *os.File) (DirectoryID, error) {
	if file == nil {
		return DirectoryID{}, ErrInvalidIdentity
	}
	info, err := inspect(file)
	if err != nil {
		return DirectoryID{}, err
	}
	if !info.mode.IsDir() {
		return DirectoryID{}, ErrWrongKind
	}
	return DirectoryID{id: info.id}, nil
}

// IdentifyFile obtains the identity and link count of an already-open regular
// file. Link count is returned so the owner can reject unsafe external aliases.
// Like IdentifyDirectory, this can block and must run only on the log worker.
// Keep the file handle open until every operation using its claim has returned.
func IdentifyFile(file *os.File) (FileID, uint64, error) {
	if file == nil {
		return FileID{}, 0, ErrInvalidIdentity
	}
	info, err := inspect(file)
	if err != nil {
		return FileID{}, 0, err
	}
	if !info.mode.IsRegular() {
		return FileID{}, 0, ErrWrongKind
	}
	if info.links == 0 {
		return FileID{}, 0, ErrInvalidIdentity
	}
	return FileID{id: info.id}, info.links, nil
}
