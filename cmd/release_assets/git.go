package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
)

const pointerHeader = "version https://git-lfs.github.com/spec/v1"

var fullObjectID = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)
var lfsPointer = regexp.MustCompile(`^version https://git-lfs.github.com/spec/v1\noid sha256:([0-9a-f]{64})\nsize ([0-9]+)\n?$`)

type gitBlob struct {
	mode, oid string
	size      int64
	lfs       bool
}

type gitTree struct {
	root  *os.Root
	blobs map[string]gitBlob
}

func (tree *gitTree) git(args ...string) *exec.Cmd {
	cmd := exec.CommandContext(context.Background(), "git", args...) // #nosec G204 -- fixed subcommands and Git's own object IDs.
	cmd.Dir = tree.root.Name()
	return cmd
}

func readTree(repo, commit string) (*gitTree, error) {
	root, err := os.OpenRoot(repo)
	if err != nil {
		return nil, err
	}
	tree := &gitTree{root: root, blobs: map[string]gitBlob{}}
	if err := tree.readCommit(commit); err != nil {
		root.Close()
		return nil, err
	}
	return tree, nil
}

func (tree *gitTree) readCommit(commit string) error {
	// The release commit, including its attributes, is the authority even
	// when a checkout was made without Git LFS installed or with smudge off.
	data, err := tree.git("ls-tree", "-rlz", "--full-tree", commit, "--", "data").Output()
	if err != nil {
		return fmt.Errorf("read release tree: %w", err)
	}
	var paths bytes.Buffer
	for record := range strings.SplitSeq(strings.TrimSuffix(string(data), "\x00"), "\x00") {
		meta, name, ok := strings.Cut(record, "\t")
		fields := strings.Fields(meta)
		if !ok || len(fields) != 4 {
			return errors.New("malformed Git tree or no tracked release data")
		}
		if fields[1] != "blob" {
			continue
		}
		size, err := strconv.ParseInt(fields[3], 10, 64)
		if err != nil || size < 0 || !fullObjectID.MatchString(fields[2]) {
			return fmt.Errorf("invalid Git blob for %s", name)
		}
		tree.blobs[name] = gitBlob{mode: fields[0], oid: fields[2], size: size}
		paths.WriteString(name)
		paths.WriteByte(0)
	}
	attrs := tree.git("check-attr", "--source="+commit, "--stdin", "-z", "filter")
	attrs.Stdin = &paths
	values, err := attrs.Output()
	if err != nil {
		return fmt.Errorf("read release commit's filter attributes: %w", err)
	}
	fields := strings.Split(strings.TrimSuffix(string(values), "\x00"), "\x00")
	if len(fields) != 3*len(tree.blobs) {
		return errors.New("incomplete Git attribute inventory")
	}
	for i := 0; i < len(fields); i += 3 {
		name := fields[i]
		blob, ok := tree.blobs[name]
		if !ok || fields[i+1] != "filter" {
			return errors.New("malformed Git attribute inventory")
		}
		blob.lfs = fields[i+2] == "lfs"
		tree.blobs[name] = blob
	}
	return nil
}

// Root confines access even across races. Reject links and non-regular files
// inside it too; checkout paths are never a substitute for tracked blobs.
func checkRegularPath(root *os.Root, name string, allowMissing bool) error {
	for current := name; current != "."; current = path.Dir(current) {
		info, err := root.Lstat(current)
		if allowMissing && errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if reparsePoint(info) || (current == name && !info.Mode().IsRegular()) || (current != name && !info.IsDir()) {
			return fmt.Errorf("non-regular or linked release input: %s", current)
		}
	}
	return nil
}

func (tree *gitTree) stageAsset(stage *os.Root, name string) (string, error) {
	blob, found := tree.blobs[name]
	if !found || (blob.mode != "100644" && blob.mode != "100755") {
		return "", errors.New("missing regular file at release commit")
	}
	if err := checkRegularPath(tree.root, name, false); err != nil {
		return "", err
	}
	if err := checkRegularPath(stage, name, true); err != nil {
		return "", err
	}
	if err := stage.MkdirAll(path.Dir(name), 0755); err != nil {
		return "", err
	}
	// Unlink a prior stage copy before creating a fresh file, so an existing
	// hard link can never redirect writes into the checkout.
	if err := stage.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	file, err := stage.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	w := io.MultiWriter(file, h)
	var want string
	if blob.lfs {
		want, err = tree.copyLFS(w, name, blob)
	} else {
		cmd := tree.git("cat-file", "blob", blob.oid)
		cmd.Stdout = w
		err = cmd.Run()
	}
	if err = errors.Join(err, file.Close()); err != nil {
		return "", err
	}
	digest := hex.EncodeToString(h.Sum(nil))
	if want != "" && digest != want {
		return "", fmt.Errorf("SHA-256 mismatch against release commit LFS OID %s; hydrate the checkout with git lfs pull", want)
	}
	// Also refuse a committed pointer accidentally stored without the LFS
	// filter. Its plausible Git blob hash would pass an ordinary digest check.
	staged, err := stage.Open(name)
	if err != nil {
		return "", err
	}
	defer staged.Close()
	head, err := bufio.NewReader(staged).Peek(len(pointerHeader))
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	if string(head) == pointerHeader {
		return "", errors.New("found Git LFS pointer in place of release data; hydrate the checkout with git lfs pull")
	}
	return digest, nil
}

func (tree *gitTree) copyLFS(w io.Writer, name string, blob gitBlob) (string, error) {
	if blob.size > 1024 {
		return "", errors.New("release commit contains an invalid LFS pointer")
	}
	pointer, err := tree.git("cat-file", "blob", blob.oid).Output()
	if err != nil {
		return "", err
	}
	match := lfsPointer.FindSubmatch(pointer)
	if len(match) != 3 {
		return "", errors.New("missing canonical LFS OID and size at release commit")
	}
	size, err := strconv.ParseInt(string(match[2]), 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid LFS object size: %w", err)
	}
	in, err := tree.root.Open(name)
	if err != nil {
		return "", err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return "", err
	}
	if info.Size() != size {
		return "", fmt.Errorf("expected %d bytes from Git LFS, found %d; hydrate the checkout with git lfs pull", size, info.Size())
	}
	if _, err := io.CopyN(w, in, size); err != nil {
		return "", err
	}
	return string(match[1]), nil
}
