package reputation

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"dxcluster/download"
	"dxcluster/internal/fsutil"
)

// Use the same cleaned absolute path for validation and I/O. On Unix a
// symlink followed by a parent segment can otherwise send the write somewhere
// different from filepath.Abs's lexical path.
func absolutizeIPInfoPaths(cfg *gateConfig) error {
	for _, item := range []struct {
		key  string
		path *string
	}{
		{"ipinfo_pebble_path", &cfg.ipinfoPebblePath},
		{"ipinfo_download_path", &cfg.ipinfoDownloadPath},
		{"ipinfo_snapshot_path", &cfg.ipinfoSnapshotPath},
	} {
		if *item.path == "" {
			continue
		}
		path, err := filepath.Abs(*item.path)
		if err != nil {
			return fmt.Errorf("IPinfo path %s: %w", item.key, err)
		}
		*item.path = path
	}
	return nil
}

type ipinfoManagedPath struct {
	key, absolute, resolved string
	ancestors               []ipinfoPathAncestor
}

type ipinfoPathAncestor struct {
	info os.FileInfo
	tail string
}

// Check both configured and resolved identities before fetching, extracting or
// cleaning up. Otherwise an archive or CSV can replace the serving pointer,
// verification receipt, or a file inside a database generation.
func validateIPInfoPaths(cfg gateConfig, active *ipinfoSnapshot) error {
	root, err := inspectIPInfoPath("ipinfo_pebble_path", cfg.ipinfoPebblePath)
	if err != nil {
		return err
	}
	protected := []struct{ key, path string }{
		{ipinfoPebbleCurrentFile, filepath.Join(root.absolute, ipinfoPebbleCurrentFile)},
		{ipinfoVerificationFile, filepath.Join(root.absolute, ipinfoVerificationFile)},
	}
	if selected, err := resolveIPInfoPebblePath(root.absolute); err == nil {
		protected = append(protected, struct{ key, path string }{"selected IPinfo database", selected})
	}
	if active != nil {
		protected = append(protected, struct{ key, path string }{"serving IPinfo database", active.path})
	}
	var paths []ipinfoManagedPath
	for _, item := range protected {
		path, err := inspectIPInfoPath(item.key, item.path)
		if err != nil {
			return err
		}
		paths = append(paths, path)
	}
	for _, item := range []struct{ key, path string }{
		{"ipinfo_download_path", cfg.ipinfoDownloadPath},
		{"IPinfo download metadata", download.MetadataPath(cfg.ipinfoDownloadPath)},
		{"ipinfo_snapshot_path", cfg.ipinfoSnapshotPath},
	} {
		path, err := inspectIPInfoPath(item.key, item.path)
		if err != nil {
			return err
		}
		if path.conflictsWithRoot(root) {
			return fmt.Errorf("IPinfo path collision: %s and %s", path.key, root.key)
		}
		for _, prior := range paths {
			if path.collides(prior) {
				return fmt.Errorf("IPinfo path collision: %s and %s", path.key, prior.key)
			}
		}
		paths = append(paths, path)
	}
	return nil
}

func inspectIPInfoPath(key, path string) (ipinfoManagedPath, error) {
	if path == "" {
		return ipinfoManagedPath{}, fmt.Errorf("IPinfo path is empty: %s", key)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return ipinfoManagedPath{}, fmt.Errorf("IPinfo path %s: %w", key, err)
	}
	if runtime.GOOS == "windows" && strings.Contains(absolute[len(filepath.VolumeName(absolute)):], ":") {
		return ipinfoManagedPath{}, fmt.Errorf("IPinfo path %s uses a Windows alternate data stream", key)
	}
	resolved, err := fsutil.ResolvePathIdentity(absolute)
	if err != nil {
		return ipinfoManagedPath{}, fmt.Errorf("IPinfo path %s: %w", key, err)
	}
	identity := ipinfoManagedPath{key: key, absolute: absolute, resolved: resolved}
	for ancestor := resolved; ; ancestor = filepath.Dir(ancestor) {
		info, err := os.Stat(ancestor)
		if err == nil {
			// Compare ancestors even when the target does not exist yet.
			// DOS device prefixes or mapped volumes can spell one directory
			// differently, while its filesystem identity remains the same.
			tail, err := filepath.Rel(ancestor, resolved)
			if err != nil {
				return ipinfoManagedPath{}, fmt.Errorf("IPinfo path %s: %w", key, err)
			}
			identity.ancestors = append(identity.ancestors, ipinfoPathAncestor{info: info, tail: tail})
		} else if !errors.Is(err, os.ErrNotExist) {
			return ipinfoManagedPath{}, fmt.Errorf("IPinfo path %s: %w", key, err)
		}
		if ancestor == filepath.Dir(ancestor) {
			return identity, nil
		}
	}
}

func (p ipinfoManagedPath) collides(other ipinfoManagedPath) bool {
	return p.hasRelativeTo(other, anyIPInfoRelative) || other.hasRelativeTo(p, anyIPInfoRelative)
}

func anyIPInfoRelative(string) bool {
	return true
}

// Test all configured and physical identities. A diagnostic directory can be
// lexically separate while resolving into a reserved database generation.
func (p ipinfoManagedPath) hasRelativeTo(parent ipinfoManagedPath, match func(string) bool) bool {
	if rel, ok := ipinfoLocalRelative(parent.absolute, p.absolute); ok && match(rel) {
		return true
	}
	if rel, ok := ipinfoLocalRelative(parent.resolved, p.resolved); ok && match(rel) {
		return true
	}
	for _, ancestor := range p.ancestors {
		for _, prior := range parent.ancestors {
			if os.SameFile(ancestor.info, prior.info) {
				if rel, ok := ipinfoLocalRelative(prior.tail, ancestor.tail); ok && match(rel) {
					return true
				}
			}
		}
	}
	return false
}

func ipinfoLocalRelative(parent, child string) (string, bool) {
	rel, err := filepath.Rel(ipinfoComparisonPath(parent), ipinfoComparisonPath(child))
	return rel, err == nil && filepath.IsLocal(rel)
}

func ipinfoComparisonPath(path string) string {
	path = strings.ToLower(path)
	if runtime.GOOS != "windows" {
		return path
	}
	// Ordinary Windows paths ignore trailing dots and spaces in components.
	// Compare extended spellings conservatively even when they preserve them.
	volume := filepath.VolumeName(path)
	parts := strings.Split(path[len(volume):], string(filepath.Separator))
	for i, part := range parts {
		if part != "." && part != ".." {
			parts[i] = strings.TrimRight(part, ". ")
		}
	}
	return volume + strings.Join(parts, string(filepath.Separator))
}

func (p ipinfoManagedPath) conflictsWithRoot(root ipinfoManagedPath) bool {
	return root.hasRelativeTo(p, anyIPInfoRelative) || p.hasRelativeTo(root, isIPInfoGenerationPath)
}

func isIPInfoGenerationPath(relative string) bool {
	top, _, _ := strings.Cut(relative, string(filepath.Separator))
	return top == "db" || strings.HasPrefix(top, "db-")
}
