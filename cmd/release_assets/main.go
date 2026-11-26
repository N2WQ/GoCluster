// Command release_assets stages public startup files and writes their expected
// ZIP paths and SHA-256 digests into the packager's private run directory.
package main

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"dxcluster/config"
	"dxcluster/internal/toxicity"
)

type manifestEntry struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

func main() {
	repo := flag.String("repo", "", "source checkout")
	commit := flag.String("commit", "", "full release commit ID")
	stage := flag.String("stage", "", "prepared package directory")
	manifest := flag.String("manifest", "", "private output manifest path")
	smoke := flag.String("check-smoke-config", "", "check an isolated startup probe's rewritten YAML directory")
	flag.Parse()
	var err error
	if *smoke != "" {
		if flag.NFlag() != 1 {
			err = errors.New("-check-smoke-config is a separate read-only operation")
		} else {
			err = checkSmokeDirectory(*smoke)
		}
	} else {
		err = run(*repo, *commit, *stage, *manifest)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "release assets:", err)
		os.Exit(1)
	}
}

func run(repo, commit, stage, manifest string) error {
	if repo == "" || stage == "" || manifest == "" || !fullObjectID.MatchString(commit) {
		return errors.New("supply -repo, -stage, -manifest and a full -commit")
	}
	// Runtime path checks resolve relative to process CWD. Keep config.Load's
	// existence probes on the release stage even if a live process uses repo/data.
	repo, err := filepath.Abs(repo)
	if err != nil {
		return err
	}
	manifest, err = filepath.Abs(manifest)
	if err != nil {
		return err
	}
	stage, err = filepath.Abs(stage)
	if err != nil {
		return err
	}
	if err := os.Chdir(stage); err != nil {
		return err
	}
	source, err := readTree(repo, commit)
	if err != nil {
		return err
	}
	defer source.root.Close()
	stageRoot, err := os.OpenRoot(stage)
	if err != nil {
		return err
	}
	defer stageRoot.Close()

	configs, err := checkedConfigFiles(source, stageRoot)
	if err != nil {
		return err
	}
	required := map[string]bool{}
	for _, name := range configs {
		required[name] = true
	}
	cfgDir := filepath.Join(stage, "data", "config")
	cfg, err := config.Load(cfgDir)
	if err != nil {
		return err
	}
	tc, _, err := toxicity.LoadConfig(filepath.Join(cfgDir, "toxicity.yaml"))
	if err != nil {
		return err
	}
	for _, in := range startupInputs(cfg, tc) {
		paths, err := in.paths()
		if err != nil {
			return err
		}
		for _, name := range paths {
			// These two stores bootstrap on an empty installation. Use a
			// checked-in release seed when available, never a running node's DB.
			if _, tracked := source.blobs[name]; !tracked && (in.setting == "skew.file" || in.setting == "peering.topology.db_path") {
				continue
			}
			required[name] = true
		}
	}
	// Earlier ZIPs shipped this public seed for operators enabling peering
	// after installation, even when the example config disabled persistence.
	const historicalTopologySeed = "data/peers/topology.db"
	if _, present := source.blobs[historicalTopologySeed]; present {
		required[historicalTopologySeed] = true
	}
	entries := make([]manifestEntry, 0, len(required))
	for name := range required {
		entries = append(entries, manifestEntry{Path: name})
	}
	slices.SortFunc(entries, func(a, b manifestEntry) int { return cmp.Compare(a.Path, b.Path) })
	for i, entry := range entries {
		digest, err := source.stageAsset(stageRoot, entry.Path)
		if err != nil {
			return fmt.Errorf("%s: %w", entry.Path, err)
		}
		entries[i].SHA256 = digest
	}
	encoded, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(manifest, encoded, 0600)
}

// Dirty previews also pass public-config checks. Never silently replace an
// operator edit with the committed config. Allow the checkout's LF -> CRLF
// conversion and then stage the exact committed bytes for archive verification.
func checkedConfigFiles(source *gitTree, stage *os.Root) ([]string, error) {
	names, err := checkConfigDirectory(stage, "data/config", false)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		entry, ok := source.blobs[name]
		if !ok || entry.lfs || entry.mode != "100644" {
			return nil, fmt.Errorf("%s: expected ordinary tracked public config at release commit", name)
		}
		committed, err := source.git("cat-file", "blob", entry.oid).Output()
		if err != nil {
			return nil, err
		}
		staged, err := stage.ReadFile(name)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(committed, staged) && !bytes.Equal(committed, bytes.ReplaceAll(staged, []byte("\r\n"), []byte("\n"))) {
			return nil, fmt.Errorf("%s: staged configuration differs from release commit", name)
		}
	}
	for name := range source.blobs {
		if path.Dir(name) == "data/config" && isYAML(name) && !slices.Contains(names, name) {
			return nil, fmt.Errorf("%s: public configuration missing from stage", name)
		}
	}
	return names, nil
}

func isYAML(name string) bool {
	ext := strings.ToLower(path.Ext(name))
	return ext == ".yaml" || ext == ".yml"
}

// Both the release builder and the smoke probe check all effective YAML before
// config.Load can probe a configured path. Verification here performs no Git
// operations and never follows paths named by the configuration.
func checkSmokeDirectory(dir string) error {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()
	_, err = checkConfigDirectory(root, ".", true)
	return err
}

func checkConfigDirectory(root *os.Root, dir string, smoke bool) ([]string, error) {
	files, err := fs.ReadDir(root.FS(), dir)
	if err != nil {
		return nil, err
	}
	classified := startupInputs(&config.Config{}, toxicity.Config{})
	var names []string
	for _, de := range files {
		if !isYAML(de.Name()) {
			continue
		}
		name := path.Join(dir, de.Name())
		if err := checkRegularPath(root, name, false); err != nil {
			return nil, err
		}
		content, err := root.ReadFile(name)
		if err != nil {
			return nil, err
		}
		if err := checkClassification(de.Name(), content, classified, smoke); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}
