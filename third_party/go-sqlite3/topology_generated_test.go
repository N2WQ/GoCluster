package sqlite3

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestV15SQLiteGeneratedProvenance(t *testing.T) {
	data, err := os.ReadFile("engine/UPSTREAM-FILES.json")
	if err != nil {
		t.Fatal(err)
	}
	var entries []struct{ Path, SHA256 string }
	if err := json.Unmarshal(data, &entries); err != nil || len(entries) != 5 {
		t.Fatalf("invalid pinned engine manifest: %v", err)
	}
	for _, entry := range entries {
		if filepath.Base(entry.Path) != entry.Path {
			t.Fatalf("unexpected manifest path %q", entry.Path)
		}
		contents, err := os.ReadFile(filepath.Join("engine", entry.Path))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(contents)
		if !strings.EqualFold(hex.EncodeToString(digest[:]), entry.SHA256) {
			t.Fatalf("pinned generated engine changed: %s", entry.Path)
		}
	}
}

// Generated libc's ParseInt error can copy a large input. Its strtol callers
// are upstream test executables, not reachable production SQL. Check every
// selector reference, including function-table entries, to keep that proof
// honest without changing generated engine code or restricting SQL values.
func TestV15SQLiteGeneratedHostReachability(t *testing.T) {
	allowed := map[string]map[string]bool{
		"_strtol":          {"Xmain_mptest": true, "_runScript": true, "Xmain_speedtest1": true},
		"_strtoll_helper":  {"_strtol": true},
		"_runScript":       {"Xmain_mptest": true, "_runScript": true},
		"Xmain_mptest":     {},
		"Xmain_speedtest1": {},
	}
	strtolRefs := 0
	for _, name := range []string{"engine/sqlite3.go", "engine/libc.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			caller := "package initialization"
			if fn, ok := decl.(*ast.FuncDecl); ok {
				caller = fn.Name.Name
			}
			ast.Inspect(decl, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				callers, controlled := allowed[selector.Sel.Name]
				if controlled && !callers[caller] {
					t.Errorf("unproved generated host allocation path %s -> %s", caller, selector.Sel.Name)
				}
				if selector.Sel.Name == "_strtol" {
					strtolRefs++
				}
				return true
			})
		}
	}
	if strtolRefs != 12 {
		t.Fatalf("pinned strtol call-site inventory changed: %d", strtolRefs)
	}
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path == "engine" || path == "qualification" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		match, err := build.Default.MatchFile(filepath.Dir(path), filepath.Base(path))
		if err != nil || !match {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if selector, ok := node.(*ast.SelectorExpr); ok {
				if _, controlled := allowed[selector.Sel.Name]; controlled {
					t.Errorf("handwritten production reference enters test-only generated program: %s %s", path, selector.Sel.Name)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("12 strtol references confined to upstream test programs; no production wrapper or generated function-table entry reaches them")
}
