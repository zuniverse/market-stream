package book

import (
	"go/parser"
	"go/token"
	"io/fs"
	"strconv"
	"strings"
	"testing"
)

// TestPackageTakesNoLocks enforces D3 and D4 mechanically rather than by
// discipline. A book is owned by exactly one goroutine and is read by sending
// a request to that goroutine, so nothing in this package has any use for a
// mutex or an atomic. The reflex to add one arrives with the first caller who
// wants to read a book from an HTTP handler, which is precisely when the
// property is worth the most and is easiest to lose.
//
// Test files are exempt: they run books on the test goroutine and may
// synchronise their own scaffolding.
func TestPackageTakesNoLocks(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}
	if len(pkgs) == 0 {
		t.Fatal("no package parsed: the check would pass vacuously")
	}

	banned := map[string]string{
		"sync":        "book state carries no mutex (D3)",
		"sync/atomic": "a book is owned by one goroutine, so nothing in it is shared (D3)",
	}
	var files int
	for _, pkg := range pkgs {
		for name, file := range pkg.Files {
			files++
			for _, imp := range file.Imports {
				path, err := strconv.Unquote(imp.Path.Value)
				if err != nil {
					t.Fatalf("%s: bad import %s", name, imp.Path.Value)
				}
				if why, bad := banned[path]; bad {
					t.Errorf("%s imports %q: %s", name, path, why)
				}
			}
		}
	}
	if files < 4 {
		t.Errorf("checked only %d files: the parser filter is probably wrong", files)
	}
}
