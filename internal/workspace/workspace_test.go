package workspace

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewUnderBuildsDeterministicWorkspacePaths(t *testing.T) {
	ws, err := NewUnder("/tmp/gvy-ui", " run/123 ")
	if err != nil {
		t.Fatalf("NewUnder() error = %v", err)
	}

	if ws.RunID != "run-123" {
		t.Fatalf("RunID = %q, want run-123", ws.RunID)
	}
	if ws.RootDir != filepath.Join("/tmp/gvy-ui", "run-123") {
		t.Fatalf("RootDir = %q", ws.RootDir)
	}
	if ws.InputCSVPath != filepath.Join(ws.RootDir, "input.csv") {
		t.Fatalf("InputCSVPath = %q", ws.InputCSVPath)
	}
	if ws.SchemaPath != filepath.Join(ws.RootDir, "schema.json") {
		t.Fatalf("SchemaPath = %q", ws.SchemaPath)
	}
	if ws.SplitDir != filepath.Join(ws.RootDir, "split") {
		t.Fatalf("SplitDir = %q", ws.SplitDir)
	}
	if ws.SuccessDir != filepath.Join(ws.RootDir, "success") {
		t.Fatalf("SuccessDir = %q", ws.SuccessDir)
	}
	if ws.ErrorDir != filepath.Join(ws.RootDir, "errors") {
		t.Fatalf("ErrorDir = %q", ws.ErrorDir)
	}
	if ws.BatchExportDir != filepath.Join(ws.RootDir, "batch_export") {
		t.Fatalf("BatchExportDir = %q", ws.BatchExportDir)
	}
	if ws.MetadataPath != filepath.Join(ws.RootDir, "run.json") {
		t.Fatalf("MetadataPath = %q", ws.MetadataPath)
	}
}

func TestPrepareCreatesWorkspaceDirectories(t *testing.T) {
	ws, err := NewUnder(t.TempDir(), "run-prepare")
	if err != nil {
		t.Fatalf("NewUnder() error = %v", err)
	}
	if err := ws.Prepare(); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}

	for _, path := range []string{ws.RootDir, ws.SplitDir, ws.SuccessDir, ws.ErrorDir, ws.BatchExportDir} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat(%q) error = %v", path, err)
		}
		if !info.IsDir() {
			t.Fatalf("%q is not a directory", path)
		}
	}
}
