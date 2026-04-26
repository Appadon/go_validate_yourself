package batchparquet

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateBatchInputFileAcceptsRegularFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ok.parquet")
	if err := os.WriteFile(path, []byte("PAR1DATA"), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	if err := validateBatchInputFile(path); err != nil {
		t.Fatalf("validateBatchInputFile() error = %v", err)
	}
}

func TestValidateBatchInputFileRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.parquet")
	if err := os.WriteFile(target, []byte("PAR1DATA"), 0o644); err != nil {
		t.Fatalf("os.WriteFile() target error = %v", err)
	}

	link := filepath.Join(dir, "link.parquet")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("os.Symlink() error = %v", err)
	}

	if err := validateBatchInputFile(link); err == nil {
		t.Fatal("validateBatchInputFile() expected symlink error")
	}
}

func TestValidateBatchInputFileRejectsTooSmallFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tiny.parquet")
	if err := os.WriteFile(path, []byte("small"), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	if err := validateBatchInputFile(path); err == nil {
		t.Fatal("validateBatchInputFile() expected size error")
	}
}
