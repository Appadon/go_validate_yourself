package monitor

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNormalizedCPUPercentUsesTotalMachineCapacity(t *testing.T) {
	if got := normalizedCPUPercent(100, 1000); got != 10 {
		t.Fatalf("normalizedCPUPercent() = %v, want 10", got)
	}
}

func TestNormalizedCPUPercentCapsImpossibleSamples(t *testing.T) {
	if got := normalizedCPUPercent(1200, 1000); got != 100 {
		t.Fatalf("normalizedCPUPercent() = %v, want 100", got)
	}
}

func TestNormalizedCPUPercentHandlesZeroSystemDelta(t *testing.T) {
	if got := normalizedCPUPercent(100, 0); got != 0 {
		t.Fatalf("normalizedCPUPercent() = %v, want 0", got)
	}
}

func TestDirectorySizeSumsRegularFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "nested"), 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("1234"), 0o644); err != nil {
		t.Fatalf("write a.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "nested", "b.txt"), []byte("123456"), 0o644); err != nil {
		t.Fatalf("write b.txt: %v", err)
	}

	got, err := DirectorySize(root)
	if err != nil {
		t.Fatalf("DirectorySize() error = %v", err)
	}
	if got != 10 {
		t.Fatalf("DirectorySize() = %d, want 10", got)
	}
}
