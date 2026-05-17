package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"go_validate_yourself/internal/progress"
	"go_validate_yourself/internal/splitcsv"
)

func TestRunAutoReusesSplitCacheAndKeepsSplitDirWhenClearingValidationCache(t *testing.T) {
	tempDir := t.TempDir()
	inputPath := filepath.Join(tempDir, "input.csv")
	schemaPath := filepath.Join(tempDir, "schema.json")
	writeTestFile(t, inputPath, "Record ID,Amount\n1,10\n2,20\n")
	writeTestFile(t, schemaPath, `{"fields":[{"name":"Record ID","type":"string","required":true},{"name":"Amount","type":"int","required":true}]}`)

	opts := autoTestOptions(tempDir, inputPath, schemaPath)

	firstResult, err := New().RunAuto(context.Background(), opts)
	if err != nil {
		t.Fatalf("RunAuto() first run error = %v", err)
	}
	if firstResult.SplitReused {
		t.Fatal("RunAuto() first run unexpectedly reused split cache")
	}
	if _, err := os.Stat(splitcsv.CacheMetadataPath(opts.SplitOutputDir)); err != nil {
		t.Fatalf("split cache metadata missing: %v", err)
	}

	splitSentinel := filepath.Join(opts.SplitOutputDir, "keep.txt")
	successSentinel := filepath.Join(opts.SuccessDir, "stale.txt")
	errorSentinel := filepath.Join(opts.ErrorDir, "stale.txt")
	batchSentinel := filepath.Join(opts.BatchExportDir, "stale.txt")
	writeTestFile(t, splitSentinel, "keep")
	writeTestFile(t, successSentinel, "clear")
	writeTestFile(t, errorSentinel, "clear")
	writeTestFile(t, batchSentinel, "clear")

	opts.ClearValidationCache = true
	secondResult, err := New().RunAuto(context.Background(), opts)
	if err != nil {
		t.Fatalf("RunAuto() second run error = %v", err)
	}
	if !secondResult.SplitReused {
		t.Fatal("RunAuto() second run did not reuse split cache")
	}
	assertExists(t, splitSentinel)
	assertNotExists(t, successSentinel)
	assertNotExists(t, errorSentinel)
	assertNotExists(t, batchSentinel)
}

func TestRunAutoResplitsWhenInputHashChanges(t *testing.T) {
	tempDir := t.TempDir()
	inputPath := filepath.Join(tempDir, "input.csv")
	schemaPath := filepath.Join(tempDir, "schema.json")
	writeTestFile(t, inputPath, "Record ID,Amount\n1,10\n2,20\n")
	writeTestFile(t, schemaPath, `{"fields":[{"name":"Record ID","type":"string","required":true},{"name":"Amount","type":"int","required":true}]}`)

	opts := autoTestOptions(tempDir, inputPath, schemaPath)

	firstResult, err := New().RunAuto(context.Background(), opts)
	if err != nil {
		t.Fatalf("RunAuto() first run error = %v", err)
	}
	if firstResult.SplitReused {
		t.Fatal("RunAuto() first run unexpectedly reused split cache")
	}

	splitSentinel := filepath.Join(opts.SplitOutputDir, "stale.txt")
	writeTestFile(t, splitSentinel, "remove")
	writeTestFile(t, inputPath, "Record ID,Amount\n1,10\n2,20\n3,30\n")

	secondResult, err := New().RunAuto(context.Background(), opts)
	if err != nil {
		t.Fatalf("RunAuto() second run error = %v", err)
	}
	if secondResult.SplitReused {
		t.Fatal("RunAuto() second run unexpectedly reused stale split cache")
	}
	assertNotExists(t, splitSentinel)
	assertExists(t, filepath.Join(opts.SplitOutputDir, "3.parquet"))
}

func TestRunAutoReturnsCanceledContextError(t *testing.T) {
	tempDir := t.TempDir()
	inputPath := filepath.Join(tempDir, "input.csv")
	schemaPath := filepath.Join(tempDir, "schema.json")
	writeTestFile(t, inputPath, "Record ID,Amount\n1,10\n2,20\n")
	writeTestFile(t, schemaPath, `{"fields":[{"name":"Record ID","type":"string","required":true},{"name":"Amount","type":"int","required":true}]}`)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := New().RunAuto(ctx, autoTestOptions(tempDir, inputPath, schemaPath))
	if err != context.Canceled {
		t.Fatalf("RunAuto() error = %v, want %v", err, context.Canceled)
	}
}

func TestRunAutoEmitsStructuredProgressEventsAcrossPhases(t *testing.T) {
	tempDir := t.TempDir()
	inputPath := filepath.Join(tempDir, "input.csv")
	schemaPath := filepath.Join(tempDir, "schema.json")
	writeTestFile(t, inputPath, "Record ID,Amount\n1,10\n2,20\n")
	writeTestFile(t, schemaPath, `{"fields":[{"name":"Record ID","type":"string","required":true},{"name":"Amount","type":"int","required":true}]}`)

	var events []progress.Event
	reporter := progress.FuncReporter(func(event progress.Event) {
		events = append(events, event)
	})

	opts := autoTestOptions(tempDir, inputPath, schemaPath)
	opts.RunID = "run-test-123"
	opts.Reporter = reporter

	if _, err := New().RunAuto(context.Background(), opts); err != nil {
		t.Fatalf("RunAuto() error = %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected progress events")
	}

	seenPhases := map[string]bool{}
	seenProgress := map[string]bool{}
	for _, event := range events {
		if event.RunID != "run-test-123" {
			t.Fatalf("event RunID = %q, want run-test-123", event.RunID)
		}
		if event.Time.IsZero() {
			t.Fatalf("event %+v missing timestamp", event)
		}
		seenPhases[event.Phase] = true
		if event.Type == progress.TypeProgress {
			seenProgress[event.Phase] = true
		}
	}

	for _, phase := range []string{progress.PhaseRun, progress.PhaseSplit, progress.PhaseValidate, progress.PhaseBatch} {
		if !seenPhases[phase] {
			t.Fatalf("missing phase %q in events: %+v", phase, events)
		}
	}
	for _, phase := range []string{progress.PhaseSplit, progress.PhaseValidate, progress.PhaseBatch} {
		if !seenProgress[phase] {
			t.Fatalf("missing progress event for phase %q", phase)
		}
	}
}

func autoTestOptions(tempDir, inputPath, schemaPath string) AutoOptions {
	return AutoOptions{
		MainInputCSV:         inputPath,
		SchemaPath:           schemaPath,
		SplitOutputDir:       filepath.Join(tempDir, "split"),
		SplitPrimaryKey:      "Record ID",
		SplitMaxOpen:         16,
		SplitMissingFile:     "missing_keys.parquet",
		Threads:              1,
		WriteEmptyError:      false,
		ClearValidationCache: false,
		SuccessDir:           filepath.Join(tempDir, "success"),
		ErrorDir:             filepath.Join(tempDir, "errors"),
		BatchDir:             "",
		BatchExportDir:       filepath.Join(tempDir, "batch_export"),
		BatchSize:            1000,
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
}

func assertExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected %q to exist: %v", path, err)
	}
}

func assertNotExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected %q to be absent, got err=%v", path, err)
	}
}
