package service

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go_validate_yourself/internal/progress"
	"go_validate_yourself/internal/splitcsv"
)

func TestRunPipelineSplitOnlyExecutesSplitOnly(t *testing.T) {
	tempDir := t.TempDir()
	inputPath, schemaPath := writePipelineInputFixture(t, tempDir)
	_ = schemaPath
	var events []progress.Event

	result, err := New().RunPipeline(context.Background(), PipelineOptions{
		Phases:       []PipelinePhase{PipelinePhaseSplit},
		ResumePolicy: PipelineResumePolicyRunAll,
		Split: SplitOptions{
			InputPath:       inputPath,
			OutputDir:       filepath.Join(tempDir, "split"),
			PrimaryKey:      "Record ID",
			MaxOpenWriters:  8,
			MissingKeysFile: "missing_keys.csv",
		},
		Reporter: collectPipelineEvents(&events),
	})
	if err != nil {
		t.Fatalf("RunPipeline() error = %v", err)
	}
	if result.SplitSummary.TotalRows != 2 {
		t.Fatalf("SplitSummary.TotalRows = %d, want 2", result.SplitSummary.TotalRows)
	}
	assertExists(t, filepath.Join(tempDir, "split", "1.csv"))
	assertPipelinePhases(t, result.RanPhases, []PipelinePhase{PipelinePhaseSplit})
	assertNoPipelineEventPhase(t, events, progress.PhaseValidate)
	assertNoPipelineEventPhase(t, events, progress.PhaseBatch)
}

func TestRunPipelineValidateOnlyConsumesExistingSplitDirectory(t *testing.T) {
	tempDir := t.TempDir()
	_, schemaPath := writePipelineInputFixture(t, tempDir)
	splitDir := filepath.Join(tempDir, "split")
	writeTestFile(t, filepath.Join(splitDir, "1.csv"), "Record ID,Amount\n1,10\n")

	result, err := New().RunPipeline(context.Background(), PipelineOptions{
		Phases: []PipelinePhase{PipelinePhaseValidate},
		Validate: ValidateOptions{
			SchemaPath: schemaPath,
			InputDir:   splitDir,
			Threads:    1,
			SuccessDir: filepath.Join(tempDir, "success"),
			ErrorDir:   filepath.Join(tempDir, "errors"),
		},
	})
	if err != nil {
		t.Fatalf("RunPipeline() error = %v", err)
	}
	if result.ValidationDir == nil || result.ValidationDir.Summary.Files != 1 {
		t.Fatalf("ValidationDir = %+v, want one validated file", result.ValidationDir)
	}
	assertExists(t, filepath.Join(tempDir, "success", "1.parquet"))
	assertPipelinePhases(t, result.RanPhases, []PipelinePhase{PipelinePhaseValidate})
}

func TestRunPipelineBatchOnlyConsumesExistingParquetDirectory(t *testing.T) {
	tempDir := t.TempDir()
	parquetDir := createPipelineParquetDir(t, tempDir)
	var events []progress.Event

	result, err := New().RunPipeline(context.Background(), PipelineOptions{
		Phases: []PipelinePhase{PipelinePhaseBatch},
		Batch: BatchOptions{
			InputDir:  parquetDir,
			OutputDir: filepath.Join(tempDir, "batch_export"),
			BatchSize: 1,
			Workers:   1,
		},
		Reporter: collectPipelineEvents(&events),
	})
	if err != nil {
		t.Fatalf("RunPipeline() error = %v", err)
	}
	if result.BatchSummary.Batches != 1 {
		t.Fatalf("BatchSummary.Batches = %d, want 1", result.BatchSummary.Batches)
	}
	assertExists(t, filepath.Join(tempDir, "batch_export", "validation_batch_1.parquet"))
	assertNoPipelineEventPhase(t, events, progress.PhaseSplit)
	assertNoPipelineEventPhase(t, events, progress.PhaseValidate)
}

func TestRunPipelineValidateBatchExecutesValidationThenBatch(t *testing.T) {
	tempDir := t.TempDir()
	_, schemaPath := writePipelineInputFixture(t, tempDir)
	inputDir := filepath.Join(tempDir, "split")
	writeTestFile(t, filepath.Join(inputDir, "1.csv"), "Record ID,Amount\n1,10\n")

	result, err := New().RunPipeline(context.Background(), PipelineOptions{
		Phases: []PipelinePhase{PipelinePhaseValidate, PipelinePhaseBatch},
		Validate: ValidateOptions{
			SchemaPath: schemaPath,
			InputDir:   inputDir,
			Threads:    1,
			SuccessDir: filepath.Join(tempDir, "success"),
			ErrorDir:   filepath.Join(tempDir, "errors"),
		},
		Batch: BatchOptions{
			OutputDir: filepath.Join(tempDir, "batch_export"),
			BatchSize: 1,
			Workers:   1,
		},
	})
	if err != nil {
		t.Fatalf("RunPipeline() error = %v", err)
	}
	assertPipelinePhases(t, result.RanPhases, []PipelinePhase{PipelinePhaseValidate, PipelinePhaseBatch})
	if result.ValidationDir == nil || result.BatchSummary.Batches != 1 {
		t.Fatalf("result = %+v, want validation and batch results", result)
	}
}

func TestRunPipelineSplitValidateExecutesSplitThenValidation(t *testing.T) {
	tempDir := t.TempDir()
	inputPath, schemaPath := writePipelineInputFixture(t, tempDir)

	result, err := New().RunPipeline(context.Background(), PipelineOptions{
		Phases: []PipelinePhase{PipelinePhaseSplit, PipelinePhaseValidate},
		Split: SplitOptions{
			InputPath:       inputPath,
			OutputDir:       filepath.Join(tempDir, "split"),
			PrimaryKey:      "Record ID",
			MaxOpenWriters:  8,
			MissingKeysFile: "missing_keys.csv",
		},
		Validate: ValidateOptions{
			SchemaPath: schemaPath,
			Threads:    1,
			SuccessDir: filepath.Join(tempDir, "success"),
			ErrorDir:   filepath.Join(tempDir, "errors"),
		},
	})
	if err != nil {
		t.Fatalf("RunPipeline() error = %v", err)
	}
	assertPipelinePhases(t, result.RanPhases, []PipelinePhase{PipelinePhaseSplit, PipelinePhaseValidate})
	if result.ValidationDir == nil || result.ValidationDir.Summary.Files != 2 {
		t.Fatalf("ValidationDir = %+v, want two split files validated", result.ValidationDir)
	}
	assertExists(t, filepath.Join(tempDir, "success", "1.parquet"))
	assertExists(t, filepath.Join(tempDir, "success", "2.parquet"))
}

func TestRunPipelineFullMatchesRunAutoBehavior(t *testing.T) {
	pipelineDir := t.TempDir()
	pipelineInput, pipelineSchema := writePipelineInputFixture(t, pipelineDir)
	pipelineResult, err := New().RunPipeline(context.Background(), fullPipelineOptions(pipelineDir, pipelineInput, pipelineSchema))
	if err != nil {
		t.Fatalf("RunPipeline() error = %v", err)
	}

	autoDir := t.TempDir()
	autoInput, autoSchema := writePipelineInputFixture(t, autoDir)
	autoResult, err := New().RunAuto(context.Background(), autoTestOptions(autoDir, autoInput, autoSchema))
	if err != nil {
		t.Fatalf("RunAuto() error = %v", err)
	}

	if pipelineResult.SplitSummary.TotalRows != autoResult.SplitSummary.TotalRows {
		t.Fatalf("split total rows = %d, want %d", pipelineResult.SplitSummary.TotalRows, autoResult.SplitSummary.TotalRows)
	}
	if pipelineResult.ValidationDir == nil {
		t.Fatal("pipeline validation result missing")
	}
	if pipelineResult.ValidationDir.Summary.Files != autoResult.Validation.Summary.Files {
		t.Fatalf("validated files = %d, want %d", pipelineResult.ValidationDir.Summary.Files, autoResult.Validation.Summary.Files)
	}
	if pipelineResult.BatchSummary.Batches != autoResult.BatchSummary.Batches {
		t.Fatalf("batch count = %d, want %d", pipelineResult.BatchSummary.Batches, autoResult.BatchSummary.Batches)
	}
}

func TestRunPipelineInvalidPhaseOrderRejectedBeforeExecution(t *testing.T) {
	tempDir := t.TempDir()
	inputPath, schemaPath := writePipelineInputFixture(t, tempDir)
	var events []progress.Event

	_, err := New().RunPipeline(context.Background(), PipelineOptions{
		Phases: []PipelinePhase{PipelinePhaseSplit, PipelinePhaseBatch},
		Split: SplitOptions{
			InputPath:  inputPath,
			OutputDir:  filepath.Join(tempDir, "split"),
			PrimaryKey: "Record ID",
		},
		Validate: ValidateOptions{
			SchemaPath: schemaPath,
			SuccessDir: filepath.Join(tempDir, "success"),
			ErrorDir:   filepath.Join(tempDir, "errors"),
		},
		Batch: BatchOptions{
			InputDir:  filepath.Join(tempDir, "success"),
			OutputDir: filepath.Join(tempDir, "batch_export"),
		},
		Reporter: collectPipelineEvents(&events),
	})
	if err == nil {
		t.Fatal("RunPipeline() error = nil, want unsupported sequence error")
	}
	if !strings.Contains(err.Error(), "unsupported pipeline phase sequence") {
		t.Fatalf("RunPipeline() error = %v, want unsupported sequence", err)
	}
	if len(events) != 0 {
		t.Fatalf("events emitted before validation failure: %+v", events)
	}
	assertNotExists(t, filepath.Join(tempDir, "split"))
}

func TestRunPipelineSplitCacheReuseHonoredWhenConfigured(t *testing.T) {
	tempDir := t.TempDir()
	inputPath, _ := writePipelineInputFixture(t, tempDir)
	opts := PipelineOptions{
		Phases:          []PipelinePhase{PipelinePhaseSplit},
		ResumePolicy:    PipelineResumePolicyReuseValidOutputs,
		ReuseSplitCache: true,
		Split: SplitOptions{
			InputPath:       inputPath,
			OutputDir:       filepath.Join(tempDir, "split"),
			PrimaryKey:      "Record ID",
			MaxOpenWriters:  8,
			MissingKeysFile: "missing_keys.csv",
		},
	}

	firstResult, err := New().RunPipeline(context.Background(), opts)
	if err != nil {
		t.Fatalf("RunPipeline() first run error = %v", err)
	}
	if firstResult.SplitReused {
		t.Fatal("first split unexpectedly reused cache")
	}
	if _, err := os.Stat(splitcsv.CacheMetadataPath(opts.Split.OutputDir)); err != nil {
		t.Fatalf("split cache metadata missing: %v", err)
	}

	sentinel := filepath.Join(opts.Split.OutputDir, "keep.txt")
	writeTestFile(t, sentinel, "keep")
	secondResult, err := New().RunPipeline(context.Background(), opts)
	if err != nil {
		t.Fatalf("RunPipeline() second run error = %v", err)
	}
	if !secondResult.SplitReused {
		t.Fatal("second split did not reuse cache")
	}
	assertExists(t, sentinel)
}

func TestRunPipelineClearOutputsRemovesSplitCacheAndDownstreamOutputs(t *testing.T) {
	tempDir := t.TempDir()
	inputPath, schemaPath := writePipelineInputFixture(t, tempDir)
	opts := fullPipelineOptions(tempDir, inputPath, schemaPath)

	firstResult, err := New().RunPipeline(context.Background(), opts)
	if err != nil {
		t.Fatalf("RunPipeline() first run error = %v", err)
	}
	if firstResult.SplitReused {
		t.Fatal("first split unexpectedly reused cache")
	}

	splitSentinel := filepath.Join(opts.Split.OutputDir, "stale.csv")
	successSentinel := filepath.Join(opts.Validate.SuccessDir, "stale.parquet")
	errorSentinel := filepath.Join(opts.Validate.ErrorDir, "stale.parquet")
	batchSentinel := filepath.Join(opts.Batch.OutputDir, "stale.parquet")
	writeTestFile(t, splitSentinel, "stale")
	writeTestFile(t, successSentinel, "stale")
	writeTestFile(t, errorSentinel, "stale")
	writeTestFile(t, batchSentinel, "stale")

	opts.ClearValidationOutputDirs = true
	opts.ClearSplitOutputDir = true
	secondResult, err := New().RunPipeline(context.Background(), opts)
	if err != nil {
		t.Fatalf("RunPipeline() second run error = %v", err)
	}
	if secondResult.SplitReused {
		t.Fatal("second split reused cache after clear outputs")
	}
	assertNotExists(t, splitSentinel)
	assertNotExists(t, successSentinel)
	assertNotExists(t, errorSentinel)
	assertNotExists(t, batchSentinel)
	assertExists(t, splitcsv.CacheMetadataPath(opts.Split.OutputDir))
}

func writePipelineInputFixture(t *testing.T, tempDir string) (string, string) {
	t.Helper()
	inputPath := filepath.Join(tempDir, "input.csv")
	schemaPath := filepath.Join(tempDir, "schema.json")
	writeTestFile(t, inputPath, "Record ID,Amount\n1,10\n2,20\n")
	writeTestFile(t, schemaPath, `{"fields":[{"name":"Record ID","type":"string","required":true},{"name":"Amount","type":"int","required":true}]}`)
	return inputPath, schemaPath
}

func createPipelineParquetDir(t *testing.T, tempDir string) string {
	t.Helper()
	_, schemaPath := writePipelineInputFixture(t, tempDir)
	inputDir := filepath.Join(tempDir, "csv")
	writeTestFile(t, filepath.Join(inputDir, "1.csv"), "Record ID,Amount\n1,10\n")
	parquetDir := filepath.Join(tempDir, "success")
	_, err := New().RunPipeline(context.Background(), PipelineOptions{
		Phases: []PipelinePhase{PipelinePhaseValidate},
		Validate: ValidateOptions{
			SchemaPath: schemaPath,
			InputDir:   inputDir,
			Threads:    1,
			SuccessDir: parquetDir,
			ErrorDir:   filepath.Join(tempDir, "errors"),
		},
	})
	if err != nil {
		t.Fatalf("RunPipeline() setup validation error = %v", err)
	}
	return parquetDir
}

func fullPipelineOptions(tempDir, inputPath, schemaPath string) PipelineOptions {
	return PipelineOptions{
		Phases:          []PipelinePhase{PipelinePhaseSplit, PipelinePhaseValidate, PipelinePhaseBatch},
		ResumePolicy:    PipelineResumePolicyReuseValidOutputs,
		ReuseSplitCache: true,
		Split: SplitOptions{
			InputPath:       inputPath,
			OutputDir:       filepath.Join(tempDir, "split"),
			PrimaryKey:      "Record ID",
			MaxOpenWriters:  16,
			MissingKeysFile: "missing_keys.csv",
		},
		Validate: ValidateOptions{
			SchemaPath: schemaPath,
			Threads:    1,
			SuccessDir: filepath.Join(tempDir, "success"),
			ErrorDir:   filepath.Join(tempDir, "errors"),
		},
		Batch: BatchOptions{
			OutputDir: filepath.Join(tempDir, "batch_export"),
			BatchSize: 1000,
			Workers:   1,
		},
		Mode: "auto",
	}
}

func collectPipelineEvents(events *[]progress.Event) progress.Reporter {
	return progress.FuncReporter(func(event progress.Event) {
		*events = append(*events, event)
	})
}

func assertPipelinePhases(t *testing.T, got, want []PipelinePhase) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("phases = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("phases = %v, want %v", got, want)
		}
	}
}

func assertNoPipelineEventPhase(t *testing.T, events []progress.Event, phase string) {
	t.Helper()
	for _, event := range events {
		if event.Phase == phase {
			t.Fatalf("unexpected event for phase %q: %+v", phase, event)
		}
	}
}
