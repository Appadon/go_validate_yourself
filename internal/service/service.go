package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"go_validate_yourself/internal/batchparquet"
	"go_validate_yourself/internal/progress"
	"go_validate_yourself/internal/splitcsv"
	"go_validate_yourself/internal/validator"
)

const defaultSchemaPath = "policy_schema.json"

/* Service coordinates high-level split, validate, and batch workflows. */
type Service struct{}

/* SplitOptions defines inputs for split mode orchestration. */
type SplitOptions struct {
	InputPath       string
	OutputDir       string
	PrimaryKey      string
	MaxOpenWriters  int
	MissingKeysFile string
	RunID           string
	Reporter        progress.Reporter
}

/* ValidateOptions defines inputs for single-file or directory validation. */
type ValidateOptions struct {
	SchemaPath      string
	InputCSV        string
	InputDir        string
	Threads         int
	WriteEmptyError bool
	SuccessDir      string
	ErrorDir        string
	RunID           string
	Reporter        progress.Reporter
}

/* BatchOptions defines inputs for parquet batch export. */
type BatchOptions struct {
	InputDir       string
	OutputDir      string
	BatchSize      int
	Workers        int
	ClearOutputDir bool
	RunID          string
	Reporter       progress.Reporter
}

/* AutoOptions defines the full split + validate + batch workflow. */
type AutoOptions struct {
	MainInputCSV         string
	SchemaPath           string
	SplitOutputDir       string
	SplitPrimaryKey      string
	SplitMaxOpen         int
	SplitMissingFile     string
	Threads              int
	WriteEmptyError      bool
	ClearValidationCache bool
	SuccessDir           string
	ErrorDir             string
	BatchDir             string
	BatchExportDir       string
	BatchSize            int
	RunID                string
	Reporter             progress.Reporter
}

/* ValidationResult captures outputs for one validated file. */
type ValidationResult struct {
	InputPath   string          `json:"input_path"`
	ParquetPath string          `json:"parquet_path"`
	ErrorPath   string          `json:"error_path"`
	Stats       validator.Stats `json:"stats"`
}

/* DirectoryValidationResult captures outputs for directory validation. */
type DirectoryValidationResult struct {
	InputDir   string                     `json:"input_dir"`
	FileCount  int                        `json:"file_count"`
	Summary    validator.DirectorySummary `json:"summary"`
	SuccessDir string                     `json:"success_dir"`
	ErrorDir   string                     `json:"error_dir"`
}

/* AutoResult captures the combined outputs of the auto workflow. */
type AutoResult struct {
	MainInputCSV    string                    `json:"main_input_csv"`
	SchemaPath      string                    `json:"schema_path"`
	SplitPrimaryKey string                    `json:"split_primary_key"`
	SplitReused     bool                      `json:"split_reused"`
	SplitSummary    splitcsv.Summary          `json:"split_summary"`
	Validation      DirectoryValidationResult `json:"validation"`
	BatchSummary    batchparquet.Summary      `json:"batch_summary"`
}

/* New returns a stateless workflow service. */
func New() Service {
	return Service{}
}

/* DefaultThreadCount returns the default worker count used by the engine. */
func DefaultThreadCount() int {
	cpus := runtime.NumCPU()
	threads := int(float64(cpus) * 0.6)
	if threads < 1 {
		return 1
	}
	return threads
}

/* ResolveDefaultSchemaPath returns the default schema file when it exists. */
func ResolveDefaultSchemaPath() (string, error) {
	if _, err := os.Stat(defaultSchemaPath); err == nil {
		return defaultSchemaPath, nil
	}
	return "", fmt.Errorf("missing schema; pass -schema <path> (default %q not found)", defaultSchemaPath)
}

/* DetectPrimaryKey reads the first source header and uses it as the split key. */
func DetectPrimaryKey(inputPath string) (string, error) {
	header, err := splitcsv.Header(inputPath)
	if err != nil {
		return "", err
	}
	if len(header) == 0 {
		return "", fmt.Errorf("input %q has no header columns", inputPath)
	}

	selected := strings.TrimSpace(header[0])
	if selected == "" {
		return "", fmt.Errorf("first header column is blank")
	}
	return selected, nil
}

/* LoadSchema loads and validates a schema JSON document. */
func LoadSchema(schemaPath string) (validator.SchemaConfig, error) {
	schema, err := validator.LoadSchema(schemaPath)
	if err != nil {
		return validator.SchemaConfig{}, fmt.Errorf("failed loading schema: %w", err)
	}
	if err := validator.ValidateSchema(&schema); err != nil {
		return validator.SchemaConfig{}, fmt.Errorf("invalid schema: %w", err)
	}
	return schema, nil
}

/* RunSplit executes split mode and returns split metrics. */
func (Service) RunSplit(ctx context.Context, opts SplitOptions) (splitcsv.Summary, error) {
	ctx = ensureContext(ctx)
	emitter := progress.NewEmitter(opts.RunID, opts.Reporter)
	emitter.Started(progress.PhaseRun, "validation run started", map[string]any{
		"mode":  "split",
		"phase": progress.PhaseSplit,
	})
	summary, err := runSplitPhase(ctx, opts, emitter)
	if err != nil {
		emitter.Failed(progress.PhaseRun, err.Error(), map[string]any{
			"mode":  "split",
			"phase": progress.PhaseSplit,
		})
		return splitcsv.Summary{}, err
	}
	emitter.Completed(progress.PhaseRun, "validation run complete", map[string]any{
		"mode":  "split",
		"phase": progress.PhaseSplit,
	})
	return summary, nil
}

func runSplitPhase(ctx context.Context, opts SplitOptions, emitter progress.Emitter) (splitcsv.Summary, error) {
	primaryKey := strings.TrimSpace(opts.PrimaryKey)
	if primaryKey == "" {
		return splitcsv.Summary{}, fmt.Errorf("missing split primary key")
	}
	maxOpen := opts.MaxOpenWriters
	if maxOpen < 1 {
		maxOpen = 1
	}

	emitter.Started(progress.PhaseSplit, fmt.Sprintf("starting split phase [input %s] [output_dir %s] [primary_key %q]", opts.InputPath, opts.OutputDir, primaryKey), map[string]any{
		"input_path":        opts.InputPath,
		"output_dir":        opts.OutputDir,
		"primary_key":       primaryKey,
		"max_open_writers":  maxOpen,
		"missing_keys_file": opts.MissingKeysFile,
	})

	if err := ctx.Err(); err != nil {
		return splitcsv.Summary{}, err
	}

	summary, err := splitcsv.SplitByPrimaryKey(ctx, splitcsv.Config{
		InputPath:       opts.InputPath,
		OutputDir:       opts.OutputDir,
		PrimaryKey:      primaryKey,
		MaxOpenWriters:  maxOpen,
		MissingKeysFile: opts.MissingKeysFile,
		Progress:        emitter,
	})
	if err != nil {
		emitter.Failed(progress.PhaseSplit, fmt.Sprintf("split failed: %v", err), map[string]any{
			"input_path": opts.InputPath,
			"output_dir": opts.OutputDir,
		})
		return splitcsv.Summary{}, fmt.Errorf("split failed: %w", err)
	}

	emitter.Completed(progress.PhaseSplit, fmt.Sprintf("split complete [total %d] [missing_key_rows %d] [files %d] [out_dir %s]", summary.TotalRows, summary.MissingKeyRows, summary.OutputFiles, opts.OutputDir), map[string]any{
		"total_rows":       summary.TotalRows,
		"split_rows":       summary.SplitRows,
		"missing_key_rows": summary.MissingKeyRows,
		"output_files":     summary.OutputFiles,
		"output_dir":       opts.OutputDir,
	})
	return summary, nil
}

/* RunValidateFile validates one CSV or Parquet file and writes parquet and error outputs. */
func (Service) RunValidateFile(ctx context.Context, opts ValidateOptions) (ValidationResult, error) {
	ctx = ensureContext(ctx)
	emitter := progress.NewEmitter(opts.RunID, opts.Reporter)
	emitter.Started(progress.PhaseRun, "validation run started", map[string]any{
		"mode":  "validate-file",
		"phase": progress.PhaseValidate,
	})
	result, err := runValidateFilePhase(ctx, opts, emitter)
	if err != nil {
		emitter.Failed(progress.PhaseRun, err.Error(), map[string]any{
			"mode":  "validate-file",
			"phase": progress.PhaseValidate,
		})
		return ValidationResult{}, err
	}
	emitter.Completed(progress.PhaseRun, "validation run complete", map[string]any{
		"mode":  "validate-file",
		"phase": progress.PhaseValidate,
	})
	return result, nil
}

func runValidateFilePhase(ctx context.Context, opts ValidateOptions, emitter progress.Emitter) (ValidationResult, error) {
	if err := createOutputDirs(opts.SuccessDir, opts.ErrorDir); err != nil {
		return ValidationResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return ValidationResult{}, err
	}
	schema, err := LoadSchema(opts.SchemaPath)
	if err != nil {
		return ValidationResult{}, err
	}

	emitter.Started(progress.PhaseValidate, fmt.Sprintf("starting single-file validation [input %s] [schema %s]", opts.InputCSV, opts.SchemaPath), map[string]any{
		"input_path":        opts.InputCSV,
		"schema_path":       opts.SchemaPath,
		"success_dir":       opts.SuccessDir,
		"error_dir":         opts.ErrorDir,
		"write_empty_error": opts.WriteEmptyError,
	})

	parquetPath, errorPath := validator.OutputPaths(opts.InputCSV, opts.SuccessDir, opts.ErrorDir)
	stats, err := validator.RunValidationAndWriteParquet(ctx, opts.InputCSV, parquetPath, errorPath, schema, opts.WriteEmptyError)
	if err != nil {
		emitter.Failed(progress.PhaseValidate, fmt.Sprintf("single-file validation failed: %v", err), map[string]any{
			"input_path": opts.InputCSV,
		})
		return ValidationResult{}, fmt.Errorf("processing failed: %w", err)
	}

	emitter.Completed(progress.PhaseValidate, fmt.Sprintf("single-file complete total=%d valid=%d invalid=%d written=%s errors=%s",
		stats.TotalRows,
		stats.ValidRows,
		stats.InvalidRows,
		parquetPath,
		errorPath,
	), map[string]any{
		"input_path":      opts.InputCSV,
		"parquet_path":    parquetPath,
		"error_path":      errorPath,
		"total_rows":      stats.TotalRows,
		"valid_rows":      stats.ValidRows,
		"invalid_rows":    stats.InvalidRows,
		"write_empty_err": opts.WriteEmptyError,
	})

	return ValidationResult{
		InputPath:   opts.InputCSV,
		ParquetPath: parquetPath,
		ErrorPath:   errorPath,
		Stats:       stats,
	}, nil
}

/* RunValidateDir validates all supported data files in a directory using a worker pool. */
func (Service) RunValidateDir(ctx context.Context, opts ValidateOptions) (DirectoryValidationResult, error) {
	ctx = ensureContext(ctx)
	emitter := progress.NewEmitter(opts.RunID, opts.Reporter)
	emitter.Started(progress.PhaseRun, "validation run started", map[string]any{
		"mode":  "validate-dir",
		"phase": progress.PhaseValidate,
	})
	result, err := runValidateDirPhase(ctx, opts, emitter)
	if err != nil {
		emitter.Failed(progress.PhaseRun, err.Error(), map[string]any{
			"mode":  "validate-dir",
			"phase": progress.PhaseValidate,
		})
		return result, err
	}
	emitter.Completed(progress.PhaseRun, "validation run complete", map[string]any{
		"mode":  "validate-dir",
		"phase": progress.PhaseValidate,
	})
	return result, nil
}

func runValidateDirPhase(ctx context.Context, opts ValidateOptions, emitter progress.Emitter) (DirectoryValidationResult, error) {
	if err := createOutputDirs(opts.SuccessDir, opts.ErrorDir); err != nil {
		return DirectoryValidationResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return DirectoryValidationResult{}, err
	}
	schema, err := LoadSchema(opts.SchemaPath)
	if err != nil {
		return DirectoryValidationResult{}, err
	}

	workers := opts.Threads
	if workers < 1 {
		workers = 1
	}

	files, err := validator.ListCSVFiles(opts.InputDir)
	if err != nil {
		return DirectoryValidationResult{}, fmt.Errorf("failed listing validation input files: %w", err)
	}
	if len(files) == 0 {
		return DirectoryValidationResult{}, fmt.Errorf("no csv or parquet files found in directory: %s", opts.InputDir)
	}

	emitter.Started(progress.PhaseValidate, fmt.Sprintf("starting directory validation [files %d] [workers %d]", len(files), workers), map[string]any{
		"input_dir":         opts.InputDir,
		"schema_path":       opts.SchemaPath,
		"files_total":       len(files),
		"workers":           workers,
		"success_dir":       opts.SuccessDir,
		"error_dir":         opts.ErrorDir,
		"write_empty_error": opts.WriteEmptyError,
	})

	summary, err := validator.ProcessDirectory(ctx, files, workers, opts.SuccessDir, opts.ErrorDir, schema, opts.WriteEmptyError, emitter)
	if err != nil {
		emitter.Failed(progress.PhaseValidate, fmt.Sprintf("directory validation failed: %v", err), map[string]any{
			"input_dir":    opts.InputDir,
			"files_total":  len(files),
			"failed_files": summary.FailedFiles,
			"total_rows":   summary.TotalRows,
			"valid_rows":   summary.ValidRows,
			"invalid_rows": summary.InvalidRows,
		})
		return DirectoryValidationResult{
			InputDir:   opts.InputDir,
			FileCount:  len(files),
			Summary:    summary,
			SuccessDir: opts.SuccessDir,
			ErrorDir:   opts.ErrorDir,
		}, err
	}
	emitter.Completed(progress.PhaseValidate, fmt.Sprintf("directory complete files=%d failed_files=%d total=%d valid=%d invalid=%d workers=%d",
		summary.Files,
		summary.FailedFiles,
		summary.TotalRows,
		summary.ValidRows,
		summary.InvalidRows,
		workers,
	), map[string]any{
		"input_dir":    opts.InputDir,
		"files":        summary.Files,
		"failed_files": summary.FailedFiles,
		"total_rows":   summary.TotalRows,
		"valid_rows":   summary.ValidRows,
		"invalid_rows": summary.InvalidRows,
		"workers":      workers,
	})
	if summary.FailedFiles > 0 {
		return DirectoryValidationResult{
			InputDir:   opts.InputDir,
			FileCount:  len(files),
			Summary:    summary,
			SuccessDir: opts.SuccessDir,
			ErrorDir:   opts.ErrorDir,
		}, fmt.Errorf("directory validation completed with %d failed file(s)", summary.FailedFiles)
	}

	return DirectoryValidationResult{
		InputDir:   opts.InputDir,
		FileCount:  len(files),
		Summary:    summary,
		SuccessDir: opts.SuccessDir,
		ErrorDir:   opts.ErrorDir,
	}, nil
}

/* RunBatch exports parquet files into fixed-size parquet batches. */
func (Service) RunBatch(ctx context.Context, opts BatchOptions) (batchparquet.Summary, error) {
	ctx = ensureContext(ctx)
	emitter := progress.NewEmitter(opts.RunID, opts.Reporter)
	emitter.Started(progress.PhaseRun, "validation run started", map[string]any{
		"mode":  "batch",
		"phase": progress.PhaseBatch,
	})
	summary, err := runBatchPhase(ctx, opts, emitter)
	if err != nil {
		emitter.Failed(progress.PhaseRun, err.Error(), map[string]any{
			"mode":  "batch",
			"phase": progress.PhaseBatch,
		})
		return batchparquet.Summary{}, err
	}
	emitter.Completed(progress.PhaseRun, "validation run complete", map[string]any{
		"mode":  "batch",
		"phase": progress.PhaseBatch,
	})
	return summary, nil
}

func runBatchPhase(ctx context.Context, opts BatchOptions, emitter progress.Emitter) (batchparquet.Summary, error) {
	if opts.ClearOutputDir {
		emitter.Log(progress.PhaseRun, fmt.Sprintf("clearing batch export directory: %s", opts.OutputDir), map[string]any{
			"output_dir": opts.OutputDir,
		})
		emitter.Log(progress.PhaseRun, "this might take a while depending on the size of the cache", nil)
		if err := os.RemoveAll(opts.OutputDir); err != nil {
			return batchparquet.Summary{}, fmt.Errorf("failed clearing batch export dir %q: %w", opts.OutputDir, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return batchparquet.Summary{}, err
	}

	summary, err := batchparquet.BatchDirectory(ctx, opts.InputDir, opts.OutputDir, normalizeBatchSize(opts.BatchSize), normalizeWorkers(opts.Workers), emitter)
	if err != nil {
		emitter.Failed(progress.PhaseBatch, fmt.Sprintf("batch phase failed: %v", err), map[string]any{
			"input_dir":  opts.InputDir,
			"output_dir": opts.OutputDir,
		})
		return batchparquet.Summary{}, fmt.Errorf("batch phase failed: %w", err)
	}

	emitter.Completed(progress.PhaseBatch, fmt.Sprintf("batch complete files=%d batches=%d total_rows=%d batch_size=%d workers=%d out_dir=%s",
		summary.InputFiles,
		summary.Batches,
		summary.TotalRows,
		summary.BatchSize,
		summary.Workers,
		summary.OutputDir,
	), map[string]any{
		"input_files": summary.InputFiles,
		"batches":     summary.Batches,
		"total_rows":  summary.TotalRows,
		"batch_size":  summary.BatchSize,
		"workers":     summary.Workers,
		"output_dir":  summary.OutputDir,
	})
	return summary, nil
}

/* RunAuto executes the legacy split, directory validation, and batch workflow. */
func (s Service) RunAuto(ctx context.Context, opts AutoOptions) (AutoResult, error) {
	pipelineResult, err := s.RunPipeline(ctx, PipelineOptions{
		Phases:       []PipelinePhase{PipelinePhaseSplit, PipelinePhaseValidate, PipelinePhaseBatch},
		ResumePolicy: PipelineResumePolicyReuseValidOutputs,
		Split: SplitOptions{
			InputPath:       opts.MainInputCSV,
			OutputDir:       opts.SplitOutputDir,
			PrimaryKey:      opts.SplitPrimaryKey,
			MaxOpenWriters:  opts.SplitMaxOpen,
			MissingKeysFile: opts.SplitMissingFile,
		},
		Validate: ValidateOptions{
			SchemaPath:      opts.SchemaPath,
			InputDir:        opts.SplitOutputDir,
			Threads:         normalizeWorkers(opts.Threads),
			WriteEmptyError: opts.WriteEmptyError,
			SuccessDir:      opts.SuccessDir,
			ErrorDir:        opts.ErrorDir,
		},
		Batch: BatchOptions{
			InputDir:       resolveAutoBatchDir(opts),
			OutputDir:      opts.BatchExportDir,
			BatchSize:      opts.BatchSize,
			Workers:        normalizeWorkers(opts.Threads),
			ClearOutputDir: false,
		},
		ReuseSplitCache:           true,
		ClearValidationOutputDirs: opts.ClearValidationCache,
		RunID:                     opts.RunID,
		Reporter:                  opts.Reporter,
		Mode:                      "auto",
	})

	result := AutoResult{
		MainInputCSV:    opts.MainInputCSV,
		SchemaPath:      opts.SchemaPath,
		SplitPrimaryKey: pipelineResult.SplitPrimaryKey,
		SplitReused:     pipelineResult.SplitReused,
		SplitSummary:    pipelineResult.SplitSummary,
		BatchSummary:    pipelineResult.BatchSummary,
	}
	if pipelineResult.ValidationDir != nil {
		result.Validation = *pipelineResult.ValidationDir
	}
	return result, err
}

/* prepareAutoSplit reuses an existing split directory when the input hash matches. */
func (s Service) prepareAutoSplit(ctx context.Context, opts AutoOptions, primaryKey string, emitter progress.Emitter) (splitcsv.Summary, bool, error) {
	if err := ctx.Err(); err != nil {
		return splitcsv.Summary{}, false, err
	}
	inputHash, err := splitcsv.HashFile(opts.MainInputCSV)
	if err != nil {
		return splitcsv.Summary{}, false, fmt.Errorf("compute split cache hash: %w", err)
	}

	meta, err := splitcsv.ReadCacheMetadata(opts.SplitOutputDir)
	switch {
	case err == nil:
		if meta.Matches(inputHash, primaryKey, opts.SplitMissingFile) {
			emitter.Log(progress.PhaseRun, fmt.Sprintf("reusing split cache [output_dir %s] [input_hash %s]", opts.SplitOutputDir, inputHash[:12]), map[string]any{
				"output_dir": opts.SplitOutputDir,
				"input_hash": inputHash,
			})
			return splitcsv.Summary{}, true, nil
		}
		emitter.Log(progress.PhaseRun, fmt.Sprintf("split cache miss [output_dir %s] [reason input or split settings changed]", opts.SplitOutputDir), map[string]any{
			"output_dir": opts.SplitOutputDir,
			"reason":     "input or split settings changed",
		})
	case errors.Is(err, os.ErrNotExist):
		emitter.Log(progress.PhaseRun, fmt.Sprintf("split cache miss [output_dir %s] [reason no cache metadata found]", opts.SplitOutputDir), map[string]any{
			"output_dir": opts.SplitOutputDir,
			"reason":     "no cache metadata found",
		})
	default:
		emitter.Log(progress.PhaseRun, fmt.Sprintf("split cache metadata unreadable, rebuilding split output [output_dir %s] [error %s]", opts.SplitOutputDir, err), map[string]any{
			"output_dir": opts.SplitOutputDir,
			"error":      err.Error(),
		})
	}

	if err := os.RemoveAll(opts.SplitOutputDir); err != nil {
		return splitcsv.Summary{}, false, fmt.Errorf("failed clearing split output dir %q: %w", opts.SplitOutputDir, err)
	}

	summary, err := runSplitPhase(ctx, SplitOptions{
		InputPath:       opts.MainInputCSV,
		OutputDir:       opts.SplitOutputDir,
		PrimaryKey:      primaryKey,
		MaxOpenWriters:  opts.SplitMaxOpen,
		MissingKeysFile: opts.SplitMissingFile,
	}, emitter)
	if err != nil {
		return splitcsv.Summary{}, false, err
	}

	if err := splitcsv.WriteCacheMetadata(opts.SplitOutputDir, splitcsv.CacheMetadata{
		InputPath:       opts.MainInputCSV,
		InputHash:       inputHash,
		PrimaryKey:      primaryKey,
		MissingKeysFile: opts.SplitMissingFile,
		CreatedAt:       time.Now(),
	}); err != nil {
		return splitcsv.Summary{}, false, fmt.Errorf("write split cache metadata: %w", err)
	}
	return summary, false, nil
}

func ensureContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

/* createOutputDirs ensures validation output directories exist before execution. */
func createOutputDirs(successDir, errorDir string) error {
	if err := os.MkdirAll(successDir, 0o755); err != nil {
		return fmt.Errorf("failed creating success dir: %w", err)
	}
	if err := os.MkdirAll(errorDir, 0o755); err != nil {
		return fmt.Errorf("failed creating errors dir: %w", err)
	}
	return nil
}

/* clearPipelineOutputDirs removes prior artifacts for selected pipeline phases. */
func clearPipelineOutputDirs(opts PipelineOptions) error {
	for _, dir := range outputDirsToClear(opts) {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("failed clearing output dir %q: %w", dir, err)
		}
	}
	return nil
}

func outputDirsToClear(opts PipelineOptions) []string {
	dirs := make([]string, 0, 4)
	if opts.ClearSplitOutputDir && containsPipelinePhase(opts.Phases, PipelinePhaseSplit) {
		dirs = append(dirs, opts.Split.OutputDir)
	}
	if opts.ClearValidationOutputDirs && containsPipelinePhase(opts.Phases, PipelinePhaseValidate) {
		dirs = append(dirs, opts.Validate.SuccessDir, opts.Validate.ErrorDir)
	}
	if opts.ClearValidationOutputDirs && containsPipelinePhase(opts.Phases, PipelinePhaseBatch) {
		dirs = append(dirs, opts.Batch.OutputDir)
	}

	seen := make(map[string]struct{}, len(dirs))
	deduped := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		dir = strings.TrimSpace(dir)
		if dir == "" {
			continue
		}
		if _, ok := seen[dir]; ok {
			continue
		}
		seen[dir] = struct{}{}
		deduped = append(deduped, dir)
	}
	return deduped
}

/* normalizeBatchSize applies the lower bound used by batch mode. */
func normalizeBatchSize(batchSize int) int {
	if batchSize < 1 {
		return 1
	}
	return batchSize
}

/* normalizeWorkers applies a minimum worker count of one. */
func normalizeWorkers(workers int) int {
	if workers < 1 {
		return 1
	}
	return workers
}

/* resolveAutoBatchDir returns the batch input directory for auto mode. */
func resolveAutoBatchDir(opts AutoOptions) string {
	if strings.TrimSpace(opts.BatchDir) == "" {
		return opts.SuccessDir
	}
	if abs, err := filepath.Abs(opts.BatchDir); err == nil {
		if successAbs, absErr := filepath.Abs(opts.SuccessDir); absErr == nil && abs == successAbs {
			return opts.SuccessDir
		}
	}
	return opts.BatchDir
}
