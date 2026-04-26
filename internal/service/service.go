package service

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"go_validate_yourself/internal/batchparquet"
	"go_validate_yourself/internal/console"
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
}

/* BatchOptions defines inputs for parquet batch export. */
type BatchOptions struct {
	InputDir       string
	OutputDir      string
	BatchSize      int
	Workers        int
	ClearOutputDir bool
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
}

/* ValidationResult captures outputs for one validated file. */
type ValidationResult struct {
	InputPath    string          `json:"input_path"`
	ParquetPath  string          `json:"parquet_path"`
	ErrorCSVPath string          `json:"error_csv_path"`
	Stats        validator.Stats `json:"stats"`
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

/* DetectPrimaryKey reads the first CSV header and uses it as the split key. */
func DetectPrimaryKey(inputPath string) (string, error) {
	f, err := os.Open(inputPath)
	if err != nil {
		return "", fmt.Errorf("open input: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return "", fmt.Errorf("input %q is empty", inputPath)
		}
		return "", fmt.Errorf("read header: %w", err)
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
	primaryKey := strings.TrimSpace(opts.PrimaryKey)
	if primaryKey == "" {
		return splitcsv.Summary{}, fmt.Errorf("missing split primary key")
	}
	maxOpen := opts.MaxOpenWriters
	if maxOpen < 1 {
		maxOpen = 1
	}

	console.Infof(
		"starting split phase [input %s] [output_dir %s] [primary_key %s]",
		console.GreenValue(opts.InputPath),
		console.GreenValue(opts.OutputDir),
		console.GreenValue(fmt.Sprintf("%q", primaryKey)),
	)

	if err := ctx.Err(); err != nil {
		return splitcsv.Summary{}, err
	}

	summary, err := splitcsv.SplitByPrimaryKey(ctx, splitcsv.Config{
		InputPath:       opts.InputPath,
		OutputDir:       opts.OutputDir,
		PrimaryKey:      primaryKey,
		MaxOpenWriters:  maxOpen,
		MissingKeysFile: opts.MissingKeysFile,
	})
	if err != nil {
		return splitcsv.Summary{}, fmt.Errorf("split failed: %w", err)
	}

	console.Successf(
		"split complete [total %s] [missing_key_rows %s] [files %s] [out_dir %s]",
		console.GreenValue(fmt.Sprintf("%d", summary.TotalRows)),
		console.GreenValue(fmt.Sprintf("%d", summary.MissingKeyRows)),
		console.GreenValue(fmt.Sprintf("%d", summary.OutputFiles)),
		console.GreenValue(opts.OutputDir),
	)
	return summary, nil
}

/* RunValidateFile validates one CSV file and writes parquet and error outputs. */
func (Service) RunValidateFile(ctx context.Context, opts ValidateOptions) (ValidationResult, error) {
	ctx = ensureContext(ctx)
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

	parquetPath, errorCSVPath := validator.OutputPaths(opts.InputCSV, opts.SuccessDir, opts.ErrorDir)
	stats, err := validator.RunValidationAndWriteParquet(ctx, opts.InputCSV, parquetPath, errorCSVPath, schema, opts.WriteEmptyError)
	if err != nil {
		return ValidationResult{}, fmt.Errorf("processing failed: %w", err)
	}

	console.Successf(
		"single-file complete total=%d valid=%d invalid=%d written=%s errors=%s",
		stats.TotalRows,
		stats.ValidRows,
		stats.InvalidRows,
		parquetPath,
		errorCSVPath,
	)

	return ValidationResult{
		InputPath:    opts.InputCSV,
		ParquetPath:  parquetPath,
		ErrorCSVPath: errorCSVPath,
		Stats:        stats,
	}, nil
}

/* RunValidateDir validates all CSV files in a directory using a worker pool. */
func (Service) RunValidateDir(ctx context.Context, opts ValidateOptions) (DirectoryValidationResult, error) {
	ctx = ensureContext(ctx)
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
		return DirectoryValidationResult{}, fmt.Errorf("failed listing csv files: %w", err)
	}
	if len(files) == 0 {
		return DirectoryValidationResult{}, fmt.Errorf("no csv files found in directory: %s", opts.InputDir)
	}

	console.Infof(
		"starting directory validation [files %s] [workers %s]",
		console.GreenValue(fmt.Sprintf("%d", len(files))),
		console.GreenValue(fmt.Sprintf("%d", workers)),
	)

	summary, err := validator.ProcessDirectory(ctx, files, workers, opts.SuccessDir, opts.ErrorDir, schema, opts.WriteEmptyError)
	if err != nil {
		return DirectoryValidationResult{
			InputDir:   opts.InputDir,
			FileCount:  len(files),
			Summary:    summary,
			SuccessDir: opts.SuccessDir,
			ErrorDir:   opts.ErrorDir,
		}, err
	}
	console.Successf(
		"directory complete files=%d failed_files=%d total=%d valid=%d invalid=%d workers=%d",
		summary.Files,
		summary.FailedFiles,
		summary.TotalRows,
		summary.ValidRows,
		summary.InvalidRows,
		workers,
	)
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
	if opts.ClearOutputDir {
		console.Infof("clearing batch export directory: %s", opts.OutputDir)
		console.Infof("this might take a while depending on the size of the cache")
		if err := os.RemoveAll(opts.OutputDir); err != nil {
			return batchparquet.Summary{}, fmt.Errorf("failed clearing batch export dir %q: %w", opts.OutputDir, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return batchparquet.Summary{}, err
	}

	summary, err := batchparquet.BatchDirectory(ctx, opts.InputDir, opts.OutputDir, normalizeBatchSize(opts.BatchSize), normalizeWorkers(opts.Workers))
	if err != nil {
		return batchparquet.Summary{}, fmt.Errorf("batch phase failed: %w", err)
	}

	console.Successf(
		"batch complete files=%d batches=%d total_rows=%d batch_size=%d workers=%d out_dir=%s",
		summary.InputFiles,
		summary.Batches,
		summary.TotalRows,
		summary.BatchSize,
		summary.Workers,
		summary.OutputDir,
	)
	return summary, nil
}

/* RunAuto executes split, directory validation, and batch export in sequence. */
func (s Service) RunAuto(ctx context.Context, opts AutoOptions) (AutoResult, error) {
	ctx = ensureContext(ctx)
	if opts.ClearValidationCache {
		console.Infof(
			"clearing validation cache directories: %s, %s, %s",
			opts.SuccessDir,
			opts.ErrorDir,
			opts.BatchExportDir,
		)
		console.Infof("this might take a while depending on the size of the cache")
		if err := clearValidationOutputDirs(opts.SuccessDir, opts.ErrorDir, opts.BatchExportDir); err != nil {
			return AutoResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return AutoResult{}, err
	}

	primaryKey := strings.TrimSpace(opts.SplitPrimaryKey)
	if primaryKey == "" {
		detected, err := DetectPrimaryKey(opts.MainInputCSV)
		if err != nil {
			return AutoResult{}, fmt.Errorf("failed detecting split primary key: %w", err)
		}
		primaryKey = detected
	}

	splitSummary, splitReused, err := s.prepareAutoSplit(ctx, opts, primaryKey)
	if err != nil {
		return AutoResult{}, err
	}

	validationResult, err := s.RunValidateDir(ctx, ValidateOptions{
		SchemaPath:      opts.SchemaPath,
		InputDir:        opts.SplitOutputDir,
		Threads:         normalizeWorkers(opts.Threads),
		WriteEmptyError: opts.WriteEmptyError,
		SuccessDir:      opts.SuccessDir,
		ErrorDir:        opts.ErrorDir,
	})
	if err != nil {
		return AutoResult{
			MainInputCSV:    opts.MainInputCSV,
			SchemaPath:      opts.SchemaPath,
			SplitPrimaryKey: primaryKey,
			SplitReused:     splitReused,
			SplitSummary:    splitSummary,
			Validation:      validationResult,
		}, err
	}

	batchInputDir := resolveAutoBatchDir(opts)
	batchSummary, err := s.RunBatch(ctx, BatchOptions{
		InputDir:       batchInputDir,
		OutputDir:      opts.BatchExportDir,
		BatchSize:      opts.BatchSize,
		Workers:        normalizeWorkers(opts.Threads),
		ClearOutputDir: false,
	})
	if err != nil {
		return AutoResult{
			MainInputCSV:    opts.MainInputCSV,
			SchemaPath:      opts.SchemaPath,
			SplitPrimaryKey: primaryKey,
			SplitReused:     splitReused,
			SplitSummary:    splitSummary,
			Validation:      validationResult,
		}, err
	}

	return AutoResult{
		MainInputCSV:    opts.MainInputCSV,
		SchemaPath:      opts.SchemaPath,
		SplitPrimaryKey: primaryKey,
		SplitReused:     splitReused,
		SplitSummary:    splitSummary,
		Validation:      validationResult,
		BatchSummary:    batchSummary,
	}, nil
}

/* prepareAutoSplit reuses an existing split directory when the input hash matches. */
func (s Service) prepareAutoSplit(ctx context.Context, opts AutoOptions, primaryKey string) (splitcsv.Summary, bool, error) {
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
			console.Infof(
				"reusing split cache [output_dir %s] [input_hash %s]",
				console.GreenValue(opts.SplitOutputDir),
				console.GreenValue(inputHash[:12]),
			)
			return splitcsv.Summary{}, true, nil
		}
		console.Infof(
			"split cache miss [output_dir %s] [reason input or split settings changed]",
			console.GreenValue(opts.SplitOutputDir),
		)
	case errors.Is(err, os.ErrNotExist):
		console.Infof(
			"split cache miss [output_dir %s] [reason no cache metadata found]",
			console.GreenValue(opts.SplitOutputDir),
		)
	default:
		console.Infof(
			"split cache metadata unreadable, rebuilding split output [output_dir %s] [error %s]",
			console.GreenValue(opts.SplitOutputDir),
			err,
		)
	}

	if err := os.RemoveAll(opts.SplitOutputDir); err != nil {
		return splitcsv.Summary{}, false, fmt.Errorf("failed clearing split output dir %q: %w", opts.SplitOutputDir, err)
	}

	summary, err := s.RunSplit(ctx, SplitOptions{
		InputPath:       opts.MainInputCSV,
		OutputDir:       opts.SplitOutputDir,
		PrimaryKey:      primaryKey,
		MaxOpenWriters:  opts.SplitMaxOpen,
		MissingKeysFile: opts.SplitMissingFile,
	})
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

/* clearValidationOutputDirs removes prior validation and batch artifacts. */
func clearValidationOutputDirs(successDir, errorDir, batchExportDir string) error {
	for _, dir := range []string{successDir, errorDir, batchExportDir} {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("failed clearing output dir %q: %w", dir, err)
		}
	}
	return nil
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
