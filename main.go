package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"rt_policies_validator/internal/splitcsv"
	"rt_policies_validator/internal/validator"
)

/* cliOptions holds parsed command-line flags. */
type cliOptions struct {
	schemaPath       string
	inputDir         string
	threads          int
	successDir       string
	errorDir         string
	splitInput       string
	splitOutputDir   string
	splitPrimaryKey  string
	splitMaxOpen     int
	splitMissingFile string
}

/* main parses arguments and dispatches either split or validation modes. */
func main() {
	opts := parseFlags()
	if strings.TrimSpace(opts.splitInput) != "" {
		runSplitMode(opts.splitInput, opts.splitOutputDir, opts.splitPrimaryKey, opts.splitMissingFile, opts.splitMaxOpen)
		return
	}
	runValidationMode(opts, flag.Args())
}

/* parseFlags parses CLI flags for both validation and split modes. */
func parseFlags() cliOptions {
	opts := cliOptions{}
	flag.StringVar(&opts.schemaPath, "schema", "", "Schema JSON file (required)")
	flag.StringVar(&opts.inputDir, "dir", "", "Directory containing CSV files to validate")
	flag.IntVar(&opts.threads, "t", 1, "Number of concurrent workers for -dir mode")
	flag.StringVar(&opts.successDir, "success-dir", "success", "Directory for valid parquet output")
	flag.StringVar(&opts.errorDir, "error-dir", "errors", "Directory for validation error CSV output")
	flag.StringVar(&opts.splitInput, "split-input", "", "Input CSV file to split by primary key")
	flag.StringVar(&opts.splitOutputDir, "split-output-dir", "split", "Output directory for split CSV files")
	flag.StringVar(&opts.splitPrimaryKey, "split-primary-key", "", "Header name to use as split key")
	flag.IntVar(&opts.splitMaxOpen, "split-max-open", 256, "Maximum number of concurrently open split file writers")
	flag.StringVar(&opts.splitMissingFile, "split-missing-file", "missing_keys.csv", "Name for rows where split key is blank")
	flag.Parse()
	return opts
}

/* runValidationMode validates CLI arguments and executes single-file or directory processing. */
func runValidationMode(opts cliOptions, args []string) {
	normalizeValidationOptions(&opts)
	validateValidationArgs(opts, args)

	writeEmptyError, err := parseWriteEmptyErrorArg(opts.inputDir, args)
	if err != nil {
		exitf("invalid write_empty_error: %v", err)
	}
	createOutputDirs(opts.successDir, opts.errorDir)
	schema := loadAndValidateSchema(opts.schemaPath)

	if opts.inputDir == "" {
		runSingleFileValidation(args[0], opts.successDir, opts.errorDir, schema, writeEmptyError)
		return
	}
	runDirectoryValidation(opts.inputDir, opts.threads, opts.successDir, opts.errorDir, schema, writeEmptyError)
}

/* normalizeValidationOptions applies bounds and defaults for validation mode. */
func normalizeValidationOptions(opts *cliOptions) {
	if opts.threads < 1 {
		opts.threads = 1
	}
}

/* validateValidationArgs checks argument combinations for validation mode. */
func validateValidationArgs(opts cliOptions, args []string) {
	if strings.TrimSpace(opts.schemaPath) == "" {
		exitf("missing required -schema <path>")
	}
	if opts.inputDir != "" && len(args) > 1 {
		exitf("for -dir mode, optional positional is only [write_empty_error]")
	}
	if opts.inputDir == "" && (len(args) < 1 || len(args) > 2) {
		printUsageAndExit(2)
	}
}

/* printUsageAndExit writes CLI usage and exits. */
func printUsageAndExit(code int) {
	fmt.Fprintf(flag.CommandLine.Output(), "Usage:\n  %s [flags] <input.csv> [write_empty_error]\n  %s [flags] -dir <input_dir> [write_empty_error]\n", os.Args[0], os.Args[0])
	flag.PrintDefaults()
	fmt.Fprintf(flag.CommandLine.Output(), "\nOptional positional [write_empty_error]: true|false (default false)\n")
	os.Exit(code)
}

/* createOutputDirs ensures output directories exist before validation starts. */
func createOutputDirs(successDir, errorDir string) {
	if err := os.MkdirAll(successDir, 0o755); err != nil {
		exitf("failed creating success dir: %v", err)
	}
	if err := os.MkdirAll(errorDir, 0o755); err != nil {
		exitf("failed creating errors dir: %v", err)
	}
}

/* loadAndValidateSchema loads schema JSON and validates it for processing. */
func loadAndValidateSchema(schemaPath string) validator.SchemaConfig {
	schema, err := validator.LoadSchema(schemaPath)
	if err != nil {
		exitf("failed loading schema: %v", err)
	}
	if err := validator.ValidateSchema(&schema); err != nil {
		exitf("invalid schema: %v", err)
	}
	return schema
}

/* runSingleFileValidation processes one CSV file and prints row-level summary metrics. */
func runSingleFileValidation(input, successDir, errorDir string, schema validator.SchemaConfig, writeEmptyError bool) {
	parquetPath, errorCSVPath := validator.OutputPaths(input, successDir, errorDir)
	stats, err := validator.RunValidationAndWriteParquet(input, parquetPath, errorCSVPath, schema, writeEmptyError)
	if err != nil {
		exitf("processing failed: %v", err)
	}
	fmt.Printf("done: total=%d valid=%d invalid=%d written=%s errors=%s\n", stats.TotalRows, stats.ValidRows, stats.InvalidRows, parquetPath, errorCSVPath)
}

/* runDirectoryValidation processes all CSV files in a directory with a worker pool. */
func runDirectoryValidation(inputDir string, threads int, successDir, errorDir string, schema validator.SchemaConfig, writeEmptyError bool) {
	files, err := validator.ListCSVFiles(inputDir)
	if err != nil {
		exitf("failed listing csv files: %v", err)
	}
	if len(files) == 0 {
		exitf("no csv files found in directory: %s", inputDir)
	}

	fmt.Fprintf(os.Stderr, "starting directory run: files=%d workers=%d\n", len(files), threads)
	summary := validator.ProcessDirectory(files, threads, successDir, errorDir, schema, writeEmptyError)
	fmt.Printf("done: files=%d failed_files=%d total=%d valid=%d invalid=%d workers=%d\n", summary.Files, summary.FailedFiles, summary.TotalRows, summary.ValidRows, summary.InvalidRows, threads)
	if summary.FailedFiles > 0 {
		os.Exit(1)
	}
}

/* runSplitMode validates split flags and executes split processing. */
func runSplitMode(input, outDir, primaryKey, missingFile string, maxOpen int) {
	if strings.TrimSpace(primaryKey) == "" {
		exitf("missing required -split-primary-key <header_name>")
	}
	if maxOpen < 1 {
		maxOpen = 1
	}

	summary, err := splitcsv.SplitByPrimaryKey(splitcsv.Config{
		InputPath:       input,
		OutputDir:       outDir,
		PrimaryKey:      primaryKey,
		MaxOpenWriters:  maxOpen,
		MissingKeysFile: missingFile,
	})
	if err != nil {
		exitf("split failed: %v", err)
	}
	fmt.Printf(
		"split done: total=%d split_rows=%d missing_key_rows=%d files=%d out_dir=%s\n",
		summary.TotalRows, summary.SplitRows, summary.MissingKeyRows, summary.OutputFiles, outDir,
	)
}

/* parseWriteEmptyErrorArg parses optional positional bool for writing empty error CSV outputs. */
func parseWriteEmptyErrorArg(inputDir string, args []string) (bool, error) {
	if inputDir == "" {
		if len(args) < 2 {
			return false, nil
		}
		return strconv.ParseBool(strings.TrimSpace(args[1]))
	}
	if len(args) == 0 {
		return false, nil
	}
	return strconv.ParseBool(strings.TrimSpace(args[0]))
}

/* exitf writes an error message to stderr and exits the process. */
func exitf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
