package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"go_validate_yourself/internal/console"
	"go_validate_yourself/internal/splitcsv"
	"go_validate_yourself/internal/validator"
)

/* cliOptions holds parsed command-line flags. */
type cliOptions struct {
	schemaPath       string
	inputDir         string
	threads          int
	threadsSpecified bool
	successDir       string
	errorDir         string
	splitInput       string
	splitOutputDir   string
	splitPrimaryKey  string
	splitMaxOpen     int
	splitMissingFile string
}

var runStartedAt = time.Now()

/* main parses arguments and dispatches either split or validation modes. */
func main() {
	runStartedAt = time.Now()
	defer logTotalRuntime()

	opts := parseFlags()
	args := flag.Args()

	if shouldRunAutoMode(opts, args) {
		runAutoMode(opts, args)
		return
	}

	if strings.TrimSpace(opts.splitInput) != "" {
		printSplitModeBanner(opts.splitInput, opts.splitOutputDir, opts.splitPrimaryKey, opts.splitMaxOpen, opts.splitMissingFile)
		runSplitMode(opts.splitInput, opts.splitOutputDir, opts.splitPrimaryKey, opts.splitMissingFile, opts.splitMaxOpen)
		return
	}
	runValidationMode(opts, args)
}

/* parseFlags parses CLI flags for both validation and split modes. */
func parseFlags() cliOptions {
	opts := cliOptions{}
	flag.Usage = printUsage
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
	opts.threadsSpecified = isFlagProvided("t")
	return opts
}

/* isFlagProvided reports whether a CLI flag was explicitly set by the user. */
func isFlagProvided(name string) bool {
	provided := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			provided = true
		}
	})
	return provided
}

/* shouldRunAutoMode reports whether invocation matches positional auto mode. */
func shouldRunAutoMode(opts cliOptions, args []string) bool {
	if strings.TrimSpace(opts.splitInput) != "" || strings.TrimSpace(opts.inputDir) != "" {
		return false
	}
	if strings.TrimSpace(opts.schemaPath) != "" {
		return len(args) >= 1
	}
	return len(args) >= 2
}

/* runAutoMode splits the main CSV and validates the split directory in one command. */
func runAutoMode(opts cliOptions, args []string) {
	mainInput, schemaPath, autoArgs, err := resolveAutoModeInputs(opts, args)
	if err != nil {
		exitf("auto mode argument error: %v", err)
	}

	writeEmptyError, clearValidationCache, err := parseAutoModeOptionalArgs(autoArgs)
	if err != nil {
		exitf("invalid auto mode optional args: %v", err)
	}

	primaryKey := strings.TrimSpace(opts.splitPrimaryKey)
	if primaryKey == "" {
		primaryKey, err = detectPrimaryKey(mainInput)
		if err != nil {
			exitf("failed detecting split primary key: %v", err)
		}
	}

	threads := opts.threads
	threadSource := "cli"
	if !opts.threadsSpecified {
		threads = defaultThreadCount()
		threadSource = "default(60% cpu)"
	}
	primaryKeySource := "cli"
	if strings.TrimSpace(opts.splitPrimaryKey) == "" {
		primaryKeySource = "auto-detected(first header)"
	}

	printAutoModeBanner(autoModeBannerConfig{
		MainInput:            mainInput,
		SchemaPath:           schemaPath,
		WriteEmptyError:      writeEmptyError,
		ClearValidationCache: clearValidationCache,
		SplitOutputDir:       opts.splitOutputDir,
		SuccessDir:           opts.successDir,
		ErrorDir:             opts.errorDir,
		PrimaryKey:           primaryKey,
		PrimaryKeySource:     primaryKeySource,
		SplitMaxOpen:         opts.splitMaxOpen,
		MissingKeysFile:      opts.splitMissingFile,
		Threads:              threads,
		ThreadSource:         threadSource,
		CPUCount:             runtime.NumCPU(),
	})

	if clearValidationCache {
		console.Infof("clearing validation cache directories: %s, %s, %s", opts.splitOutputDir, opts.successDir, opts.errorDir)
		console.Infof("this might take a while depending on the size of the cache")
		clearValidationOutputDirs(opts.splitOutputDir, opts.successDir, opts.errorDir)
	}
	runSplitMode(mainInput, opts.splitOutputDir, primaryKey, opts.splitMissingFile, opts.splitMaxOpen)

	createOutputDirs(opts.successDir, opts.errorDir)
	schema := loadAndValidateSchema(schemaPath)
	runDirectoryValidation(opts.splitOutputDir, threads, opts.successDir, opts.errorDir, schema, writeEmptyError)
}

/* resolveAutoModeInputs maps supported auto-mode CLI patterns to input and schema paths. */
func resolveAutoModeInputs(opts cliOptions, args []string) (string, string, []string, error) {
	if strings.TrimSpace(opts.schemaPath) != "" {
		if len(args) < 1 {
			return "", "", nil, fmt.Errorf("missing main input CSV")
		}
		return args[0], opts.schemaPath, args[1:], nil
	}
	if len(args) < 2 {
		return "", "", nil, fmt.Errorf("missing required <main.csv> <schema.json>")
	}
	return args[0], args[1], args[2:], nil
}

/* parseAutoModeOptionalArgs parses [write_empty_error] [clear_validation_cache] for auto mode. */
func parseAutoModeOptionalArgs(args []string) (bool, bool, error) {
	writeEmptyError := false
	clearValidationCache := true

	if len(args) >= 1 {
		parsed, err := strconv.ParseBool(strings.TrimSpace(args[0]))
		if err != nil {
			return false, true, fmt.Errorf("write_empty_error: %w", err)
		}
		writeEmptyError = parsed
	}
	if len(args) >= 2 {
		parsed, err := strconv.ParseBool(strings.TrimSpace(args[1]))
		if err != nil {
			return false, true, fmt.Errorf("clear_validation_cache: %w", err)
		}
		clearValidationCache = parsed
	}
	if len(args) > 2 {
		return false, true, fmt.Errorf("too many optional positional arguments")
	}

	return writeEmptyError, clearValidationCache, nil
}

/* clearValidationOutputDirs removes stale split/success/error artifacts before auto-mode run. */
func clearValidationOutputDirs(splitOutputDir, successDir, errorDir string) {
	dirs := []string{splitOutputDir, successDir, errorDir}
	for _, dir := range dirs {
		if err := os.RemoveAll(dir); err != nil {
			exitf("failed clearing output dir %q: %v", dir, err)
		}
	}
}

/* detectPrimaryKey reads the first CSV header and returns it as split key. */
func detectPrimaryKey(inputPath string) (string, error) {
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

/* defaultThreadCount returns 60 percent of available CPUs, with a minimum of one. */
func defaultThreadCount() int {
	cpus := runtime.NumCPU()
	threads := int(float64(cpus) * 0.6)
	if threads < 1 {
		return 1
	}
	return threads
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
		printValidationBanner(validationBannerConfig{
			Mode:            "single-file validation",
			SchemaPath:      opts.schemaPath,
			Input:           args[0],
			SuccessDir:      opts.successDir,
			ErrorDir:        opts.errorDir,
			WriteEmptyError: writeEmptyError,
			Threads:         1,
		})
		runSingleFileValidation(args[0], opts.successDir, opts.errorDir, schema, writeEmptyError)
		return
	}
	printValidationBanner(validationBannerConfig{
		Mode:            "directory validation",
		SchemaPath:      opts.schemaPath,
		Input:           opts.inputDir,
		SuccessDir:      opts.successDir,
		ErrorDir:        opts.errorDir,
		WriteEmptyError: writeEmptyError,
		Threads:         opts.threads,
	})
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
	printUsage()
	exitWithCode(code)
}

/* printUsage writes complete CLI help, including mode-specific positionals and examples. */
func printUsage() {
	out := flag.CommandLine.Output()
	bin := filepath.Base(os.Args[0])
	fmt.Fprintf(out, "Usage:\n")
	fmt.Fprintf(out, "  %s [flags] <main.csv> <schema.json> [write_empty_error] [clear_validation_cache]\n", bin)
	fmt.Fprintf(out, "  %s [flags] -schema <schema.json> <main.csv> [write_empty_error] [clear_validation_cache]\n", bin)
	fmt.Fprintf(out, "  %s [flags] <input.csv> [write_empty_error]\n", bin)
	fmt.Fprintf(out, "  %s [flags] -dir <input_dir> [write_empty_error]\n", bin)
	fmt.Fprintf(out, "  %s [flags] -split-input <input.csv> -split-primary-key <header_name>\n", bin)

	fmt.Fprintf(out, "\nModes:\n")
	fmt.Fprintf(out, "  auto mode:\n")
	fmt.Fprintf(out, "    Splits a main CSV by primary key, then validates the generated split directory.\n")
	fmt.Fprintf(out, "    Required positional args:\n")
	fmt.Fprintf(out, "      <main.csv> <schema.json>  (or provide -schema and only <main.csv>)\n")
	fmt.Fprintf(out, "    Optional positional args:\n")
	fmt.Fprintf(out, "      [write_empty_error]      true|false (default: false)\n")
	fmt.Fprintf(out, "      [clear_validation_cache] true|false (default: true)\n")

	fmt.Fprintf(out, "  single-file validation mode:\n")
	fmt.Fprintf(out, "    Validates one CSV using a schema.\n")
	fmt.Fprintf(out, "    Required: -schema <schema.json> <input.csv>\n")
	fmt.Fprintf(out, "    Optional positional args:\n")
	fmt.Fprintf(out, "      [write_empty_error]      true|false (default: false)\n")

	fmt.Fprintf(out, "  directory validation mode:\n")
	fmt.Fprintf(out, "    Validates every CSV file in a directory using a schema.\n")
	fmt.Fprintf(out, "    Required: -schema <schema.json> -dir <input_dir>\n")
	fmt.Fprintf(out, "    Optional positional args:\n")
	fmt.Fprintf(out, "      [write_empty_error]      true|false (default: false)\n")

	fmt.Fprintf(out, "  split-only mode:\n")
	fmt.Fprintf(out, "    Splits one CSV into many files by primary key.\n")
	fmt.Fprintf(out, "    Required flags: -split-input <input.csv> -split-primary-key <header_name>\n")

	fmt.Fprintf(out, "\nHelp:\n")
	fmt.Fprintf(out, "  -h, -help\n")
	fmt.Fprintf(out, "    Show this help message.\n")

	fmt.Fprintf(out, "\nFlags:\n")
	flag.PrintDefaults()

	fmt.Fprintf(out, "\nExamples:\n")
	fmt.Fprintf(out, "  %s main.csv schema.json\n", bin)
	fmt.Fprintf(out, "  %s -schema schema.json main.csv true false\n", bin)
	fmt.Fprintf(out, "  %s -schema schema.json input.csv true\n", bin)
	fmt.Fprintf(out, "  %s -schema schema.json -dir split -t 8 true\n", bin)
	fmt.Fprintf(out, "  %s -split-input main.csv -split-primary-key policy_number -split-output-dir split\n", bin)
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
	console.Successf("single-file complete total=%d valid=%d invalid=%d written=%s errors=%s", stats.TotalRows, stats.ValidRows, stats.InvalidRows, parquetPath, errorCSVPath)
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

	console.Infof(
		"starting directory validation [files %s] [workers %s]",
		console.GreenValue(strconv.Itoa(len(files))),
		console.GreenValue(strconv.Itoa(threads)),
	)
	summary := validator.ProcessDirectory(files, threads, successDir, errorDir, schema, writeEmptyError)
	console.Successf("directory complete files=%d failed_files=%d total=%d valid=%d invalid=%d workers=%d", summary.Files, summary.FailedFiles, summary.TotalRows, summary.ValidRows, summary.InvalidRows, threads)
	if summary.FailedFiles > 0 {
		exitf("directory validation completed with %d failed file(s)", summary.FailedFiles)
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
	console.Infof(
		"starting split phase [input %s] [output_dir %s] [primary_key %s]",
		console.GreenValue(input),
		console.GreenValue(outDir),
		console.GreenValue(fmt.Sprintf("%q", primaryKey)),
	)

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
	console.Successf(
		"split complete [total %s] [missing_key_rows %s] [files %s] [out_dir %s]",
		console.GreenValue(strconv.Itoa(summary.TotalRows)),
		console.GreenValue(strconv.Itoa(summary.MissingKeyRows)),
		console.GreenValue(strconv.Itoa(summary.OutputFiles)),
		console.GreenValue(outDir),
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
	console.Errorf(format, args...)
	exitWithCode(1)
}

func exitWithCode(code int) {
	logTotalRuntime()
	os.Exit(code)
}

func logTotalRuntime() {
	elapsed := time.Since(runStartedAt)
	console.Infof("total run time %s", console.GreenValue(console.FormatDuration(elapsed)))
}

type autoModeBannerConfig struct {
	MainInput            string
	SchemaPath           string
	WriteEmptyError      bool
	ClearValidationCache bool
	SplitOutputDir       string
	SuccessDir           string
	ErrorDir             string
	PrimaryKey           string
	PrimaryKeySource     string
	SplitMaxOpen         int
	MissingKeysFile      string
	Threads              int
	ThreadSource         string
	CPUCount             int
}

type validationBannerConfig struct {
	Mode            string
	SchemaPath      string
	Input           string
	SuccessDir      string
	ErrorDir        string
	WriteEmptyError bool
	Threads         int
}

func printAutoModeBanner(cfg autoModeBannerConfig) {
	items := []console.BannerItem{
		{Key: "mode", Value: "auto (split + directory validate)"},
		{Key: "input_csv", Value: cfg.MainInput},
		{Key: "schema", Value: cfg.SchemaPath},
		{Key: "write_empty_error", Value: strconv.FormatBool(cfg.WriteEmptyError)},
		{Key: "clear_validation_cache", Value: strconv.FormatBool(cfg.ClearValidationCache)},
		{Key: "split_output_dir", Value: cfg.SplitOutputDir},
		{Key: "success_dir", Value: cfg.SuccessDir},
		{Key: "error_dir", Value: cfg.ErrorDir},
		{Key: "primary_key", Value: fmt.Sprintf("%q (%s)", cfg.PrimaryKey, cfg.PrimaryKeySource)},
		{Key: "split_max_open", Value: strconv.Itoa(cfg.SplitMaxOpen)},
		{Key: "missing_keys_file", Value: cfg.MissingKeysFile},
		{Key: "threads", Value: fmt.Sprintf("%d (%s)", cfg.Threads, cfg.ThreadSource)},
		{Key: "cpu_count", Value: strconv.Itoa(cfg.CPUCount)},
	}
	console.PrintBanner("Validation Run Configuration", items)
}

func printValidationBanner(cfg validationBannerConfig) {
	items := []console.BannerItem{
		{Key: "mode", Value: cfg.Mode},
		{Key: "input", Value: cfg.Input},
		{Key: "schema", Value: cfg.SchemaPath},
		{Key: "write_empty_error", Value: strconv.FormatBool(cfg.WriteEmptyError)},
		{Key: "success_dir", Value: cfg.SuccessDir},
		{Key: "error_dir", Value: cfg.ErrorDir},
		{Key: "threads", Value: strconv.Itoa(cfg.Threads)},
	}
	console.PrintBanner("Validation Run Configuration", items)
}

func printSplitModeBanner(input, splitOutputDir, primaryKey string, splitMaxOpen int, splitMissingFile string) {
	items := []console.BannerItem{
		{Key: "mode", Value: "split-only"},
		{Key: "input_csv", Value: input},
		{Key: "split_output_dir", Value: splitOutputDir},
		{Key: "primary_key", Value: fmt.Sprintf("%q", primaryKey)},
		{Key: "split_max_open", Value: strconv.Itoa(splitMaxOpen)},
		{Key: "missing_keys_file", Value: splitMissingFile},
	}
	console.PrintBanner("Validation Run Configuration", items)
}
