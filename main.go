package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"go_validate_yourself/internal/api"
	"go_validate_yourself/internal/console"
	"go_validate_yourself/internal/service"
)

/* cliOptions holds parsed command-line flags. */
type cliOptions struct {
	mode             string
	modeSpecified    bool
	schemaPath       string
	inputDir         string
	threads          int
	threadsSpecified bool
	writeEmptyError  bool
	clearCache       bool
	clearCacheSet    bool
	successDir       string
	errorDir         string
	splitInput       string
	splitOutputDir   string
	splitPrimaryKey  string
	splitMaxOpen     int
	splitMissingFile string
	batchSize        int
	batchDir         string
	batchExportDir   string
	host             string
	port             int
}

var runStartedAt = time.Now()

const (
	modeAuto     = "auto"
	modeValidate = "validate"
	modeSplit    = "split"
	modeBatch    = "batch"
	modeServer   = "server"

	defaultSchemaPath = "policy_schema.json"
)

/* main parses arguments and dispatches CLI or server execution modes. */
func main() {
	runStartedAt = time.Now()
	defer logTotalRuntime()

	opts := parseFlags()
	args := flag.Args()

	mode, err := resolveMode(opts, args)
	if err != nil {
		console.Infof("%v", err)
		printUsageAndExit(2)
	}

	switch mode {
	case modeAuto:
		runAutoMode(opts, args)
	case modeSplit:
		runSplitOnlyMode(opts, args)
	case modeValidate:
		runValidationMode(opts, args)
	case modeBatch:
		runBatchMode(opts, args)
	case modeServer:
		runServerMode(opts)
	default:
		exitf("unsupported mode %q", mode)
	}
}

/* parseFlags parses CLI flags for validation, batch, split, and server modes. */
func parseFlags() cliOptions {
	opts := cliOptions{}
	normalizedArgs := normalizeArgsForFlexibleFlags(os.Args[1:])
	flag.Usage = printUsage
	flag.StringVar(&opts.mode, "mode", "", "Execution mode: auto | validate | split | batch | server (default: inferred)")
	flag.StringVar(&opts.schemaPath, "schema", "", "Schema JSON file (required)")
	flag.StringVar(&opts.inputDir, "dir", "", "Directory containing CSV files to validate")
	flag.IntVar(&opts.threads, "t", service.DefaultThreadCount(), "Number of concurrent workers for -dir mode")
	flag.BoolVar(&opts.writeEmptyError, "write-empty-error", false, "Write empty error CSV files for fully valid inputs")
	flag.BoolVar(&opts.clearCache, "clear-validation-cache", false, "Clear split/success/error directories before auto mode run")
	flag.StringVar(&opts.successDir, "success-dir", "success", "Directory for valid parquet output")
	flag.StringVar(&opts.errorDir, "error-dir", "errors", "Directory for validation error CSV output")
	flag.StringVar(&opts.splitInput, "split-input", "", "Input CSV file to split by primary key")
	flag.StringVar(&opts.splitOutputDir, "split-output-dir", "split", "Output directory for split CSV files")
	flag.StringVar(&opts.splitPrimaryKey, "split-primary-key", "", "Header name to use as split key")
	flag.IntVar(&opts.splitMaxOpen, "split-max-open", 256, "Maximum number of concurrently open split file writers")
	flag.StringVar(&opts.splitMissingFile, "split-missing-file", "missing_keys.csv", "Name for rows where split key is blank")
	flag.IntVar(&opts.batchSize, "batch-size", 1000, "Number of parquet files per output batch")
	flag.StringVar(&opts.batchDir, "batch-dir", "", "Directory containing parquet files for batch mode (defaults to success-dir in auto mode)")
	flag.StringVar(&opts.batchExportDir, "batch-export-dir", "batch_export", "Directory for batch mode output parquet files")
	flag.StringVar(&opts.host, "host", "127.0.0.1", "Host for server mode")
	flag.IntVar(&opts.port, "port", 8080, "Port for server mode")
	if err := flag.CommandLine.Parse(normalizedArgs); err != nil {
		exitWithCode(2)
	}
	opts.modeSpecified = isFlagProvided("mode")
	opts.threadsSpecified = isFlagProvided("t")
	opts.clearCacheSet = isFlagProvided("clear-validation-cache")
	if !opts.threadsSpecified {
		opts.threads = service.DefaultThreadCount()
	}
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

/* normalizeArgsForFlexibleFlags allows flags before or after positional arguments. */
func normalizeArgsForFlexibleFlags(raw []string) []string {
	flags := make([]string, 0, len(raw))
	positionals := make([]string, 0, len(raw))
	takesValue := map[string]bool{
		"mode":                   true,
		"schema":                 true,
		"dir":                    true,
		"t":                      true,
		"write-empty-error":      false,
		"clear-validation-cache": false,
		"success-dir":            true,
		"error-dir":              true,
		"split-input":            true,
		"split-output-dir":       true,
		"split-primary-key":      true,
		"split-max-open":         true,
		"split-missing-file":     true,
		"batch-size":             true,
		"batch-dir":              true,
		"batch-export-dir":       true,
		"host":                   true,
		"port":                   true,
	}

	for i := 0; i < len(raw); i++ {
		token := raw[i]
		if token == "--" {
			positionals = append(positionals, raw[i+1:]...)
			break
		}
		if !strings.HasPrefix(token, "-") || token == "-" {
			positionals = append(positionals, token)
			continue
		}

		name, hasInlineValue := parseLongFlagName(token)
		if !takesValue[name] || hasInlineValue {
			flags = append(flags, token)
			continue
		}

		flags = append(flags, token)
		if i+1 < len(raw) {
			i++
			flags = append(flags, raw[i])
		}
	}

	return append(flags, positionals...)
}

/* parseLongFlagName extracts the flag name and reports whether it includes an inline value. */
func parseLongFlagName(token string) (string, bool) {
	clean := strings.TrimLeft(token, "-")
	if eq := strings.Index(clean, "="); eq >= 0 {
		return clean[:eq], true
	}
	return clean, false
}

/* resolveMode selects execution mode from explicit -mode or inferred defaults. */
func resolveMode(opts cliOptions, args []string) (string, error) {
	explicitMode := strings.ToLower(strings.TrimSpace(opts.mode))
	if explicitMode != "" {
		switch explicitMode {
		case modeAuto:
			if strings.TrimSpace(opts.inputDir) != "" {
				return "", fmt.Errorf("-dir is only valid in %q mode; use -mode validate -dir <input_dir>", modeValidate)
			}
			return modeAuto, nil
		case modeValidate:
			return modeValidate, nil
		case modeSplit:
			return modeSplit, nil
		case modeBatch:
			return modeBatch, nil
		case modeServer:
			return modeServer, nil
		default:
			return "", fmt.Errorf("invalid -mode %q (expected: auto | validate | split | batch | server)", opts.mode)
		}
	}

	if strings.TrimSpace(opts.inputDir) != "" && looksLikeImplicitAuto(args) {
		return "", fmt.Errorf("inferred auto mode from <main.csv> <schema.json>, but -dir requires validation mode; use -mode validate -dir <input_dir>")
	}
	if looksLikeImplicitAuto(args) {
		return modeAuto, nil
	}
	if strings.TrimSpace(opts.splitInput) != "" {
		return modeSplit, nil
	}
	if strings.TrimSpace(opts.batchDir) != "" {
		return modeBatch, nil
	}
	if strings.TrimSpace(opts.inputDir) != "" || strings.TrimSpace(opts.schemaPath) != "" || len(args) > 0 {
		return modeValidate, nil
	}
	return modeAuto, nil
}

/* looksLikeImplicitAuto reports whether positional args match inferred auto mode shape. */
func looksLikeImplicitAuto(args []string) bool {
	if len(args) < 2 {
		return false
	}
	return strings.ToLower(filepath.Ext(args[1])) == ".json"
}

/* runSplitOnlyMode resolves split inputs and executes split mode with auto key detection. */
func runSplitOnlyMode(opts cliOptions, args []string) {
	input, err := resolveSplitInput(opts, args)
	if err != nil {
		exitf("split mode argument error: %v", err)
	}

	primaryKey := strings.TrimSpace(opts.splitPrimaryKey)
	if primaryKey == "" {
		primaryKey, err = service.DetectPrimaryKey(input)
		if err != nil {
			exitf("failed detecting split primary key: %v", err)
		}
	}

	printSplitModeBanner(input, opts.splitOutputDir, primaryKey, opts.splitMaxOpen, opts.splitMissingFile)
	runSplitMode(input, opts.splitOutputDir, primaryKey, opts.splitMissingFile, opts.splitMaxOpen)
}

/* resolveSplitInput resolves split input from either -split-input or one positional argument. */
func resolveSplitInput(opts cliOptions, args []string) (string, error) {
	splitInput := strings.TrimSpace(opts.splitInput)
	if splitInput == "" {
		if len(args) == 0 {
			return "", fmt.Errorf("missing split input CSV; use -mode split <input.csv> or -split-input <input.csv>")
		}
		if len(args) > 1 {
			return "", fmt.Errorf("split mode accepts one positional input CSV")
		}
		return args[0], nil
	}
	if len(args) == 0 {
		return splitInput, nil
	}
	if len(args) > 1 {
		return "", fmt.Errorf("split mode accepts one positional input CSV")
	}
	if args[0] != splitInput {
		return "", fmt.Errorf("conflicting split input values: %q and %q", splitInput, args[0])
	}
	return splitInput, nil
}

/* runAutoMode executes the full split, validate, and batch workflow. */
func runAutoMode(opts cliOptions, args []string) {
	mainInput, schemaPath, err := resolveAutoModeInputs(opts, args)
	if err != nil {
		exitf("auto mode argument error: %v", err)
	}

	writeEmptyError := opts.writeEmptyError
	clearValidationCache := opts.clearCache
	if !opts.modeSpecified && !opts.clearCacheSet {
		clearValidationCache = true
	}

	primaryKey := strings.TrimSpace(opts.splitPrimaryKey)
	if primaryKey == "" {
		primaryKey, err = service.DetectPrimaryKey(mainInput)
		if err != nil {
			exitf("failed detecting split primary key: %v", err)
		}
	}

	threads := opts.threads
	threadSource := "cli"
	if !opts.threadsSpecified {
		threads = service.DefaultThreadCount()
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
		BatchDir:             resolveAutoBatchDir(opts),
		BatchExportDir:       opts.batchExportDir,
		BatchSize:            normalizeBatchSize(opts.batchSize),
		BatchThreads:         threads,
		BatchThreadSource:    threadSource,
	})

	_, err = service.New().RunAuto(service.AutoOptions{
		MainInputCSV:         mainInput,
		SchemaPath:           schemaPath,
		SplitOutputDir:       opts.splitOutputDir,
		SplitPrimaryKey:      primaryKey,
		SplitMaxOpen:         opts.splitMaxOpen,
		SplitMissingFile:     opts.splitMissingFile,
		Threads:              threads,
		WriteEmptyError:      writeEmptyError,
		ClearValidationCache: clearValidationCache,
		SuccessDir:           opts.successDir,
		ErrorDir:             opts.errorDir,
		BatchDir:             resolveAutoBatchDir(opts),
		BatchExportDir:       opts.batchExportDir,
		BatchSize:            normalizeBatchSize(opts.batchSize),
	})
	if err != nil {
		exitf("%v", err)
	}
}

/* resolveAutoModeInputs maps supported auto-mode CLI patterns to input and schema paths. */
func resolveAutoModeInputs(opts cliOptions, args []string) (string, string, error) {
	remaining := append([]string{}, args...)
	schemaPath := strings.TrimSpace(opts.schemaPath)
	if schemaPath == "" {
		idx := indexOfSchemaArg(remaining)
		if idx == -1 {
			return "", "", fmt.Errorf("missing schema path; pass <schema.json> or -schema <path>")
		}
		schemaPath = remaining[idx]
		remaining = removeArgAt(remaining, idx)
	}
	if len(remaining) < 1 {
		return "", "", fmt.Errorf("missing main input CSV")
	}
	if len(remaining) > 1 {
		return "", "", fmt.Errorf("auto mode accepts only <main.csv> plus flags")
	}
	return remaining[0], schemaPath, nil
}

/* runBatchMode resolves batch inputs and executes parquet batching mode. */
func runBatchMode(opts cliOptions, args []string) {
	batchDir, err := resolveBatchInput(opts, args)
	if err != nil {
		exitf("batch mode argument error: %v", err)
	}
	clearValidationCache := opts.clearCache
	if !opts.clearCacheSet {
		clearValidationCache = true
	}
	batchSize := normalizeBatchSize(opts.batchSize)
	threads := opts.threads
	threadSource := "cli"
	if !opts.threadsSpecified {
		threads = service.DefaultThreadCount()
		threadSource = "default(60% cpu)"
	}

	printBatchModeBanner(batchDir, opts.batchExportDir, batchSize, threads, threadSource, clearValidationCache)
	_, err = service.New().RunBatch(service.BatchOptions{
		InputDir:       batchDir,
		OutputDir:      opts.batchExportDir,
		BatchSize:      batchSize,
		Workers:        threads,
		ClearOutputDir: clearValidationCache,
	})
	if err != nil {
		exitf("%v", err)
	}
}

/* resolveBatchInput resolves batch input directory from -batch-dir or one positional arg. */
func resolveBatchInput(opts cliOptions, args []string) (string, error) {
	batchDir := strings.TrimSpace(opts.batchDir)
	if batchDir == "" {
		if len(args) == 0 {
			return "", fmt.Errorf("missing batch directory; use -mode batch -batch-dir <dir> or -mode batch <dir>")
		}
		if len(args) > 1 {
			return "", fmt.Errorf("batch mode accepts one positional directory")
		}
		return args[0], nil
	}
	if len(args) == 0 {
		return batchDir, nil
	}
	if len(args) > 1 {
		return "", fmt.Errorf("batch mode accepts one positional directory")
	}
	if args[0] != batchDir {
		return "", fmt.Errorf("conflicting batch directory values: %q and %q", batchDir, args[0])
	}
	return batchDir, nil
}

/* normalizeBatchSize applies lower bounds and defaults for parquet batching. */
func normalizeBatchSize(batchSize int) int {
	if batchSize < 1 {
		return 1
	}
	return batchSize
}

/* resolveAutoBatchDir returns the parquet input directory used during auto mode batch phase. */
func resolveAutoBatchDir(opts cliOptions) string {
	if strings.TrimSpace(opts.batchDir) == "" {
		return opts.successDir
	}
	return opts.batchDir
}

/* runValidationMode validates CLI arguments and executes single-file or directory processing. */
func runValidationMode(opts cliOptions, args []string) {
	normalizeValidationOptions(&opts)
	inputCSV, schemaPath, err := resolveValidationInputs(opts, args)
	if err != nil {
		exitf("validation mode argument error: %v", err)
	}

	printValidationBanner(validationBannerConfig{
		Mode:            validationModeLabel(opts),
		SchemaPath:      schemaPath,
		Input:           validationInputLabel(opts, inputCSV),
		SuccessDir:      opts.successDir,
		ErrorDir:        opts.errorDir,
		WriteEmptyError: opts.writeEmptyError,
		Threads:         validationThreadCount(opts),
	})

	if strings.TrimSpace(opts.inputDir) == "" {
		runSingleFileValidation(inputCSV, schemaPath, opts.successDir, opts.errorDir, opts.writeEmptyError)
		return
	}
	runDirectoryValidation(opts.inputDir, schemaPath, opts.threads, opts.successDir, opts.errorDir, opts.writeEmptyError)
}

/* normalizeValidationOptions applies bounds and defaults for validation mode. */
func normalizeValidationOptions(opts *cliOptions) {
	if opts.threads < 1 {
		opts.threads = 1
	}
}

/* resolveValidationInputs resolves schema and input targets for validation mode. */
func resolveValidationInputs(opts cliOptions, args []string) (string, string, error) {
	remaining := append([]string{}, args...)
	schemaPath := strings.TrimSpace(opts.schemaPath)
	if schemaPath == "" {
		idx := indexOfSchemaArg(remaining)
		if idx >= 0 {
			schemaPath = remaining[idx]
			remaining = removeArgAt(remaining, idx)
		}
	}
	if schemaPath == "" {
		defaulted, err := service.ResolveDefaultSchemaPath()
		if err != nil {
			return "", "", err
		}
		schemaPath = defaulted
		console.Infof("no schema provided; defaulting to %s", console.GreenValue(schemaPath))
	}

	if strings.TrimSpace(opts.inputDir) != "" {
		if len(remaining) > 0 {
			return "", "", fmt.Errorf("for -dir mode, use flags only (no positional arguments)")
		}
		return "", schemaPath, nil
	}
	if len(remaining) < 1 {
		return "", "", fmt.Errorf("missing input CSV; use -mode validate <input.csv> [-schema <schema.json>]")
	}
	if len(remaining) > 1 {
		return "", "", fmt.Errorf("single-file validation accepts only <input.csv> plus flags")
	}
	return remaining[0], schemaPath, nil
}

/* validationModeLabel returns the label used in the validation banner. */
func validationModeLabel(opts cliOptions) string {
	if strings.TrimSpace(opts.inputDir) == "" {
		return "single-file validation"
	}
	return "directory validation"
}

/* validationInputLabel returns the input label used in the validation banner. */
func validationInputLabel(opts cliOptions, inputCSV string) string {
	if strings.TrimSpace(opts.inputDir) == "" {
		return inputCSV
	}
	return opts.inputDir
}

/* validationThreadCount returns the effective thread count for the validation banner. */
func validationThreadCount(opts cliOptions) int {
	if strings.TrimSpace(opts.inputDir) == "" {
		return 1
	}
	return opts.threads
}

/* indexOfSchemaArg returns the first positional index that looks like a JSON schema path. */
func indexOfSchemaArg(args []string) int {
	for i, arg := range args {
		if strings.ToLower(filepath.Ext(strings.TrimSpace(arg))) == ".json" {
			return i
		}
	}
	return -1
}

/* removeArgAt returns a new slice without the element at idx. */
func removeArgAt(args []string, idx int) []string {
	if idx < 0 || idx >= len(args) {
		return args
	}
	return append(args[:idx], args[idx+1:]...)
}

/* printUsageAndExit writes CLI usage and exits. */
func printUsageAndExit(code int) {
	printUsage()
	exitWithCode(code)
}

/* printUsage writes complete CLI help, including the new server mode. */
func printUsage() {
	out := flag.CommandLine.Output()
	bin := filepath.Base(os.Args[0])
	fmt.Fprintf(out, "Usage:\n")
	fmt.Fprintf(out, "  %s <main.csv> <schema.json> [flags]\n", bin)
	fmt.Fprintf(out, "  %s -mode auto <main.csv> <schema.json> [flags]\n", bin)
	fmt.Fprintf(out, "  %s -mode validate <input.csv> [-schema <schema.json>] [flags]\n", bin)
	fmt.Fprintf(out, "  %s -mode validate -dir <input_dir> [-schema <schema.json>] [flags]\n", bin)
	fmt.Fprintf(out, "  %s -mode split <input.csv>\n", bin)
	fmt.Fprintf(out, "  %s -mode split -split-input <input.csv>\n", bin)
	fmt.Fprintf(out, "  %s -mode batch -batch-dir <input_dir> [-batch-size <n>] [flags]\n", bin)
	fmt.Fprintf(out, "  %s -mode server [-host 127.0.0.1] [-port 8080]\n", bin)

	fmt.Fprintf(out, "\nModes:\n")
	fmt.Fprintf(out, "  auto mode:\n")
	fmt.Fprintf(out, "    Splits a main CSV by primary key, validates split files, then batches success parquet outputs.\n")
	fmt.Fprintf(out, "    Required positional args:\n")
	fmt.Fprintf(out, "      <main.csv> <schema.json>\n")
	fmt.Fprintf(out, "    Optional flags:\n")
	fmt.Fprintf(out, "      -t=<n> (workers for validate + batch phases; default ~60%% cpu)\n")
	fmt.Fprintf(out, "      -write-empty-error=true\n")
	fmt.Fprintf(out, "      -clear-validation-cache=true\n")
	fmt.Fprintf(out, "      -batch-size=<n> (default 1000)\n")
	fmt.Fprintf(out, "      -batch-dir=<path> (default: value of -success-dir)\n")
	fmt.Fprintf(out, "      -batch-export-dir=<path> (default batch_export)\n")
	fmt.Fprintf(out, "    Notes:\n")
	fmt.Fprintf(out, "      - If -split-primary-key is omitted, the first CSV header is used.\n")

	fmt.Fprintf(out, "  single-file validation mode:\n")
	fmt.Fprintf(out, "    Validates one CSV using a schema.\n")
	fmt.Fprintf(out, "    Required: <input.csv>\n")
	fmt.Fprintf(out, "    Optional: -schema <schema.json> (defaults to %s when present)\n", defaultSchemaPath)
	fmt.Fprintf(out, "    Optional flags:\n")
	fmt.Fprintf(out, "      -write-empty-error=true\n")

	fmt.Fprintf(out, "  directory validation mode:\n")
	fmt.Fprintf(out, "    Validates every CSV file in a directory using a schema.\n")
	fmt.Fprintf(out, "    Required: -dir <input_dir>\n")
	fmt.Fprintf(out, "    Optional: -schema <schema.json> (defaults to %s when present)\n", defaultSchemaPath)
	fmt.Fprintf(out, "    Optional flags:\n")
	fmt.Fprintf(out, "      -write-empty-error=true\n")

	fmt.Fprintf(out, "  split-only mode:\n")
	fmt.Fprintf(out, "    Splits one CSV into many files by primary key.\n")
	fmt.Fprintf(out, "    Required: <input.csv> (or -split-input <input.csv>)\n")
	fmt.Fprintf(out, "    Optional: -split-primary-key <header_name> (defaults to first CSV header)\n")

	fmt.Fprintf(out, "  batch mode:\n")
	fmt.Fprintf(out, "    Groups parquet files into batched parquet outputs.\n")
	fmt.Fprintf(out, "    Required: -batch-dir <input_dir> (or <input_dir> positional)\n")
	fmt.Fprintf(out, "    Optional: -t <n> (batch workers, default ~60%% cpu)\n")
	fmt.Fprintf(out, "    Optional: -batch-size <n> (default 1000)\n")
	fmt.Fprintf(out, "    Optional: -batch-export-dir <path> (default batch_export)\n")
	fmt.Fprintf(out, "    Optional: -clear-validation-cache=true|false (default true in batch mode)\n")

	fmt.Fprintf(out, "  server mode:\n")
	fmt.Fprintf(out, "    Starts the localhost-only HTTP API.\n")
	fmt.Fprintf(out, "    Optional: -host <addr> (default 127.0.0.1)\n")
	fmt.Fprintf(out, "    Optional: -port <n> (default 8080)\n")

	fmt.Fprintf(out, "\nHelp:\n")
	fmt.Fprintf(out, "  -h, -help\n")
	fmt.Fprintf(out, "    Show this help message.\n")

	fmt.Fprintf(out, "\nFlags:\n")
	flag.PrintDefaults()

	fmt.Fprintf(out, "\nExamples:\n")
	fmt.Fprintf(out, "  %s main.csv schema.json\n", bin)
	fmt.Fprintf(out, "  %s main.csv schema.json -t 10 -write-empty-error=true\n", bin)
	fmt.Fprintf(out, "  %s -mode validate -dir split/\n", bin)
	fmt.Fprintf(out, "  %s -mode validate input.csv -schema schema.json -write-empty-error=true\n", bin)
	fmt.Fprintf(out, "  %s -mode split main.csv\n", bin)
	fmt.Fprintf(out, "  %s -mode split -split-input main.csv -split-primary-key policy_number\n", bin)
	fmt.Fprintf(out, "  %s -mode batch -batch-size 1000 -batch-dir success/ -batch-export-dir batch_export\n", bin)
	fmt.Fprintf(out, "  %s -mode server -host 127.0.0.1 -port 8080\n", bin)
}

/* runSingleFileValidation delegates single-file validation to the shared service layer. */
func runSingleFileValidation(input, schemaPath, successDir, errorDir string, writeEmptyError bool) {
	_, err := service.New().RunValidateFile(service.ValidateOptions{
		SchemaPath:      schemaPath,
		InputCSV:        input,
		WriteEmptyError: writeEmptyError,
		SuccessDir:      successDir,
		ErrorDir:        errorDir,
	})
	if err != nil {
		exitf("%v", err)
	}
}

/* runDirectoryValidation delegates directory validation to the shared service layer. */
func runDirectoryValidation(inputDir, schemaPath string, threads int, successDir, errorDir string, writeEmptyError bool) {
	_, err := service.New().RunValidateDir(service.ValidateOptions{
		SchemaPath:      schemaPath,
		InputDir:        inputDir,
		Threads:         threads,
		WriteEmptyError: writeEmptyError,
		SuccessDir:      successDir,
		ErrorDir:        errorDir,
	})
	if err != nil {
		exitf("%v", err)
	}
}

/* runSplitMode delegates split execution to the shared service layer. */
func runSplitMode(input, outDir, primaryKey, missingFile string, maxOpen int) {
	_, err := service.New().RunSplit(service.SplitOptions{
		InputPath:       input,
		OutputDir:       outDir,
		PrimaryKey:      primaryKey,
		MaxOpenWriters:  maxOpen,
		MissingKeysFile: missingFile,
	})
	if err != nil {
		exitf("%v", err)
	}
}

/* runBatchParquetMode delegates batch execution to the shared service layer. */
func runBatchParquetMode(batchDir, batchExportDir string, batchSize, workers int) {
	_, err := service.New().RunBatch(service.BatchOptions{
		InputDir:  batchDir,
		OutputDir: batchExportDir,
		BatchSize: batchSize,
		Workers:   workers,
	})
	if err != nil {
		exitf("%v", err)
	}
}

/* runServerMode starts the localhost-only HTTP server. */
func runServerMode(opts cliOptions) {
	if strings.TrimSpace(opts.host) == "" {
		opts.host = "127.0.0.1"
	}
	if opts.port < 1 {
		exitf("invalid port %d", opts.port)
	}
	if !isLoopbackHost(opts.host) {
		exitf("server mode only supports loopback hosts; got %q", opts.host)
	}

	console.Infof("starting server mode on %s:%d", console.GreenValue(opts.host), opts.port)
	server := api.NewServer(opts.host, opts.port, service.New())
	if err := server.ListenAndServe(); err != nil {
		exitf("server failed: %v", err)
	}
}

/* isLoopbackHost reports whether the provided bind host is loopback-safe. */
func isLoopbackHost(host string) bool {
	trimmed := strings.TrimSpace(host)
	switch trimmed {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
}

/* exitf writes an error message to stderr and exits the process. */
func exitf(format string, args ...interface{}) {
	console.Errorf(format, args...)
	exitWithCode(1)
}

/* exitWithCode logs runtime and exits with the provided status code. */
func exitWithCode(code int) {
	logTotalRuntime()
	os.Exit(code)
}

/* logTotalRuntime prints total process runtime using console formatting. */
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
	BatchDir             string
	BatchExportDir       string
	BatchSize            int
	BatchThreads         int
	BatchThreadSource    string
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

/* printAutoModeBanner prints a full auto-mode configuration banner before processing starts. */
func printAutoModeBanner(cfg autoModeBannerConfig) {
	items := []console.BannerItem{
		{Key: "mode", Value: "auto (split + directory validate + batch)"},
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
		{Key: "batch_dir", Value: cfg.BatchDir},
		{Key: "batch_export_dir", Value: cfg.BatchExportDir},
		{Key: "batch_size", Value: strconv.Itoa(cfg.BatchSize)},
		{Key: "batch_threads", Value: fmt.Sprintf("%d (%s)", cfg.BatchThreads, cfg.BatchThreadSource)},
	}
	console.PrintBanner("Validation Run Configuration", items)
}

/* printValidationBanner prints a configuration banner for single-file or directory validation mode. */
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

/* printSplitModeBanner prints a split-only configuration banner before split processing starts. */
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

/* printBatchModeBanner prints a batch-only configuration banner before batch processing starts. */
func printBatchModeBanner(batchDir, batchExportDir string, batchSize, batchThreads int, threadSource string, clearValidationCache bool) {
	items := []console.BannerItem{
		{Key: "mode", Value: "batch-only"},
		{Key: "batch_dir", Value: batchDir},
		{Key: "batch_export_dir", Value: batchExportDir},
		{Key: "batch_size", Value: strconv.Itoa(batchSize)},
		{Key: "batch_threads", Value: fmt.Sprintf("%d (%s)", batchThreads, threadSource)},
		{Key: "clear_validation_cache", Value: strconv.FormatBool(clearValidationCache)},
	}
	console.PrintBanner("Validation Run Configuration", items)
}
