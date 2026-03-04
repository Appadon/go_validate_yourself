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

func main() {
	schemaPath := flag.String("schema", "", "Schema JSON file (required)")
	inputDir := flag.String("dir", "", "Directory containing CSV files to validate")
	threads := flag.Int("t", 1, "Number of concurrent workers for -dir mode")
	successDir := flag.String("success-dir", "success", "Directory for valid parquet output")
	errorDir := flag.String("error-dir", "errors", "Directory for validation error CSV output")
	splitInput := flag.String("split-input", "", "Input CSV file to split by primary key")
	splitOutputDir := flag.String("split-output-dir", "split", "Output directory for split CSV files")
	splitPrimaryKey := flag.String("split-primary-key", "", "Header name to use as split key")
	splitMaxOpen := flag.Int("split-max-open", 256, "Maximum number of concurrently open split file writers")
	splitMissingFile := flag.String("split-missing-file", "missing_keys.csv", "Name for rows where split key is blank")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage:\n  %s [flags] <input.csv> [write_empty_error]\n  %s [flags] -dir <input_dir> [write_empty_error]\n", os.Args[0], os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "\nOptional positional [write_empty_error]: true|false (default false)\n")
	}
	flag.Parse()

	if strings.TrimSpace(*splitInput) != "" {
		runSplitMode(*splitInput, *splitOutputDir, *splitPrimaryKey, *splitMissingFile, *splitMaxOpen)
		return
	}

	if *threads < 1 {
		*threads = 1
	}
	if *inputDir != "" && flag.NArg() > 1 {
		exitf("for -dir mode, optional positional is only [write_empty_error]")
	}
	if *inputDir == "" && (flag.NArg() < 1 || flag.NArg() > 2) {
		flag.Usage()
		os.Exit(2)
	}
	if strings.TrimSpace(*schemaPath) == "" {
		exitf("missing required -schema <path>")
	}

	writeEmptyError, err := parseWriteEmptyErrorArg(*inputDir, flag.Args())
	if err != nil {
		exitf("invalid write_empty_error: %v", err)
	}

	if err := os.MkdirAll(*successDir, 0o755); err != nil {
		exitf("failed creating success dir: %v", err)
	}
	if err := os.MkdirAll(*errorDir, 0o755); err != nil {
		exitf("failed creating errors dir: %v", err)
	}

	schema, err := validator.LoadSchema(*schemaPath)
	if err != nil {
		exitf("failed loading schema: %v", err)
	}

	if err := validator.ValidateSchema(&schema); err != nil {
		exitf("invalid schema: %v", err)
	}

	if *inputDir == "" {
		input := flag.Arg(0)
		parquetPath, errorCSVPath := validator.OutputPaths(input, *successDir, *errorDir)
		stats, err := validator.RunValidationAndWriteParquet(input, parquetPath, errorCSVPath, schema, writeEmptyError)
		if err != nil {
			exitf("processing failed: %v", err)
		}
		fmt.Printf("done: total=%d valid=%d invalid=%d written=%s errors=%s\n", stats.TotalRows, stats.ValidRows, stats.InvalidRows, parquetPath, errorCSVPath)
		return
	}

	files, err := validator.ListCSVFiles(*inputDir)
	if err != nil {
		exitf("failed listing csv files: %v", err)
	}
	if len(files) == 0 {
		exitf("no csv files found in directory: %s", *inputDir)
	}

	fmt.Fprintf(os.Stderr, "starting directory run: files=%d workers=%d\n", len(files), *threads)
	summary := validator.ProcessDirectory(files, *threads, *successDir, *errorDir, schema, writeEmptyError)
	fmt.Printf("done: files=%d failed_files=%d total=%d valid=%d invalid=%d workers=%d\n", summary.Files, summary.FailedFiles, summary.TotalRows, summary.ValidRows, summary.InvalidRows, *threads)
	if summary.FailedFiles > 0 {
		os.Exit(1)
	}
}

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

func exitf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
