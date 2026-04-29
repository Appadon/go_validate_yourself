package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"go_validate_yourself/internal/api"
	gvyconfig "go_validate_yourself/internal/config"
	"go_validate_yourself/internal/console"
	"go_validate_yourself/internal/service"
)

/* cliOptions holds parsed command-line flags. */
type cliOptions struct {
	mode                      string
	modeSpecified             bool
	configPath                string
	configSpecified           bool
	printConfig               bool
	phases                    string
	phasesSpecified           bool
	schemaPath                string
	schemaSpecified           bool
	inputDir                  string
	inputDirSpecified         bool
	threads                   int
	threadsSpecified          bool
	writeEmptyError           bool
	writeEmptyErrorSet        bool
	clearCache                bool
	clearCacheSet             bool
	successDir                string
	successDirSpecified       bool
	errorDir                  string
	errorDirSpecified         bool
	splitInput                string
	splitInputSpecified       bool
	splitOutputDir            string
	splitOutputDirSpecified   bool
	splitPrimaryKey           string
	splitPrimaryKeySpecified  bool
	splitMaxOpen              int
	splitMaxOpenSpecified     bool
	splitMissingFile          string
	splitMissingFileSpecified bool
	batchSize                 int
	batchSizeSpecified        bool
	batchDir                  string
	batchDirSpecified         bool
	batchExportDir            string
	batchExportDirSpecified   bool
	host                      string
	hostSpecified             bool
	port                      int
	portSpecified             bool
}

/* cliConfigResolution stores the config and metadata resolved from CLI input. */
type cliConfigResolution struct {
	Config         gvyconfig.ResolvedConfig
	ConfigPath     string
	PrintConfig    bool
	ThreadSource   string
	PositionMode   string
	ConfigProvided bool
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

	resolved, err := resolveCLIConfig(opts, args)
	if err != nil {
		console.Infof("%v", err)
		printUsageAndExit(2)
	}

	if resolved.PrintConfig {
		printResolvedConfigAndExit(resolved.Config)
		return
	}

	if err := dispatchResolvedConfig(resolved); err != nil {
		exitf("%v", err)
	}
}

/* parseFlags parses CLI flags for validation, batch, split, and server modes. */
func parseFlags() cliOptions {
	opts := cliOptions{}
	normalizedArgs := normalizeArgsForFlexibleFlags(os.Args[1:])
	flag.Usage = printUsage
	flag.StringVar(&opts.mode, "mode", "", "Execution mode: auto | validate | split | batch | server (default: inferred)")
	flag.StringVar(&opts.configPath, "config", "", "GVY run config JSON file")
	flag.BoolVar(&opts.printConfig, "print-config", false, "Print the resolved effective config and exit")
	flag.StringVar(&opts.phases, "phases", "", "Comma-separated pipeline phases: split,validate,batch")
	flag.StringVar(&opts.schemaPath, "schema", "", "Schema JSON file for validation phases")
	flag.StringVar(&opts.inputDir, "dir", "", "Directory containing CSV files to validate")
	flag.IntVar(&opts.threads, "t", service.DefaultThreadCount(), "Number of concurrent workers for validation and batch phases")
	flag.BoolVar(&opts.writeEmptyError, "write-empty-error", false, "Write empty error CSV files for fully valid inputs")
	flag.BoolVar(&opts.clearCache, "clear-validation-cache", false, "Clear success/error/batch directories before compatible full auto runs")
	flag.StringVar(&opts.successDir, "success-dir", "success", "Directory for valid parquet output")
	flag.StringVar(&opts.errorDir, "error-dir", "errors", "Directory for validation error CSV output")
	flag.StringVar(&opts.splitInput, "split-input", "", "Input CSV file to split by primary key")
	flag.StringVar(&opts.splitOutputDir, "split-output-dir", "split", "Output directory for split CSV files")
	flag.StringVar(&opts.splitPrimaryKey, "split-primary-key", "", "Header name to use as split key")
	flag.IntVar(&opts.splitMaxOpen, "split-max-open", 256, "Maximum number of concurrently open split file writers")
	flag.StringVar(&opts.splitMissingFile, "split-missing-file", "missing_keys.csv", "Name for rows where split key is blank")
	flag.IntVar(&opts.batchSize, "batch-size", 1000, "Number of parquet files per output batch")
	flag.StringVar(&opts.batchDir, "batch-dir", "", "Directory containing parquet files for batch mode, or batch input override")
	flag.StringVar(&opts.batchExportDir, "batch-export-dir", "batch_export", "Directory for batch mode output parquet files")
	flag.StringVar(&opts.host, "host", "127.0.0.1", "Host for server mode")
	flag.IntVar(&opts.port, "port", 8080, "Port for server mode")
	if err := flag.CommandLine.Parse(normalizedArgs); err != nil {
		exitWithCode(2)
	}
	opts.modeSpecified = isFlagProvided("mode")
	opts.configSpecified = isFlagProvided("config")
	opts.phasesSpecified = isFlagProvided("phases")
	opts.schemaSpecified = isFlagProvided("schema")
	opts.inputDirSpecified = isFlagProvided("dir")
	opts.threadsSpecified = isFlagProvided("t")
	opts.writeEmptyErrorSet = isFlagProvided("write-empty-error")
	opts.clearCacheSet = isFlagProvided("clear-validation-cache")
	opts.successDirSpecified = isFlagProvided("success-dir")
	opts.errorDirSpecified = isFlagProvided("error-dir")
	opts.splitInputSpecified = isFlagProvided("split-input")
	opts.splitOutputDirSpecified = isFlagProvided("split-output-dir")
	opts.splitPrimaryKeySpecified = isFlagProvided("split-primary-key")
	opts.splitMaxOpenSpecified = isFlagProvided("split-max-open")
	opts.splitMissingFileSpecified = isFlagProvided("split-missing-file")
	opts.batchSizeSpecified = isFlagProvided("batch-size")
	opts.batchDirSpecified = isFlagProvided("batch-dir")
	opts.batchExportDirSpecified = isFlagProvided("batch-export-dir")
	opts.hostSpecified = isFlagProvided("host")
	opts.portSpecified = isFlagProvided("port")
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
		"config":                 true,
		"print-config":           false,
		"phases":                 true,
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

/* resolveCLIConfig loads config defaults/files, applies CLI overlays, and normalizes the result. */
func resolveCLIConfig(opts cliOptions, args []string) (cliConfigResolution, error) {
	cfg := gvyconfig.Defaults()
	configProvided := strings.TrimSpace(opts.configPath) != ""
	if configProvided {
		loaded, err := gvyconfig.LoadFile(opts.configPath)
		if err != nil {
			return cliConfigResolution{}, err
		}
		cfg = loaded
	}

	positionMode, err := resolvePositionMode(opts, args, configProvided, cfg.Mode)
	if err != nil {
		return cliConfigResolution{}, err
	}
	if opts.modeSpecified {
		cfg.Mode = strings.ToLower(strings.TrimSpace(opts.mode))
		cfg.Pipeline.Phases = nil
	}
	if opts.phasesSpecified {
		phases, err := parseCLIPhases(opts.phases)
		if err != nil {
			return cliConfigResolution{}, err
		}
		cfg.Pipeline.Phases = phases
	}

	applyCLIFlagOverlay(&cfg, opts)
	if err := applyCLIPositionals(&cfg, opts, args, positionMode, configProvided); err != nil {
		return cliConfigResolution{}, err
	}
	applyLegacyClearDefaults(&cfg, opts, positionMode, configProvided)

	resolved, err := gvyconfig.Normalize(cfg, gvyconfig.NormalizeOptions{})
	if err != nil {
		return cliConfigResolution{}, err
	}

	return cliConfigResolution{
		Config:         resolved,
		ConfigPath:     strings.TrimSpace(opts.configPath),
		PrintConfig:    opts.printConfig,
		ThreadSource:   resolveThreadSource(opts, configProvided, cfg.Runtime.Workers),
		PositionMode:   positionMode,
		ConfigProvided: configProvided,
	}, nil
}

/* resolvePositionMode selects the legacy mode used to interpret positional arguments. */
func resolvePositionMode(opts cliOptions, args []string, configProvided bool, configMode string) (string, error) {
	if opts.phasesSpecified {
		phases, err := parseCLIPhases(opts.phases)
		if err != nil {
			return "", err
		}
		return positionModeForPhases(phases, args), nil
	}
	if opts.modeSpecified {
		return resolveMode(opts, args)
	}
	if configProvided {
		mode := strings.ToLower(strings.TrimSpace(configMode))
		if mode == "" {
			mode = strings.ToLower(strings.TrimSpace(gvyconfig.Defaults().Mode))
		}
		return mode, nil
	}
	return resolveMode(opts, args)
}

/* positionModeForPhases chooses a positional parsing mode for explicit phase input. */
func positionModeForPhases(phases []gvyconfig.Phase, args []string) string {
	if len(phases) == 1 {
		switch phases[0] {
		case gvyconfig.PhaseSplit:
			return modeSplit
		case gvyconfig.PhaseValidate:
			return modeValidate
		case gvyconfig.PhaseBatch:
			return modeBatch
		}
	}
	if looksLikeImplicitAuto(args) {
		return modeAuto
	}
	return modeValidate
}

/* parseCLIPhases converts a comma-separated -phases value into typed phases. */
func parseCLIPhases(raw string) ([]gvyconfig.Phase, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, fmt.Errorf("-phases requires at least one phase")
	}
	parts := strings.Split(trimmed, ",")
	phases := make([]gvyconfig.Phase, 0, len(parts))
	for _, part := range parts {
		phase := gvyconfig.Phase(strings.ToLower(strings.TrimSpace(part)))
		if phase == "" {
			return nil, fmt.Errorf("-phases contains an empty phase")
		}
		phases = append(phases, phase)
	}
	return phases, nil
}

/* applyCLIFlagOverlay applies explicitly provided CLI flags to the config. */
func applyCLIFlagOverlay(cfg *gvyconfig.Config, opts cliOptions) {
	if opts.schemaSpecified {
		cfg.Inputs.Schema = opts.schemaPath
	}
	if opts.inputDirSpecified {
		cfg.Inputs.ValidateDir = opts.inputDir
		cfg.Inputs.ValidateCSV = ""
	}
	if opts.threadsSpecified {
		cfg.Runtime.Workers = opts.threads
	}
	if opts.writeEmptyErrorSet {
		cfg.Validation.WriteEmptyError = opts.writeEmptyError
	}
	if opts.clearCacheSet {
		cfg.Validation.ClearOutputs = opts.clearCache
		cfg.Batch.ClearOutput = opts.clearCache
	}
	if opts.successDirSpecified {
		cfg.Outputs.SuccessDir = opts.successDir
	}
	if opts.errorDirSpecified {
		cfg.Outputs.ErrorDir = opts.errorDir
	}
	if opts.splitInputSpecified {
		cfg.Inputs.MainCSV = opts.splitInput
	}
	if opts.splitOutputDirSpecified {
		cfg.Outputs.SplitDir = opts.splitOutputDir
	}
	if opts.splitPrimaryKeySpecified {
		cfg.Split.PrimaryKey = opts.splitPrimaryKey
	}
	if opts.splitMaxOpenSpecified {
		cfg.Split.MaxOpenWriters = opts.splitMaxOpen
	}
	if opts.splitMissingFileSpecified {
		cfg.Split.MissingKeysFile = opts.splitMissingFile
	}
	if opts.batchSizeSpecified {
		cfg.Batch.Size = opts.batchSize
	}
	if opts.batchDirSpecified {
		cfg.Batch.InputDir = opts.batchDir
	}
	if opts.batchExportDirSpecified {
		cfg.Outputs.BatchExportDir = opts.batchExportDir
	}
	if opts.hostSpecified {
		cfg.Server.Host = opts.host
	}
	if opts.portSpecified {
		cfg.Server.Port = opts.port
	}
}

/* applyCLIPositionals maps legacy positional arguments into config inputs. */
func applyCLIPositionals(cfg *gvyconfig.Config, opts cliOptions, args []string, positionMode string, configProvided bool) error {
	switch positionMode {
	case modeAuto:
		return applyAutoPositionals(cfg, opts, args)
	case modeValidate:
		return applyValidatePositionals(cfg, opts, args, configProvided)
	case modeSplit:
		return applySplitPositionals(cfg, opts, args)
	case modeBatch:
		return applyBatchPositionals(cfg, opts, args)
	case modeServer:
		if len(args) > 0 {
			return fmt.Errorf("server mode does not accept positional arguments")
		}
		return nil
	default:
		return fmt.Errorf("unsupported mode %q", positionMode)
	}
}

/* applyAutoPositionals maps auto-mode positional arguments into main CSV and schema config fields. */
func applyAutoPositionals(cfg *gvyconfig.Config, opts cliOptions, args []string) error {
	remaining := append([]string{}, args...)
	if !opts.schemaSpecified {
		if idx := indexOfSchemaArg(remaining); idx >= 0 {
			cfg.Inputs.Schema = remaining[idx]
			remaining = removeArgAt(remaining, idx)
		}
	}
	if len(remaining) > 1 {
		return fmt.Errorf("auto mode accepts only <main.csv> plus flags")
	}
	if len(remaining) == 1 {
		cfg.Inputs.MainCSV = remaining[0]
	}
	return nil
}

/* applyValidatePositionals maps validation positional arguments into file or directory validation inputs. */
func applyValidatePositionals(cfg *gvyconfig.Config, opts cliOptions, args []string, configProvided bool) error {
	remaining := append([]string{}, args...)
	if !opts.schemaSpecified {
		if idx := indexOfSchemaArg(remaining); idx >= 0 {
			cfg.Inputs.Schema = remaining[idx]
			remaining = removeArgAt(remaining, idx)
		}
	}
	if strings.TrimSpace(cfg.Inputs.Schema) == "" && (!configProvided || len(remaining) > 0 || opts.inputDirSpecified) {
		defaulted, err := service.ResolveDefaultSchemaPath()
		if err != nil {
			return err
		}
		cfg.Inputs.Schema = defaulted
		console.Infof("no schema provided; defaulting to %s", console.GreenValue(cfg.Inputs.Schema))
	}
	if opts.inputDirSpecified {
		if len(remaining) > 0 {
			return fmt.Errorf("for -dir mode, use flags only (no positional arguments)")
		}
		return nil
	}
	if len(remaining) > 1 {
		return fmt.Errorf("single-file validation accepts only <input.csv> plus flags")
	}
	if len(remaining) == 1 {
		cfg.Inputs.ValidateCSV = remaining[0]
		if !opts.inputDirSpecified {
			cfg.Inputs.ValidateDir = ""
		}
	}
	return nil
}

/* applySplitPositionals maps split positional arguments into the main CSV config field. */
func applySplitPositionals(cfg *gvyconfig.Config, opts cliOptions, args []string) error {
	if opts.splitInputSpecified {
		if len(args) > 1 {
			return fmt.Errorf("split mode accepts one positional input CSV")
		}
		if len(args) == 1 && args[0] != opts.splitInput {
			return fmt.Errorf("conflicting split input values: %q and %q", opts.splitInput, args[0])
		}
		cfg.Inputs.MainCSV = opts.splitInput
		return nil
	}
	if len(args) > 1 {
		return fmt.Errorf("split mode accepts one positional input CSV")
	}
	if len(args) == 1 {
		cfg.Inputs.MainCSV = args[0]
	}
	return nil
}

/* applyBatchPositionals maps batch positional arguments into the batch input directory. */
func applyBatchPositionals(cfg *gvyconfig.Config, opts cliOptions, args []string) error {
	if opts.batchDirSpecified {
		if len(args) > 1 {
			return fmt.Errorf("batch mode accepts one positional directory")
		}
		if len(args) == 1 && args[0] != opts.batchDir {
			return fmt.Errorf("conflicting batch directory values: %q and %q", opts.batchDir, args[0])
		}
		cfg.Batch.InputDir = opts.batchDir
		return nil
	}
	if len(args) > 1 {
		return fmt.Errorf("batch mode accepts one positional directory")
	}
	if len(args) == 1 {
		cfg.Batch.InputDir = args[0]
	}
	return nil
}

/* applyLegacyClearDefaults preserves legacy no-config clearing defaults. */
func applyLegacyClearDefaults(cfg *gvyconfig.Config, opts cliOptions, positionMode string, configProvided bool) {
	if configProvided || opts.clearCacheSet {
		return
	}
	if positionMode == modeAuto && !opts.modeSpecified {
		cfg.Validation.ClearOutputs = true
	}
	if positionMode == modeBatch {
		cfg.Batch.ClearOutput = true
	}
}

/* resolveThreadSource returns a human-readable source label for worker count banners. */
func resolveThreadSource(opts cliOptions, configProvided bool, workers int) string {
	if opts.threadsSpecified {
		return "cli"
	}
	if configProvided && workers > 0 {
		return "config"
	}
	return "default(60% cpu)"
}

/* printResolvedConfigAndExit prints the effective config JSON to stdout. */
func printResolvedConfigAndExit(resolved gvyconfig.ResolvedConfig) {
	payload, err := json.MarshalIndent(resolved, "", "  ")
	if err != nil {
		exitf("failed encoding resolved config: %v", err)
	}
	fmt.Fprintln(os.Stdout, string(payload))
}

/* dispatchResolvedConfig executes the workflow represented by the resolved config. */
func dispatchResolvedConfig(resolution cliConfigResolution) error {
	resolved := resolution.Config
	if resolved.Mode == modeServer && len(resolved.Plan.Phases) == 0 {
		return runResolvedServer(resolved)
	}
	if !supportedResolvedPhases(resolved.Plan.Phases) {
		return fmt.Errorf("unsupported pipeline phase sequence %v", resolved.Plan.Phases)
	}

	primaryKey, err := printResolvedPipelineBanners(resolved, resolution.ThreadSource)
	if err != nil {
		return err
	}
	pipelineOpts := pipelineOptionsFromResolved(resolved, primaryKey)
	_, err = service.New().RunPipeline(context.Background(), pipelineOpts)
	return err
}

/* supportedResolvedPhases reports whether the CLI can dispatch a resolved phase sequence. */
func supportedResolvedPhases(phases []gvyconfig.Phase) bool {
	switch {
	case phasesEqual(phases, []gvyconfig.Phase{gvyconfig.PhaseSplit}),
		phasesEqual(phases, []gvyconfig.Phase{gvyconfig.PhaseValidate}),
		phasesEqual(phases, []gvyconfig.Phase{gvyconfig.PhaseBatch}),
		phasesEqual(phases, []gvyconfig.Phase{gvyconfig.PhaseSplit, gvyconfig.PhaseValidate}),
		phasesEqual(phases, []gvyconfig.Phase{gvyconfig.PhaseValidate, gvyconfig.PhaseBatch}),
		phasesEqual(phases, []gvyconfig.Phase{gvyconfig.PhaseSplit, gvyconfig.PhaseValidate, gvyconfig.PhaseBatch}):
		return true
	default:
		return false
	}
}

/* phasesEqual reports whether two phase slices contain the same ordered phases. */
func phasesEqual(actual, expected []gvyconfig.Phase) bool {
	if len(actual) != len(expected) {
		return false
	}
	for i := range actual {
		if actual[i] != expected[i] {
			return false
		}
	}
	return true
}

/*
printResolvedPipelineBanners prints the same CLI banners as the legacy
mode dispatchers before handing execution to the service orchestrator.
*/
func printResolvedPipelineBanners(resolved gvyconfig.ResolvedConfig, threadSource string) (string, error) {
	if phasesEqual(resolved.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseSplit, gvyconfig.PhaseValidate, gvyconfig.PhaseBatch}) {
		primaryKey, primaryKeySource, err := resolvePrimaryKeyForRun(resolved.Inputs.MainCSV, resolved.Split.PrimaryKey)
		if err != nil {
			return "", err
		}
		printAutoModeBanner(autoModeBannerConfig{
			MainInput:            resolved.Inputs.MainCSV,
			SchemaPath:           resolved.Inputs.Schema,
			WriteEmptyError:      resolved.Validation.WriteEmptyError,
			ClearValidationCache: resolved.Validation.ClearOutputs,
			SplitOutputDir:       resolved.Outputs.SplitDir,
			SuccessDir:           resolved.Outputs.SuccessDir,
			ErrorDir:             resolved.Outputs.ErrorDir,
			PrimaryKey:           primaryKey,
			PrimaryKeySource:     primaryKeySource,
			SplitMaxOpen:         resolved.Split.MaxOpenWriters,
			MissingKeysFile:      resolved.Split.MissingKeysFile,
			Threads:              resolved.EffectiveWorkers,
			ThreadSource:         threadSource,
			CPUCount:             runtime.NumCPU(),
			BatchDir:             resolved.Batch.InputDir,
			BatchExportDir:       resolved.Outputs.BatchExportDir,
			BatchSize:            resolved.Batch.Size,
			BatchThreads:         resolved.EffectiveWorkers,
			BatchThreadSource:    threadSource,
		})
		return primaryKey, nil
	}

	primaryKey := ""
	if containsResolvedPhase(resolved.Plan.Phases, gvyconfig.PhaseSplit) {
		detected, _, err := resolvePrimaryKeyForRun(resolved.Inputs.MainCSV, resolved.Split.PrimaryKey)
		if err != nil {
			return "", err
		}
		primaryKey = detected
		printSplitModeBanner(resolved.Inputs.MainCSV, resolved.Outputs.SplitDir, primaryKey, resolved.Split.MaxOpenWriters, resolved.Split.MissingKeysFile)
	}
	if containsResolvedPhase(resolved.Plan.Phases, gvyconfig.PhaseValidate) {
		if strings.TrimSpace(resolved.Inputs.ValidateCSV) != "" {
			printValidationBanner(validationBannerConfig{
				Mode:            "single-file validation",
				SchemaPath:      resolved.Inputs.Schema,
				Input:           resolved.Inputs.ValidateCSV,
				SuccessDir:      resolved.Outputs.SuccessDir,
				ErrorDir:        resolved.Outputs.ErrorDir,
				WriteEmptyError: resolved.Validation.WriteEmptyError,
				Threads:         1,
			})
		} else {
			printValidationBanner(validationBannerConfig{
				Mode:            "directory validation",
				SchemaPath:      resolved.Inputs.Schema,
				Input:           resolved.Inputs.ValidateDir,
				SuccessDir:      resolved.Outputs.SuccessDir,
				ErrorDir:        resolved.Outputs.ErrorDir,
				WriteEmptyError: resolved.Validation.WriteEmptyError,
				Threads:         resolved.EffectiveWorkers,
			})
		}
	}
	if containsResolvedPhase(resolved.Plan.Phases, gvyconfig.PhaseBatch) {
		printBatchModeBanner(resolved.Batch.InputDir, resolved.Outputs.BatchExportDir, resolved.Batch.Size, resolved.EffectiveWorkers, threadSource, resolved.Batch.ClearOutput)
	}
	return primaryKey, nil
}

/* pipelineOptionsFromResolved converts resolved config into service execution options. */
func pipelineOptionsFromResolved(resolved gvyconfig.ResolvedConfig, splitPrimaryKey string) service.PipelineOptions {
	fullAutoPipeline := phasesEqual(resolved.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseSplit, gvyconfig.PhaseValidate, gvyconfig.PhaseBatch})
	batchClearOutput := resolved.Batch.ClearOutput
	if fullAutoPipeline {
		batchClearOutput = false
	}
	return service.PipelineOptions{
		Phases:       servicePipelinePhases(resolved.Plan.Phases),
		ResumePolicy: serviceResumePolicy(resolved.Plan.ResumePolicy),
		Split: service.SplitOptions{
			InputPath:       resolved.Inputs.MainCSV,
			OutputDir:       resolved.Outputs.SplitDir,
			PrimaryKey:      splitPrimaryKey,
			MaxOpenWriters:  resolved.Split.MaxOpenWriters,
			MissingKeysFile: resolved.Split.MissingKeysFile,
		},
		Validate: service.ValidateOptions{
			SchemaPath:      resolved.Inputs.Schema,
			InputCSV:        resolved.Inputs.ValidateCSV,
			InputDir:        resolved.Inputs.ValidateDir,
			Threads:         resolved.EffectiveWorkers,
			WriteEmptyError: resolved.Validation.WriteEmptyError,
			SuccessDir:      resolved.Outputs.SuccessDir,
			ErrorDir:        resolved.Outputs.ErrorDir,
		},
		Batch: service.BatchOptions{
			InputDir:       resolved.Batch.InputDir,
			OutputDir:      resolved.Outputs.BatchExportDir,
			BatchSize:      resolved.Batch.Size,
			Workers:        resolved.EffectiveWorkers,
			ClearOutputDir: batchClearOutput,
		},
		ReuseSplitCache:           resolved.Split.ReuseCache,
		ClearValidationOutputDirs: resolved.Validation.ClearOutputs && fullAutoPipeline,
		Reporter:                  console.NewProgressReporter(),
		Mode:                      resolvedPipelineMode(resolved),
	}
}

func servicePipelinePhases(phases []gvyconfig.Phase) []service.PipelinePhase {
	out := make([]service.PipelinePhase, 0, len(phases))
	for _, phase := range phases {
		out = append(out, service.PipelinePhase(phase))
	}
	return out
}

func serviceResumePolicy(policy gvyconfig.ResumePolicy) service.PipelineResumePolicy {
	return service.PipelineResumePolicy(policy)
}

func resolvedPipelineMode(resolved gvyconfig.ResolvedConfig) string {
	if phasesEqual(resolved.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseSplit, gvyconfig.PhaseValidate, gvyconfig.PhaseBatch}) {
		return modeAuto
	}
	if phasesEqual(resolved.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseSplit}) {
		return modeSplit
	}
	if phasesEqual(resolved.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseValidate}) {
		if strings.TrimSpace(resolved.Inputs.ValidateCSV) != "" {
			return "validate-file"
		}
		return "validate-dir"
	}
	if phasesEqual(resolved.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseBatch}) {
		return modeBatch
	}
	return "pipeline"
}

func containsResolvedPhase(phases []gvyconfig.Phase, target gvyconfig.Phase) bool {
	for _, phase := range phases {
		if phase == target {
			return true
		}
	}
	return false
}

/* runResolvedServer starts server mode from resolved config values. */
func runResolvedServer(resolved gvyconfig.ResolvedConfig) error {
	if !isLoopbackHost(resolved.Server.Host) {
		return fmt.Errorf("server mode only supports loopback hosts; got %q", resolved.Server.Host)
	}
	console.Infof("starting server mode on %s:%d", console.GreenValue(resolved.Server.Host), resolved.Server.Port)
	server := api.NewServer(resolved.Server.Host, resolved.Server.Port, service.New())
	return server.ListenAndServe()
}

/* resolvePrimaryKeyForRun returns the configured or auto-detected split primary key. */
func resolvePrimaryKeyForRun(inputCSV, configured string) (string, string, error) {
	primaryKey := strings.TrimSpace(configured)
	if primaryKey != "" {
		return primaryKey, "cli/config", nil
	}
	detected, err := service.DetectPrimaryKey(inputCSV)
	if err != nil {
		return "", "", fmt.Errorf("failed detecting split primary key: %w", err)
	}
	return detected, "auto-detected(first header)", nil
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

/* printUsage writes complete CLI help, including config-first execution and API notes. */
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
	fmt.Fprintf(out, "  %s -config gvy.config.json [flags]\n", bin)
	fmt.Fprintf(out, "  %s -config gvy.config.json -print-config\n", bin)

	fmt.Fprintf(out, "\nModes:\n")
	fmt.Fprintf(out, "  config-driven pipeline:\n")
	fmt.Fprintf(out, "    Preferred contract for new automation. Loads a GVY run config JSON file and resolves it into explicit phases.\n")
	fmt.Fprintf(out, "    CLI flags override config file values.\n")
	fmt.Fprintf(out, "    Optional: -phases split,validate,batch to override the configured phase list.\n")
	fmt.Fprintf(out, "    Optional: -print-config to print the resolved effective config and exit.\n")
	fmt.Fprintf(out, "    Modes expand to phases unless pipeline.phases is set:\n")
	fmt.Fprintf(out, "      auto => split,validate,batch; split => split; validate => validate; batch => batch.\n")
	fmt.Fprintf(out, "    Server mode starts the runtime/API entry point; it is not a data phase.\n")
	fmt.Fprintf(out, "    Minimal auto config:\n")
	fmt.Fprintf(out, "      {\"mode\":\"auto\",\"inputs\":{\"main_csv\":\"main.csv\",\"schema\":\"schema.json\"}}\n")

	fmt.Fprintf(out, "  auto mode:\n")
	fmt.Fprintf(out, "    CLI compatibility shortcut for config mode=auto: split, validate, then batch.\n")
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
	fmt.Fprintf(out, "      - Split output is reused automatically when the input hash and split settings match.\n")

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
	fmt.Fprintf(out, "    Starts the localhost-only HTTP API and browser UI.\n")
	fmt.Fprintf(out, "    Optional: -host <addr> (default 127.0.0.1)\n")
	fmt.Fprintf(out, "    Optional: -port <n> (default 8080)\n")
	fmt.Fprintf(out, "    Config-first API endpoints:\n")
	fmt.Fprintf(out, "      GET  /api/config/defaults\n")
	fmt.Fprintf(out, "      POST /api/config/resolve\n")
	fmt.Fprintf(out, "      POST /api/runs/config\n")
	fmt.Fprintf(out, "    Compatibility endpoint:\n")
	fmt.Fprintf(out, "      POST /run/validate-auto\n")

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
	fmt.Fprintf(out, "  %s -config gvy.config.json -phases split\n", bin)
	fmt.Fprintf(out, "  %s -config gvy.config.json -phases validate,batch -dir split/\n", bin)
	fmt.Fprintf(out, "  %s -config gvy.config.json -t 12\n", bin)
	fmt.Fprintf(out, "  %s -config gvy.config.json -print-config\n", bin)
	fmt.Fprintf(out, "  curl -s http://127.0.0.1:8080/api/config/defaults\n")
	fmt.Fprintf(out, "  curl -s -X POST http://127.0.0.1:8080/api/config/resolve -H 'Content-Type: application/json' --data '{\"mode\":\"auto\",\"inputs\":{\"main_csv\":\"main.csv\",\"schema\":\"schema.json\"}}'\n")
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
