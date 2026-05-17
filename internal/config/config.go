package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"go_validate_yourself/internal/service"
)

const (
	defaultMode             = "auto"
	defaultSplitDir         = "split"
	defaultSuccessDir       = "success"
	defaultErrorDir         = "errors"
	defaultBatchExportDir   = "batch_export"
	defaultSplitMaxOpen     = 256
	defaultSplitMissingFile = "missing_keys.parquet"
	defaultBatchSize        = 1000
	defaultServerHost       = "127.0.0.1"
	defaultServerPort       = 1818
	defaultWorkspaceDir     = ".gvy/runs"
)

/*
Phase identifies one executable data-processing step in a resolved
pipeline plan.
*/
type Phase string

const (
	PhaseSplit    Phase = "split"
	PhaseValidate Phase = "validate"
	PhaseBatch    Phase = "batch"
)

/*
ResumePolicy describes how a future pipeline runner should treat
already-produced outputs when a plan is executed.
*/
type ResumePolicy string

const (
	ResumePolicyRunAll              ResumePolicy = "run_all"
	ResumePolicyReuseValidOutputs   ResumePolicy = "reuse_valid_outputs"
	ResumePolicyStartAtFirstMissing ResumePolicy = "start_at_first_missing"
)

/*
Config is the canonical user-facing GVY run configuration.

It is intentionally separate from the existing CSV validation schema
JSON, which continues to describe field-level validation rules.
*/
type Config struct {
	Mode       string           `json:"mode"`
	Pipeline   PipelineConfig   `json:"pipeline"`
	Inputs     InputsConfig     `json:"inputs"`
	Outputs    OutputsConfig    `json:"outputs"`
	Split      SplitConfig      `json:"split"`
	Validation ValidationConfig `json:"validation"`
	Batch      BatchConfig      `json:"batch"`
	Runtime    RuntimeConfig    `json:"runtime"`
	Server     ServerConfig     `json:"server"`
}

/*
PipelineConfig contains the phase selection and resume behavior for a
run. When Phases is non-empty it overrides the compatibility Mode preset.
*/
type PipelineConfig struct {
	Phases       []Phase      `json:"phases"`
	ResumePolicy ResumePolicy `json:"resume_policy"`
}

/*
InputsConfig contains user-selected input files and directories consumed
by the pipeline phases.
*/
type InputsConfig struct {
	MainCSV     string `json:"main_csv"`
	Schema      string `json:"schema"`
	ValidateCSV string `json:"validate_csv"`
	ValidateDir string `json:"validate_dir"`
}

/*
OutputsConfig contains output locations produced by split and validation
phases, plus the default batch export location.
*/
type OutputsConfig struct {
	SplitDir       string `json:"split_dir"`
	SuccessDir     string `json:"success_dir"`
	ErrorDir       string `json:"error_dir"`
	BatchExportDir string `json:"batch_export_dir"`
}

/*
SplitConfig contains split-phase settings that map to the existing split
service options.
*/
type SplitConfig struct {
	PrimaryKey      string `json:"primary_key"`
	MaxOpenWriters  int    `json:"max_open_writers"`
	MissingKeysFile string `json:"missing_keys_file"`
	ReuseCache      bool   `json:"reuse_cache"`
}

/*
ValidationConfig contains validation-phase output and error-file
behavior.
*/
type ValidationConfig struct {
	WriteEmptyError bool `json:"write_empty_error"`
	ClearOutputs    bool `json:"clear_outputs"`
}

/*
BatchConfig contains batch-phase input, output, and batching behavior.
*/
type BatchConfig struct {
	InputDir    string `json:"input_dir"`
	Size        int    `json:"size"`
	ClearOutput bool   `json:"clear_output"`
}

/*
RuntimeConfig contains process-level execution settings shared by
pipeline phases.
*/
type RuntimeConfig struct {
	Workers int `json:"workers"`
}

/*
ServerConfig contains localhost API server settings used by server mode.
Server mode is a runtime entry point, not a data pipeline phase.
*/
type ServerConfig struct {
	Host         string `json:"host"`
	Port         int    `json:"port"`
	WorkspaceDir string `json:"workspace_dir"`
}

/*
ResolvedConfig is the effective configuration after defaults, phase
presets, derived inputs, and compatibility validation have been applied.
*/
type ResolvedConfig struct {
	Mode             string           `json:"mode"`
	Plan             PipelinePlan     `json:"plan"`
	Inputs           InputsConfig     `json:"inputs"`
	Outputs          OutputsConfig    `json:"outputs"`
	Split            SplitConfig      `json:"split"`
	Validation       ValidationConfig `json:"validation"`
	Batch            BatchConfig      `json:"batch"`
	Runtime          RuntimeConfig    `json:"runtime"`
	Server           ServerConfig     `json:"server"`
	EffectiveWorkers int              `json:"effective_workers"`
}

/*
PipelinePlan records the ordered data phases and the resolved input and
output bindings each phase will use.
*/
type PipelinePlan struct {
	Phases               []Phase      `json:"phases"`
	ResumePolicy         ResumePolicy `json:"resume_policy"`
	SplitInputCSV        string       `json:"split_input_csv,omitempty"`
	SplitOutputDir       string       `json:"split_output_dir,omitempty"`
	ValidateInputCSV     string       `json:"validate_input_csv,omitempty"`
	ValidateInputDir     string       `json:"validate_input_dir,omitempty"`
	ValidateSchema       string       `json:"validate_schema,omitempty"`
	ValidationSuccessDir string       `json:"validation_success_dir,omitempty"`
	ValidationErrorDir   string       `json:"validation_error_dir,omitempty"`
	BatchInputDir        string       `json:"batch_input_dir,omitempty"`
	BatchOutputDir       string       `json:"batch_output_dir,omitempty"`
}

/*
NormalizeOptions controls optional checks performed while building a
ResolvedConfig from a Config value.
*/
type NormalizeOptions struct {
	RequireExistingInputs bool
}

/*
Defaults returns GVY's built-in configuration defaults in one place.

These values mirror the current CLI/API/service defaults, without
changing those entry points in this stage.
*/
func Defaults() Config {
	return Config{
		Mode: defaultMode,
		Pipeline: PipelineConfig{
			ResumePolicy: ResumePolicyReuseValidOutputs,
		},
		Outputs: OutputsConfig{
			SplitDir:       defaultSplitDir,
			SuccessDir:     defaultSuccessDir,
			ErrorDir:       defaultErrorDir,
			BatchExportDir: defaultBatchExportDir,
		},
		Split: SplitConfig{
			MaxOpenWriters:  defaultSplitMaxOpen,
			MissingKeysFile: defaultSplitMissingFile,
			ReuseCache:      true,
		},
		Batch: BatchConfig{
			Size: defaultBatchSize,
		},
		Server: ServerConfig{
			Host:         defaultServerHost,
			Port:         defaultServerPort,
			WorkspaceDir: defaultWorkspaceDir,
		},
	}
}

/*
LoadFile reads a JSON GVY config file with strict unknown-field
rejection.

The decode starts from Defaults so omitted fields inherit built-in
values while explicitly provided zero values remain visible to Normalize.
*/
func LoadFile(path string) (Config, error) {
	clean := strings.TrimSpace(path)
	if clean == "" {
		return Config{}, fmt.Errorf("config path is required")
	}

	file, err := os.Open(clean)
	if err != nil {
		return Config{}, fmt.Errorf("open config %q: %w", clean, err)
	}
	defer file.Close()

	cfg := Defaults()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config %q: %w", clean, err)
	}
	if err := decoder.Decode(new(struct{})); !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("decode config %q: config file must contain a single JSON object", clean)
	}
	return cfg, nil
}

/*
Normalize applies defaults, expands mode presets into phases, resolves
derived phase inputs, and validates phase/input compatibility.
*/
func Normalize(cfg Config, opts NormalizeOptions) (ResolvedConfig, error) {
	cfg = applyDefaults(cfg)

	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	phases, err := resolvePhases(mode, cfg.Pipeline.Phases)
	if err != nil {
		return ResolvedConfig{}, err
	}
	cfg.Pipeline.Phases = phases

	if err := validateResumePolicy(cfg.Pipeline.ResumePolicy); err != nil {
		return ResolvedConfig{}, err
	}
	if err := validateScalarSettings(cfg); err != nil {
		return ResolvedConfig{}, err
	}

	if err := resolveDerivedInputs(&cfg, phases); err != nil {
		return ResolvedConfig{}, err
	}
	if err := validatePhaseInputs(cfg, phases); err != nil {
		return ResolvedConfig{}, err
	}
	if opts.RequireExistingInputs {
		if err := validateExistingInputs(cfg, phases); err != nil {
			return ResolvedConfig{}, err
		}
	}

	workers := cfg.Runtime.Workers
	if workers == 0 {
		workers = service.DefaultThreadCount()
	}
	if workers < 1 {
		return ResolvedConfig{}, fmt.Errorf("runtime.workers must be >= 0")
	}

	plan := buildPipelinePlan(cfg, phases)
	return ResolvedConfig{
		Mode:             mode,
		Plan:             plan,
		Inputs:           cfg.Inputs,
		Outputs:          cfg.Outputs,
		Split:            cfg.Split,
		Validation:       cfg.Validation,
		Batch:            cfg.Batch,
		Runtime:          cfg.Runtime,
		Server:           cfg.Server,
		EffectiveWorkers: workers,
	}, nil
}

/*
applyDefaults fills omitted string and numeric fields with built-in
defaults while leaving boolean fields unchanged.
*/
func applyDefaults(cfg Config) Config {
	defaults := Defaults()
	if strings.TrimSpace(cfg.Mode) == "" {
		cfg.Mode = defaults.Mode
	}
	if strings.TrimSpace(string(cfg.Pipeline.ResumePolicy)) == "" {
		cfg.Pipeline.ResumePolicy = defaults.Pipeline.ResumePolicy
	}
	if strings.TrimSpace(cfg.Outputs.SplitDir) == "" {
		cfg.Outputs.SplitDir = defaults.Outputs.SplitDir
	}
	if strings.TrimSpace(cfg.Outputs.SuccessDir) == "" {
		cfg.Outputs.SuccessDir = defaults.Outputs.SuccessDir
	}
	if strings.TrimSpace(cfg.Outputs.ErrorDir) == "" {
		cfg.Outputs.ErrorDir = defaults.Outputs.ErrorDir
	}
	if strings.TrimSpace(cfg.Outputs.BatchExportDir) == "" {
		cfg.Outputs.BatchExportDir = defaults.Outputs.BatchExportDir
	}
	if cfg.Split.MaxOpenWriters == 0 {
		cfg.Split.MaxOpenWriters = defaults.Split.MaxOpenWriters
	}
	if strings.TrimSpace(cfg.Split.MissingKeysFile) == "" {
		cfg.Split.MissingKeysFile = defaults.Split.MissingKeysFile
	}
	if cfg.Batch.Size == 0 {
		cfg.Batch.Size = defaults.Batch.Size
	}
	if strings.TrimSpace(cfg.Server.Host) == "" {
		cfg.Server.Host = defaults.Server.Host
	}
	if cfg.Server.Port == 0 {
		cfg.Server.Port = defaults.Server.Port
	}
	if strings.TrimSpace(cfg.Server.WorkspaceDir) == "" {
		cfg.Server.WorkspaceDir = defaults.Server.WorkspaceDir
	}
	return trimConfig(cfg)
}

/*
resolvePhases returns explicit pipeline phases when present, otherwise
it expands the compatibility mode preset into data phases.
*/
func resolvePhases(mode string, explicit []Phase) ([]Phase, error) {
	if explicit != nil {
		phases := make([]Phase, len(explicit))
		copy(phases, explicit)
		if err := validatePhases(phases); err != nil {
			return nil, err
		}
		return phases, nil
	}

	switch mode {
	case "auto":
		return []Phase{PhaseSplit, PhaseValidate, PhaseBatch}, nil
	case "split":
		return []Phase{PhaseSplit}, nil
	case "validate":
		return []Phase{PhaseValidate}, nil
	case "batch":
		return []Phase{PhaseBatch}, nil
	case "server":
		return nil, nil
	default:
		return nil, fmt.Errorf("mode must be one of auto, split, validate, batch, or server")
	}
}

/*
validatePhases rejects unknown or duplicate phases before derived inputs
are calculated.
*/
func validatePhases(phases []Phase) error {
	seen := make(map[Phase]bool, len(phases))
	for i, phase := range phases {
		phase = Phase(strings.ToLower(strings.TrimSpace(string(phase))))
		switch phase {
		case PhaseSplit, PhaseValidate, PhaseBatch:
		default:
			return fmt.Errorf("pipeline.phases[%d] must be one of split, validate, or batch", i)
		}
		if seen[phase] {
			return fmt.Errorf("pipeline.phases contains duplicate phase %q", phase)
		}
		seen[phase] = true
		phases[i] = phase
	}
	return nil
}

/*
validateResumePolicy rejects unknown resume-policy values before a plan
is returned.
*/
func validateResumePolicy(policy ResumePolicy) error {
	switch policy {
	case ResumePolicyRunAll, ResumePolicyReuseValidOutputs, ResumePolicyStartAtFirstMissing:
		return nil
	default:
		return fmt.Errorf("pipeline.resume_policy must be one of run_all, reuse_valid_outputs, or start_at_first_missing")
	}
}

/*
validateScalarSettings checks numeric settings that must be positive
after defaults have been applied.
*/
func validateScalarSettings(cfg Config) error {
	if cfg.Split.MaxOpenWriters < 1 {
		return fmt.Errorf("split.max_open_writers must be >= 1")
	}
	if cfg.Batch.Size < 1 {
		return fmt.Errorf("batch.size must be >= 1")
	}
	if cfg.Runtime.Workers < 0 {
		return fmt.Errorf("runtime.workers must be >= 0")
	}
	if cfg.Server.Port < 1 {
		return fmt.Errorf("server.port must be >= 1")
	}
	return nil
}

/*
resolveDerivedInputs applies pipeline-derived input defaults between
adjacent phases.
*/
func resolveDerivedInputs(cfg *Config, phases []Phase) error {
	if containsPhase(phases, PhaseValidate) {
		if strings.TrimSpace(cfg.Inputs.ValidateCSV) != "" && strings.TrimSpace(cfg.Inputs.ValidateDir) != "" {
			return fmt.Errorf("inputs.validate_csv and inputs.validate_dir cannot both be set")
		}
		if priorPhase(phases, PhaseSplit, PhaseValidate) && strings.TrimSpace(cfg.Inputs.ValidateCSV) == "" && strings.TrimSpace(cfg.Inputs.ValidateDir) == "" {
			cfg.Inputs.ValidateDir = cfg.Outputs.SplitDir
		}
	}
	if containsPhase(phases, PhaseBatch) {
		if priorPhase(phases, PhaseValidate, PhaseBatch) && strings.TrimSpace(cfg.Batch.InputDir) == "" {
			cfg.Batch.InputDir = cfg.Outputs.SuccessDir
		}
	}
	return nil
}

/*
validatePhaseInputs enforces the required input contract for each
selected phase.
*/
func validatePhaseInputs(cfg Config, phases []Phase) error {
	for _, phase := range phases {
		switch phase {
		case PhaseSplit:
			if strings.TrimSpace(cfg.Inputs.MainCSV) == "" {
				return fmt.Errorf("split phase requires inputs.main_csv")
			}
		case PhaseValidate:
			if strings.TrimSpace(cfg.Inputs.Schema) == "" {
				return fmt.Errorf("validate phase requires inputs.schema")
			}
			if strings.TrimSpace(cfg.Inputs.ValidateCSV) == "" && strings.TrimSpace(cfg.Inputs.ValidateDir) == "" {
				return fmt.Errorf("validate phase requires inputs.validate_dir, inputs.validate_csv, or a prior split phase")
			}
		case PhaseBatch:
			if strings.TrimSpace(cfg.Batch.InputDir) == "" {
				return fmt.Errorf("batch phase requires batch.input_dir or a prior validate phase")
			}
		}
	}
	return nil
}

/*
validateExistingInputs optionally checks that resolved input paths exist
when the caller wants filesystem validation.
*/
func validateExistingInputs(cfg Config, phases []Phase) error {
	if containsPhase(phases, PhaseSplit) {
		if err := requireFile(cfg.Inputs.MainCSV, "inputs.main_csv"); err != nil {
			return err
		}
	}
	if containsPhase(phases, PhaseValidate) {
		if err := requireFile(cfg.Inputs.Schema, "inputs.schema"); err != nil {
			return err
		}
		if strings.TrimSpace(cfg.Inputs.ValidateCSV) != "" {
			if err := requireFile(cfg.Inputs.ValidateCSV, "inputs.validate_csv"); err != nil {
				return err
			}
		}
		if strings.TrimSpace(cfg.Inputs.ValidateDir) != "" {
			if err := requireDir(cfg.Inputs.ValidateDir, "inputs.validate_dir"); err != nil {
				return err
			}
		}
	}
	if containsPhase(phases, PhaseBatch) {
		if err := requireDir(cfg.Batch.InputDir, "batch.input_dir"); err != nil {
			return err
		}
	}
	return nil
}

/*
buildPipelinePlan copies resolved phase bindings into the immutable plan
returned to callers.
*/
func buildPipelinePlan(cfg Config, phases []Phase) PipelinePlan {
	return PipelinePlan{
		Phases:               append([]Phase(nil), phases...),
		ResumePolicy:         cfg.Pipeline.ResumePolicy,
		SplitInputCSV:        cfg.Inputs.MainCSV,
		SplitOutputDir:       cfg.Outputs.SplitDir,
		ValidateInputCSV:     cfg.Inputs.ValidateCSV,
		ValidateInputDir:     cfg.Inputs.ValidateDir,
		ValidateSchema:       cfg.Inputs.Schema,
		ValidationSuccessDir: cfg.Outputs.SuccessDir,
		ValidationErrorDir:   cfg.Outputs.ErrorDir,
		BatchInputDir:        cfg.Batch.InputDir,
		BatchOutputDir:       cfg.Outputs.BatchExportDir,
	}
}

/*
trimConfig normalizes surrounding whitespace in string config fields
without changing user-selected path spelling otherwise.
*/
func trimConfig(cfg Config) Config {
	cfg.Mode = strings.ToLower(strings.TrimSpace(cfg.Mode))
	cfg.Inputs.MainCSV = strings.TrimSpace(cfg.Inputs.MainCSV)
	cfg.Inputs.Schema = strings.TrimSpace(cfg.Inputs.Schema)
	cfg.Inputs.ValidateCSV = strings.TrimSpace(cfg.Inputs.ValidateCSV)
	cfg.Inputs.ValidateDir = strings.TrimSpace(cfg.Inputs.ValidateDir)
	cfg.Outputs.SplitDir = strings.TrimSpace(cfg.Outputs.SplitDir)
	cfg.Outputs.SuccessDir = strings.TrimSpace(cfg.Outputs.SuccessDir)
	cfg.Outputs.ErrorDir = strings.TrimSpace(cfg.Outputs.ErrorDir)
	cfg.Outputs.BatchExportDir = strings.TrimSpace(cfg.Outputs.BatchExportDir)
	cfg.Split.PrimaryKey = strings.TrimSpace(cfg.Split.PrimaryKey)
	cfg.Split.MissingKeysFile = strings.TrimSpace(cfg.Split.MissingKeysFile)
	cfg.Batch.InputDir = strings.TrimSpace(cfg.Batch.InputDir)
	cfg.Server.Host = strings.TrimSpace(cfg.Server.Host)
	cfg.Server.WorkspaceDir = strings.TrimSpace(cfg.Server.WorkspaceDir)
	for i, phase := range cfg.Pipeline.Phases {
		cfg.Pipeline.Phases[i] = Phase(strings.ToLower(strings.TrimSpace(string(phase))))
	}
	cfg.Pipeline.ResumePolicy = ResumePolicy(strings.ToLower(strings.TrimSpace(string(cfg.Pipeline.ResumePolicy))))
	return cfg
}

func containsPhase(phases []Phase, target Phase) bool {
	for _, phase := range phases {
		if phase == target {
			return true
		}
	}
	return false
}

func priorPhase(phases []Phase, prior, target Phase) bool {
	priorIndex := -1
	targetIndex := -1
	for i, phase := range phases {
		if phase == prior {
			priorIndex = i
		}
		if phase == target {
			targetIndex = i
		}
	}
	return priorIndex >= 0 && targetIndex >= 0 && priorIndex < targetIndex
}

func requireFile(path, field string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%s does not exist: %w", field, err)
	}
	if info.IsDir() {
		return fmt.Errorf("%s must be a file", field)
	}
	return nil
}

func requireDir(path, field string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%s does not exist: %w", field, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s must be a directory", field)
	}
	return nil
}
