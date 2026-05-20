package main

import (
	"os"
	"path/filepath"
	"testing"

	gvyconfig "go_validate_yourself/internal/config"
	"go_validate_yourself/internal/service"
)

/* TestResolveCLIConfigLegacyAutoPreservesImplicitClear verifies legacy positional auto mapping. */
func TestResolveCLIConfigLegacyAutoPreservesImplicitClear(t *testing.T) {
	resolution, err := resolveCLIConfig(cliOptions{}, []string{"input.csv", "schema.json"})
	if err != nil {
		t.Fatalf("resolveCLIConfig() error = %v", err)
	}
	assertMainPhases(t, resolution.Config.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseSplit, gvyconfig.PhaseValidate, gvyconfig.PhaseBatch})
	if resolution.Config.Inputs.MainCSV != "input.csv" {
		t.Fatalf("MainCSV = %q, want input.csv", resolution.Config.Inputs.MainCSV)
	}
	if resolution.Config.Inputs.Schema != "schema.json" {
		t.Fatalf("Schema = %q, want schema.json", resolution.Config.Inputs.Schema)
	}
	if !resolution.Config.Validation.ClearOutputs {
		t.Fatal("Validation.ClearOutputs = false, want true for implicit legacy auto")
	}
}

/* TestResolveCLIConfigExplicitPipelineOverridesMode verifies -phases wins over mode presets. */
func TestResolveCLIConfigExplicitPipelineOverridesMode(t *testing.T) {
	resolution, err := resolveCLIConfig(cliOptions{
		mode:            modeBatch,
		modeSpecified:   true,
		phases:          "split",
		phasesSpecified: true,
	}, []string{"input.csv"})
	if err != nil {
		t.Fatalf("resolveCLIConfig() error = %v", err)
	}
	assertMainPhases(t, resolution.Config.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseSplit})
	if resolution.Config.Inputs.MainCSV != "input.csv" {
		t.Fatalf("MainCSV = %q, want input.csv", resolution.Config.Inputs.MainCSV)
	}
}

/* TestResolveCLIConfigValidateBatchFromConfig verifies config plus CLI override phase resolution. */
func TestResolveCLIConfigValidateBatchFromConfig(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "gvy.config.json")
	if err := os.WriteFile(configPath, []byte(`{
		"mode": "auto",
		"inputs": {
			"schema": "schema.json"
		}
	}`), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	resolution, err := resolveCLIConfig(cliOptions{
		configPath:        configPath,
		configSpecified:   true,
		phases:            "validate,batch",
		phasesSpecified:   true,
		inputDir:          "split",
		inputDirSpecified: true,
	}, nil)
	if err != nil {
		t.Fatalf("resolveCLIConfig() error = %v", err)
	}
	assertMainPhases(t, resolution.Config.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseValidate, gvyconfig.PhaseBatch})
	if resolution.Config.Inputs.ValidateDir != "split" {
		t.Fatalf("ValidateDir = %q, want split", resolution.Config.Inputs.ValidateDir)
	}
	if resolution.Config.Batch.InputDir != "success" {
		t.Fatalf("Batch.InputDir = %q, want success", resolution.Config.Batch.InputDir)
	}
}

/* TestResolveCLIConfigConfigOnlyServerUsesConfigMode verifies config-only server resolution. */
func TestResolveCLIConfigConfigOnlyServerUsesConfigMode(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "gvy.config.json")
	if err := os.WriteFile(configPath, []byte(`{
		"mode": "server",
		"server": {
			"host": "127.0.0.1",
			"port": 18080
		}
	}`), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	resolution, err := resolveCLIConfig(cliOptions{
		configPath:      configPath,
		configSpecified: true,
	}, nil)
	if err != nil {
		t.Fatalf("resolveCLIConfig() error = %v", err)
	}
	if resolution.Config.Mode != modeServer {
		t.Fatalf("Mode = %q, want server", resolution.Config.Mode)
	}
	if len(resolution.Config.Plan.Phases) != 0 {
		t.Fatalf("Plan.Phases = %v, want none", resolution.Config.Plan.Phases)
	}
	if resolution.Config.Server.Port != 18080 {
		t.Fatalf("Server.Port = %d, want 18080", resolution.Config.Server.Port)
	}
}

/* TestResolveCLIConfigEmptyInvocationDefaultsToServer verifies plain binary execution starts the UI server. */
func TestResolveCLIConfigEmptyInvocationDefaultsToServer(t *testing.T) {
	resolution, err := resolveCLIConfig(cliOptions{}, nil)
	if err != nil {
		t.Fatalf("resolveCLIConfig() error = %v", err)
	}
	if resolution.Config.Mode != modeServer {
		t.Fatalf("Mode = %q, want server", resolution.Config.Mode)
	}
	if len(resolution.Config.Plan.Phases) != 0 {
		t.Fatalf("Plan.Phases = %v, want none", resolution.Config.Plan.Phases)
	}
	if resolution.Config.Server.Host != "127.0.0.1" || resolution.Config.Server.Port != 1818 {
		t.Fatalf("Server = %s:%d, want 127.0.0.1:1818", resolution.Config.Server.Host, resolution.Config.Server.Port)
	}
}

func TestResolvedServerConsoleURLUsesRunManager(t *testing.T) {
	resolved := gvyconfig.ResolvedConfig{}
	resolved.Server.Host = "127.0.0.1"
	resolved.Server.Port = 1818

	if got, want := resolvedServerConsoleURL(resolved), "http://127.0.0.1:1818/run_manager"; got != want {
		t.Fatalf("resolvedServerConsoleURL() = %q, want %q", got, want)
	}
}

/* TestResolveCLIConfigConfigModeInterpretsPositionals verifies config mode controls positional overlays. */
func TestResolveCLIConfigConfigModeInterpretsPositionals(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "gvy.config.json")
	if err := os.WriteFile(configPath, []byte(`{
		"mode": "split",
		"inputs": {
			"main_csv": "from-config.csv"
		}
	}`), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	resolution, err := resolveCLIConfig(cliOptions{
		configPath:      configPath,
		configSpecified: true,
	}, []string{"from-cli.csv"})
	if err != nil {
		t.Fatalf("resolveCLIConfig() error = %v", err)
	}
	assertMainPhases(t, resolution.Config.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseSplit})
	if resolution.Config.Inputs.MainCSV != "from-cli.csv" {
		t.Fatalf("MainCSV = %q, want from-cli.csv", resolution.Config.Inputs.MainCSV)
	}
}

/* TestResolveCLIConfigBatchDefaultClearPreserved verifies legacy batch clear default. */
func TestResolveCLIConfigBatchDefaultClearPreserved(t *testing.T) {
	resolution, err := resolveCLIConfig(cliOptions{
		mode:              modeBatch,
		modeSpecified:     true,
		batchDir:          "success",
		batchDirSpecified: true,
	}, nil)
	if err != nil {
		t.Fatalf("resolveCLIConfig() error = %v", err)
	}
	assertMainPhases(t, resolution.Config.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseBatch})
	if !resolution.Config.Batch.ClearOutput {
		t.Fatal("Batch.ClearOutput = false, want true for legacy batch mode")
	}
}

/* TestPipelineOptionsFromResolvedMapsCLIConfigToOrchestrator verifies CLI dispatch wiring. */
func TestPipelineOptionsFromResolvedMapsCLIConfigToOrchestrator(t *testing.T) {
	resolution, err := resolveCLIConfig(cliOptions{
		phases:                  "validate,batch",
		phasesSpecified:         true,
		schemaPath:              "schema.json",
		schemaSpecified:         true,
		inputDir:                "split",
		inputDirSpecified:       true,
		batchExportDir:          "batches",
		batchExportDirSpecified: true,
		batchSize:               7,
		batchSizeSpecified:      true,
		threads:                 3,
		threadsSpecified:        true,
	}, nil)
	if err != nil {
		t.Fatalf("resolveCLIConfig() error = %v", err)
	}

	opts := pipelineOptionsFromResolved(resolution.Config, "")
	assertServicePhases(t, opts.Phases, []service.PipelinePhase{service.PipelinePhaseValidate, service.PipelinePhaseBatch})
	if opts.Validate.InputDir != "split" {
		t.Fatalf("Validate.InputDir = %q, want split", opts.Validate.InputDir)
	}
	if opts.Batch.InputDir != "success" {
		t.Fatalf("Batch.InputDir = %q, want success", opts.Batch.InputDir)
	}
	if opts.Batch.OutputDir != "batches" {
		t.Fatalf("Batch.OutputDir = %q, want batches", opts.Batch.OutputDir)
	}
	if opts.Batch.BatchSize != 7 || opts.Batch.Workers != 3 {
		t.Fatalf("Batch options = %+v, want size=7 workers=3", opts.Batch)
	}
	if opts.ClearValidationOutputDirs {
		t.Fatal("ClearValidationOutputDirs = true, want false outside legacy full auto")
	}
}

/* TestPipelineOptionsFromResolvedPreservesLegacyAutoClear verifies full auto clear mapping. */
func TestPipelineOptionsFromResolvedPreservesLegacyAutoClear(t *testing.T) {
	resolution, err := resolveCLIConfig(cliOptions{}, []string{"input.csv", "schema.json"})
	if err != nil {
		t.Fatalf("resolveCLIConfig() error = %v", err)
	}
	opts := pipelineOptionsFromResolved(resolution.Config, "Record ID")
	assertServicePhases(t, opts.Phases, []service.PipelinePhase{service.PipelinePhaseSplit, service.PipelinePhaseValidate, service.PipelinePhaseBatch})
	if !opts.ClearValidationOutputDirs {
		t.Fatal("ClearValidationOutputDirs = false, want true for legacy implicit auto")
	}
	if !opts.ReuseSplitCache {
		t.Fatal("ReuseSplitCache = false, want true from default resolved config")
	}

	explicitClearResolution, err := resolveCLIConfig(cliOptions{
		clearCache:    true,
		clearCacheSet: true,
	}, []string{"input.csv", "schema.json"})
	if err != nil {
		t.Fatalf("resolveCLIConfig() explicit clear error = %v", err)
	}
	explicitClearOpts := pipelineOptionsFromResolved(explicitClearResolution.Config, "Record ID")
	if !explicitClearOpts.ClearValidationOutputDirs {
		t.Fatal("explicit clear did not map to upfront validation output clearing")
	}
	if explicitClearOpts.Batch.ClearOutputDir {
		t.Fatal("Batch.ClearOutputDir = true, want false for full auto compatibility")
	}
}

func assertMainPhases(t *testing.T, got, want []gvyconfig.Phase) {
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

func assertServicePhases(t *testing.T, got, want []service.PipelinePhase) {
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
