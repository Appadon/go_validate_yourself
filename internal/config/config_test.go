package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go_validate_yourself/internal/service"
)

func TestDefaultConfigResolvesToCurrentDefaults(t *testing.T) {
	cfg := Defaults()
	cfg.Inputs.MainCSV = "policies.csv"
	cfg.Inputs.Schema = "policy_schema.json"

	resolved, err := Normalize(cfg, NormalizeOptions{})
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}

	assertPhases(t, resolved.Plan.Phases, []Phase{PhaseSplit, PhaseValidate, PhaseBatch})
	if resolved.Mode != "auto" {
		t.Fatalf("Mode = %q, want auto", resolved.Mode)
	}
	if resolved.Outputs.SplitDir != "split" {
		t.Fatalf("SplitDir = %q, want split", resolved.Outputs.SplitDir)
	}
	if resolved.Outputs.SuccessDir != "success" {
		t.Fatalf("SuccessDir = %q, want success", resolved.Outputs.SuccessDir)
	}
	if resolved.Outputs.ErrorDir != "errors" {
		t.Fatalf("ErrorDir = %q, want errors", resolved.Outputs.ErrorDir)
	}
	if resolved.Outputs.BatchExportDir != "batch_export" {
		t.Fatalf("BatchExportDir = %q, want batch_export", resolved.Outputs.BatchExportDir)
	}
	if resolved.Split.MaxOpenWriters != 256 {
		t.Fatalf("Split.MaxOpenWriters = %d, want 256", resolved.Split.MaxOpenWriters)
	}
	if resolved.Split.MissingKeysFile != "missing_keys.parquet" {
		t.Fatalf("Split.MissingKeysFile = %q, want missing_keys.parquet", resolved.Split.MissingKeysFile)
	}
	if resolved.Batch.Size != 1000 {
		t.Fatalf("Batch.Size = %d, want 1000", resolved.Batch.Size)
	}
	if resolved.Server.Host != "127.0.0.1" || resolved.Server.Port != 1818 {
		t.Fatalf("Server = %s:%d, want 127.0.0.1:1818", resolved.Server.Host, resolved.Server.Port)
	}
	if resolved.Inputs.ValidateDir != "split" {
		t.Fatalf("ValidateDir = %q, want split", resolved.Inputs.ValidateDir)
	}
	if resolved.Batch.InputDir != "success" {
		t.Fatalf("Batch.InputDir = %q, want success", resolved.Batch.InputDir)
	}
}

func TestLoadFileRejectsUnknownFields(t *testing.T) {
	path := writeTempFile(t, t.TempDir(), "gvy.config.json", `{"unknown": true}`)

	_, err := LoadFile(path)
	if err == nil {
		t.Fatal("LoadFile() error = nil, want unknown field error")
	}
	if !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("LoadFile() error = %v, want unknown field error", err)
	}
}

func TestModeAutoExpandsToAllDataPhases(t *testing.T) {
	cfg := Defaults()
	cfg.Mode = "auto"
	cfg.Inputs.MainCSV = "input.csv"
	cfg.Inputs.Schema = "schema.json"

	resolved, err := Normalize(cfg, NormalizeOptions{})
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	assertPhases(t, resolved.Plan.Phases, []Phase{PhaseSplit, PhaseValidate, PhaseBatch})
}

func TestExplicitPipelinePhasesOverrideModePreset(t *testing.T) {
	cfg := Defaults()
	cfg.Mode = "batch"
	cfg.Pipeline.Phases = []Phase{PhaseSplit}
	cfg.Inputs.MainCSV = "input.csv"

	resolved, err := Normalize(cfg, NormalizeOptions{})
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	assertPhases(t, resolved.Plan.Phases, []Phase{PhaseSplit})
	if resolved.Batch.InputDir != "" {
		t.Fatalf("Batch.InputDir = %q, want empty when batch phase is not selected", resolved.Batch.InputDir)
	}
}

func TestServerModeHasNoDataPhases(t *testing.T) {
	cfg := Defaults()
	cfg.Mode = "server"

	resolved, err := Normalize(cfg, NormalizeOptions{})
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if len(resolved.Plan.Phases) != 0 {
		t.Fatalf("Plan.Phases = %v, want no data phases", resolved.Plan.Phases)
	}
}

func TestSplitOnlyConfigResolves(t *testing.T) {
	cfg := Defaults()
	cfg.Mode = "split"
	cfg.Inputs.MainCSV = "input.csv"

	resolved, err := Normalize(cfg, NormalizeOptions{})
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	assertPhases(t, resolved.Plan.Phases, []Phase{PhaseSplit})
	if resolved.Plan.SplitInputCSV != "input.csv" {
		t.Fatalf("Plan.SplitInputCSV = %q, want input.csv", resolved.Plan.SplitInputCSV)
	}
}

func TestValidateOnlyFromExistingSplitDirectoryResolves(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := writeTempFile(t, tempDir, "schema.json", `{"fields":[]}`)
	splitDir := filepath.Join(tempDir, "split")
	if err := os.Mkdir(splitDir, 0o755); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	cfg := Defaults()
	cfg.Pipeline.Phases = []Phase{PhaseValidate}
	cfg.Inputs.Schema = schemaPath
	cfg.Inputs.ValidateDir = splitDir

	resolved, err := Normalize(cfg, NormalizeOptions{RequireExistingInputs: true})
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	assertPhases(t, resolved.Plan.Phases, []Phase{PhaseValidate})
	if resolved.Plan.ValidateInputDir != splitDir {
		t.Fatalf("Plan.ValidateInputDir = %q, want %q", resolved.Plan.ValidateInputDir, splitDir)
	}
}

func TestBatchOnlyFromExistingParquetDirectoryResolves(t *testing.T) {
	tempDir := t.TempDir()
	parquetDir := filepath.Join(tempDir, "success")
	if err := os.Mkdir(parquetDir, 0o755); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	cfg := Defaults()
	cfg.Pipeline.Phases = []Phase{PhaseBatch}
	cfg.Batch.InputDir = parquetDir

	resolved, err := Normalize(cfg, NormalizeOptions{RequireExistingInputs: true})
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	assertPhases(t, resolved.Plan.Phases, []Phase{PhaseBatch})
	if resolved.Plan.BatchInputDir != parquetDir {
		t.Fatalf("Plan.BatchInputDir = %q, want %q", resolved.Plan.BatchInputDir, parquetDir)
	}
}

func TestInvalidPhaseInputCombinationsFailClearly(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{
			name: "split missing main csv",
			config: Config{
				Mode: "split",
			},
			wantErr: "inputs.main_csv",
		},
		{
			name: "validate missing schema",
			config: Config{
				Mode: "validate",
				Inputs: InputsConfig{
					ValidateDir: "split",
				},
			},
			wantErr: "inputs.schema",
		},
		{
			name: "validate missing input",
			config: Config{
				Mode: "validate",
				Inputs: InputsConfig{
					Schema: "schema.json",
				},
			},
			wantErr: "inputs.validate_dir",
		},
		{
			name: "validate conflicting inputs",
			config: Config{
				Mode: "validate",
				Inputs: InputsConfig{
					Schema:      "schema.json",
					ValidateCSV: "input.csv",
					ValidateDir: "split",
				},
			},
			wantErr: "cannot both be set",
		},
		{
			name: "batch missing input",
			config: Config{
				Mode: "batch",
			},
			wantErr: "batch.input_dir",
		},
		{
			name: "unknown phase",
			config: Config{
				Pipeline: PipelineConfig{
					Phases: []Phase{"export"},
				},
			},
			wantErr: "pipeline.phases[0]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Normalize(test.config, NormalizeOptions{})
			if err == nil {
				t.Fatal("Normalize() error = nil, want error")
			}
			if !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Normalize() error = %v, want substring %q", err, test.wantErr)
			}
		})
	}
}

func TestWorkerCountZeroResolvesToServiceDefaultThreadCount(t *testing.T) {
	cfg := Defaults()
	cfg.Inputs.MainCSV = "input.csv"
	cfg.Inputs.Schema = "schema.json"
	cfg.Runtime.Workers = 0

	resolved, err := Normalize(cfg, NormalizeOptions{})
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if resolved.EffectiveWorkers != service.DefaultThreadCount() {
		t.Fatalf("EffectiveWorkers = %d, want %d", resolved.EffectiveWorkers, service.DefaultThreadCount())
	}
}

func assertPhases(t *testing.T, got, want []Phase) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("phases = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("phases = %v, want %v", got, want)
		}
	}
}

func writeTempFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return path
}
