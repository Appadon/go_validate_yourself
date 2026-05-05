package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gvyconfig "go_validate_yourself/internal/config"
	"go_validate_yourself/internal/progress"
	"go_validate_yourself/internal/runs"
	"go_validate_yourself/internal/schemaeditor"
	"go_validate_yourself/internal/service"
	"go_validate_yourself/internal/validator"
	"go_validate_yourself/internal/workspace"
)

/* TestHandleHealthReturnsIdleStatus verifies the health endpoint reports an idle server. */
func TestHandleHealthReturnsIdleStatus(t *testing.T) {
	server := NewServer("127.0.0.1", 8080, service.New())
	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleHealth(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response HealthResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != "ok" {
		t.Fatalf("expected status ok, got %q", response.Status)
	}
	if response.Busy {
		t.Fatalf("expected idle server")
	}
}

/* TestHandleHealthReturnsBusyStatus verifies health reflects the active run manager state. */
func TestHandleHealthReturnsBusyStatus(t *testing.T) {
	server := NewServer("127.0.0.1", 8080, service.New())
	if _, err := server.runManager.Create("run-busy", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleHealth(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response HealthResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !response.Busy {
		t.Fatalf("expected busy server")
	}
}

/* TestHandleValidateAutoRejectsNonLoopback verifies loopback enforcement on run endpoints. */
func TestHandleValidateAutoRejectsNonLoopback(t *testing.T) {
	server := NewServer("127.0.0.1", 8080, service.New())
	request := httptest.NewRequest(http.MethodPost, "/run/validate-auto", bytes.NewBufferString(`{}`))
	request.RemoteAddr = "10.0.0.10:12345"
	recorder := httptest.NewRecorder()

	server.handleValidateAuto(recorder, request)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d", http.StatusForbidden, recorder.Code)
	}
}

/* TestBuildAutoOptionsDefaults verifies request defaults and absolute-path validation. */
func TestBuildAutoOptionsDefaults(t *testing.T) {
	tempDir := t.TempDir()
	inputPath := filepath.Join(tempDir, "Policies_WPP2.csv")
	schemaPath := filepath.Join(tempDir, "policy_schema.json")
	if err := os.WriteFile(inputPath, []byte("Record ID\n1\n"), 0o644); err != nil {
		t.Fatalf("write input: %v", err)
	}
	if err := os.WriteFile(schemaPath, []byte(`{"fields":[{"name":"Record ID","type":"string","required":true}]}`), 0o644); err != nil {
		t.Fatalf("write schema: %v", err)
	}

	server := NewServer("127.0.0.1", 8080, service.New())
	opts, err := server.buildAutoOptions(ValidateAutoRequest{
		InputCSV:   inputPath,
		SchemaPath: schemaPath,
	})
	if err != nil {
		t.Fatalf("build options: %v", err)
	}

	if opts.MainInputCSV != inputPath {
		t.Fatalf("expected main input %q, got %q", inputPath, opts.MainInputCSV)
	}
	if opts.BatchDir != opts.SuccessDir {
		t.Fatalf("expected batch dir to default to success dir")
	}
	if !opts.ClearValidationCache {
		t.Fatalf("expected clear validation cache default to true")
	}
}

func TestHandleConfigDefaultsReturnsUsableDefaults(t *testing.T) {
	server := NewServer("127.0.0.1", 8080, service.New())
	request := httptest.NewRequest(http.MethodGet, "/api/config/defaults", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleConfigDefaults(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response ConfigDefaultsResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !response.OK {
		t.Fatalf("expected ok response: %+v", response)
	}
	response.Defaults.Inputs.MainCSV = "input.csv"
	response.Defaults.Inputs.Schema = "schema.json"
	resolved, err := gvyconfig.Normalize(response.Defaults, gvyconfig.NormalizeOptions{})
	if err != nil {
		t.Fatalf("Normalize(defaults) error = %v", err)
	}
	if !configPhasesEqual(resolved.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseSplit, gvyconfig.PhaseValidate, gvyconfig.PhaseBatch}) {
		t.Fatalf("phases = %v, want auto phases", resolved.Plan.Phases)
	}
}

func TestHandleConfigResolveExpandsAutoAndDerivedInputs(t *testing.T) {
	server := NewServer("127.0.0.1", 8080, service.New())
	request := httptest.NewRequest(http.MethodPost, "/api/config/resolve", strings.NewReader(`{
		"mode": "auto",
		"inputs": {
			"main_csv": "input.csv",
			"schema": "schema.json"
		}
	}`))
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleConfigResolve(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response ConfigResolveResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !configPhasesEqual(response.ResolvedConfig.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseSplit, gvyconfig.PhaseValidate, gvyconfig.PhaseBatch}) {
		t.Fatalf("phases = %v, want auto phases", response.ResolvedConfig.Plan.Phases)
	}
	if response.ResolvedConfig.Inputs.ValidateDir != "split" {
		t.Fatalf("validate dir = %q, want split", response.ResolvedConfig.Inputs.ValidateDir)
	}
	if response.ResolvedConfig.Batch.InputDir != "success" {
		t.Fatalf("batch input dir = %q, want success", response.ResolvedConfig.Batch.InputDir)
	}
}

func TestHandleConfigResolveExplicitPhasesOverrideMode(t *testing.T) {
	server := NewServer("127.0.0.1", 8080, service.New())
	request := httptest.NewRequest(http.MethodPost, "/api/config/resolve", strings.NewReader(`{
		"mode": "batch",
		"pipeline": {"phases": ["split"]},
		"inputs": {"main_csv": "input.csv"}
	}`))
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleConfigResolve(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response ConfigResolveResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !configPhasesEqual(response.ResolvedConfig.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseSplit}) {
		t.Fatalf("phases = %v, want split only", response.ResolvedConfig.Plan.Phases)
	}
}

func TestHandleConfigResolveRejectsUnknownFields(t *testing.T) {
	server := NewServer("127.0.0.1", 8080, service.New())
	request := httptest.NewRequest(http.MethodPost, "/api/config/resolve", strings.NewReader(`{"mode":"auto","surprise":true}`))
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleConfigResolve(recorder, request)

	assertAPIErrorCode(t, recorder, http.StatusBadRequest, "INVALID_JSON")
	if !strings.Contains(recorder.Body.String(), "unknown field") {
		t.Fatalf("expected unknown field error, got %s", recorder.Body.String())
	}
}

func TestHandleErrorReportSummarizesErrorCSVs(t *testing.T) {
	tempDir := t.TempDir()
	t.Chdir(tempDir)
	if err := os.MkdirAll("errors", 0o755); err != nil {
		t.Fatalf("mkdir errors: %v", err)
	}
	errorCSV := strings.Join([]string{
		"__row_number,__errors,Policy Number,Name",
		`2,"Policy Number: value is required | Name: min length 3",P-1,A`,
		`3,"Policy Number: value is required",,Bob`,
		"",
	}, "\n")
	if err := os.WriteFile(filepath.Join("errors", "policies_error.csv"), []byte(errorCSV), 0o644); err != nil {
		t.Fatalf("write error csv: %v", err)
	}

	server := NewServer("127.0.0.1", 8080, service.New())
	request := httptest.NewRequest(http.MethodGet, "/api/errors/report?path=errors&field=Policy&limit=1", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleErrorReport(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response ErrorReportResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !response.OK {
		t.Fatalf("expected ok response: %+v", response)
	}
	if response.FileCount != 1 || response.ScannedRows != 2 || response.MatchedRows != 2 {
		t.Fatalf("unexpected counts: files=%d scanned=%d matched=%d", response.FileCount, response.ScannedRows, response.MatchedRows)
	}
	if len(response.Samples) != 1 || response.Samples[0].RowNumber != "2" {
		t.Fatalf("unexpected samples: %+v", response.Samples)
	}
	if len(response.Samples[0].Columns) != 2 {
		t.Fatalf("sample columns = %d, want all original columns", len(response.Samples[0].Columns))
	}
	if len(response.Samples[0].ErrorFields) != 2 || response.Samples[0].ErrorFields[0] != "Policy Number" {
		t.Fatalf("unexpected error fields: %+v", response.Samples[0].ErrorFields)
	}
	if len(response.Fields) == 0 || response.Fields[0].Name != "Policy Number" || response.Fields[0].Count != 2 {
		t.Fatalf("unexpected fields: %+v", response.Fields)
	}
}

func TestHandleErrorReportNormalizesQuotedMessageValues(t *testing.T) {
	tempDir := t.TempDir()
	t.Chdir(tempDir)
	if err := os.MkdirAll("errors", 0o755); err != nil {
		t.Fatalf("mkdir errors: %v", err)
	}
	errorCSV := strings.Join([]string{
		"__row_number,__errors,ID",
		`2,"ID: invalid float: ""ABC""",ABC`,
		`3,"ID: invalid float: ""XYZ""",XYZ`,
		"",
	}, "\n")
	if err := os.WriteFile(filepath.Join("errors", "ids_error.csv"), []byte(errorCSV), 0o644); err != nil {
		t.Fatalf("write error csv: %v", err)
	}

	server := NewServer("127.0.0.1", 8080, service.New())
	request := httptest.NewRequest(http.MethodGet, "/api/errors/report?path=errors", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleErrorReport(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response ErrorReportResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Messages) != 1 {
		t.Fatalf("messages = %+v, want one normalized pattern", response.Messages)
	}
	if response.Messages[0].Message != "invalid float: <value>" || response.Messages[0].Count != 2 {
		t.Fatalf("unexpected normalized message: %+v", response.Messages[0])
	}
	if len(response.Issues) != 2 {
		t.Fatalf("issues = %+v, want one issue per invalid value", response.Issues)
	}
	if response.Issues[0].Value != "ABC" || response.Issues[0].Message != "invalid float: <value>" || response.Issues[0].Count != 1 {
		t.Fatalf("unexpected first issue: %+v", response.Issues[0])
	}
	if !strings.Contains(response.Samples[0].Errors, `"ABC"`) {
		t.Fatalf("sample errors lost original value: %q", response.Samples[0].Errors)
	}
}

func TestHandleConfigResolveRejectsInvalidPhaseInputsClearly(t *testing.T) {
	server := NewServer("127.0.0.1", 8080, service.New())
	request := httptest.NewRequest(http.MethodPost, "/api/config/resolve", strings.NewReader(`{"mode":"validate","inputs":{"schema":"schema.json"}}`))
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleConfigResolve(recorder, request)

	assertAPIErrorCode(t, recorder, http.StatusBadRequest, "INVALID_CONFIG")
	if !strings.Contains(recorder.Body.String(), "inputs.validate_dir") {
		t.Fatalf("expected invalid input combination error, got %s", recorder.Body.String())
	}
}

func TestHandleConfigRunExecutesThroughRunPipeline(t *testing.T) {
	tempDir := t.TempDir()
	inputPath := filepath.Join(tempDir, "input.csv")
	schemaPath := filepath.Join(tempDir, "schema.json")
	writeTestFile(t, inputPath, "Record ID,Amount\n1,10\n")
	writeTestFile(t, schemaPath, `{"fields":[{"name":"Record ID","type":"string","required":true},{"name":"Amount","type":"int","required":true}]}`)

	server := NewServer("127.0.0.1", 8080, service.New())
	called := false
	server.runPipeline = func(ctx context.Context, opts service.PipelineOptions) (service.PipelineResult, error) {
		called = true
		if !servicePhasesEqual(opts.Phases, []service.PipelinePhase{service.PipelinePhaseSplit, service.PipelinePhaseValidate, service.PipelinePhaseBatch}) {
			t.Fatalf("pipeline phases = %v, want auto phases", opts.Phases)
		}
		if opts.Split.InputPath != inputPath || opts.Validate.SchemaPath != schemaPath {
			t.Fatalf("unexpected pipeline inputs: %+v %+v", opts.Split, opts.Validate)
		}
		return service.PipelineResult{
			Phases:    opts.Phases,
			RanPhases: opts.Phases,
		}, nil
	}

	body, err := json.Marshal(gvyconfig.Config{
		Mode: "auto",
		Inputs: gvyconfig.InputsConfig{
			MainCSV: inputPath,
			Schema:  schemaPath,
		},
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/runs/config", bytes.NewReader(body))
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleConfigRun(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if !called {
		t.Fatal("expected RunPipeline to be called")
	}
	var response ConfigRunResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Run.State != runs.StateCompleted {
		t.Fatalf("run state = %q, want completed", response.Run.State)
	}
	if response.ResolvedConfig.Inputs.MainCSV != inputPath {
		t.Fatalf("resolved input = %q, want %q", response.ResolvedConfig.Inputs.MainCSV, inputPath)
	}
	snapshot, ok := server.runManager.Snapshot(response.Run.RunID)
	if !ok || snapshot.FinalResult == nil {
		t.Fatalf("expected final snapshot result, got ok=%v snapshot=%+v", ok, snapshot)
	}
	finalBytes, err := json.Marshal(snapshot.FinalResult)
	if err != nil {
		t.Fatalf("marshal final result: %v", err)
	}
	if !strings.Contains(string(finalBytes), "resolved_config") {
		t.Fatalf("snapshot final result missing resolved config: %s", string(finalBytes))
	}
}

func TestHandleConfigRunRejectsNonLoopback(t *testing.T) {
	server := NewServer("127.0.0.1", 8080, service.New())
	request := httptest.NewRequest(http.MethodPost, "/api/runs/config", strings.NewReader(`{}`))
	request.RemoteAddr = "10.0.0.10:12345"
	recorder := httptest.NewRecorder()

	server.handleConfigRun(recorder, request)

	assertAPIErrorCode(t, recorder, http.StatusForbidden, "FORBIDDEN")
}

func TestHandleUIRendersWorkingRootPage(t *testing.T) {
	server := newSelectionTestServer(t)
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleUI(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if contentType := recorder.Header().Get("Content-Type"); !strings.Contains(contentType, "text/html") {
		t.Fatalf("Content-Type = %q", contentType)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "Validation Console") {
		t.Fatalf("body missing UI heading: %s", body)
	}
	if !strings.Contains(body, server.workingRoot) {
		t.Fatalf("body missing working root %q: %s", server.workingRoot, body)
	}
}

func TestHandleSchemaEditorUIRendersWorkingRootPage(t *testing.T) {
	server := newSelectionTestServer(t)
	request := httptest.NewRequest(http.MethodGet, "/schema-editor", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleSchemaEditorUI(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "Schema Editor") {
		t.Fatalf("body missing schema editor heading: %s", body)
	}
	if !strings.Contains(body, server.workingRoot) {
		t.Fatalf("body missing working root %q: %s", server.workingRoot, body)
	}
}

func TestHandleSchemaWorkbenchUIRendersWorkingRootPage(t *testing.T) {
	server := newSelectionTestServer(t)
	request := httptest.NewRequest(http.MethodGet, "/schema-workbench", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleSchemaWorkbenchUI(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "Schema Editor") {
		t.Fatalf("body missing schema editor heading: %s", body)
	}
	if !strings.Contains(body, server.workingRoot) {
		t.Fatalf("body missing working root %q: %s", server.workingRoot, body)
	}
}

func TestHandleSchemaInferUIRendersWorkingRootPage(t *testing.T) {
	server := newSelectionTestServer(t)
	request := httptest.NewRequest(http.MethodGet, "/schema-infer", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleSchemaInferUI(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "Schema Inference") {
		t.Fatalf("body missing schema inference heading: %s", body)
	}
	if !strings.Contains(body, server.workingRoot) {
		t.Fatalf("body missing working root %q: %s", server.workingRoot, body)
	}
}

func TestHandleSchemaDocumentReadReturnsValidatedSchema(t *testing.T) {
	server := newSelectionTestServer(t)
	writeTestFile(t, filepath.Join(server.workingRoot, "schemas", "schema.json"), `{
		"fields": [
			{"name": "Record ID", "type": "string", "required": true},
			{"name": "Amount", "type": "float"}
		]
	}`)

	request := httptest.NewRequest(http.MethodGet, "/api/schema?path=schemas/schema.json", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleSchemaDocument(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response SchemaDocumentResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.RelativePath != "schemas/schema.json" {
		t.Fatalf("relative path = %q", response.RelativePath)
	}
	if response.Schema.Fields[0].ParquetName != "record_id" {
		t.Fatalf("expected normalized parquet name, got %+v", response.Schema.Fields[0])
	}
}

func TestHandleSchemaDocumentSaveWritesValidatedSchema(t *testing.T) {
	server := newSelectionTestServer(t)
	writeTestFile(t, filepath.Join(server.workingRoot, "schemas", ".keep"), "")

	body := `{
		"path": "schemas/saved.json",
		"schema": {
			"fields": [
				{"name": "Record ID", "type": "string", "required": true},
				{"name": "Amount", "type": "int", "non_zero": true}
			]
		}
	}`
	request := httptest.NewRequest(http.MethodPut, "/api/schema", strings.NewReader(body))
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleSchemaDocument(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	data, err := os.ReadFile(filepath.Join(server.workingRoot, "schemas", "saved.json"))
	if err != nil {
		t.Fatalf("ReadFile(saved schema) error = %v", err)
	}
	if !strings.Contains(string(data), `"parquet_name": "record_id"`) {
		t.Fatalf("saved schema missing normalized parquet name: %s", string(data))
	}
}

func TestHandleSchemaDocumentSaveRejectsOutOfRootPath(t *testing.T) {
	server := newSelectionTestServer(t)
	outsidePath := filepath.Join(t.TempDir(), "schema.json")
	body, err := json.Marshal(SchemaSaveRequest{
		Path: outsidePath,
		Schema: schemaeditor.Document{
			Fields: []validator.FieldRule{{Name: "Record ID", Type: "string"}},
		},
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	request := httptest.NewRequest(http.MethodPut, "/api/schema", bytes.NewReader(body))
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleSchemaDocument(recorder, request)

	assertAPIErrorCode(t, recorder, http.StatusBadRequest, "INVALID_SCHEMA_PATH")
}

func TestHandleSchemaInferReturnsDraftSchemaAndSampleParquet(t *testing.T) {
	server := newSelectionTestServer(t)
	writeTestFile(t, filepath.Join(server.workingRoot, "incoming", "input.csv"), strings.Join([]string{
		"id,amount,event_date,created_at,status,optional_note",
		"1,10.50,2026-05-04,2026-05-04T12:00:00Z,active,present",
		"2,11.25,2026-05-05,2026-05-05T12:00:00Z,pending,",
		"3,12.00,2026-05-06,2026-05-06T12:00:00Z,active,present",
		"",
	}, "\n"))

	body := `{
		"csv_path": "incoming/input.csv",
		"sample_size": 3,
		"strategy": "head",
		"write_sample_parquet": true
	}`
	request := httptest.NewRequest(http.MethodPost, "/api/schema/infer", strings.NewReader(body))
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleSchemaInfer(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response SchemaInferResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !response.OK {
		t.Fatalf("expected ok response: %+v", response)
	}
	if response.CSVRelativePath != "incoming/input.csv" {
		t.Fatalf("csv relative path = %q", response.CSVRelativePath)
	}
	if response.Inference.Schema.Fields[0].Type != "int" {
		t.Fatalf("id type = %q, want int", response.Inference.Schema.Fields[0].Type)
	}
	if response.Inference.Schema.Fields[1].Type != "float" {
		t.Fatalf("amount type = %q, want float", response.Inference.Schema.Fields[1].Type)
	}
	if response.SampleParquetRelativePath != ".gvy/schema_samples/input.sample.parquet" {
		t.Fatalf("sample parquet relative path = %q", response.SampleParquetRelativePath)
	}
	if _, err := os.Stat(response.SampleParquetPath); err != nil {
		t.Fatalf("sample parquet was not written: %v", err)
	}
}

func TestHandleFileListReturnsEligibleWorkingRootFiles(t *testing.T) {
	server := newSelectionTestServer(t)
	writeTestFile(t, filepath.Join(server.workingRoot, "incoming", "alpha.csv"), "Record ID\n1\n")
	writeTestFile(t, filepath.Join(server.workingRoot, "incoming", "nested", "beta.csv"), "Record ID\n2\n")
	writeTestFile(t, filepath.Join(server.workingRoot, "schemas", "policy.json"), `{"fields":[]}`)
	writeTestFile(t, filepath.Join(server.workspaceBaseDir, "run-old", "run.json"), `{"ok":true}`)

	request := httptest.NewRequest(http.MethodGet, "/api/files?kind=csv", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleFileList(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response FileListResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.CurrentPath != "" {
		t.Fatalf("current path = %q, want root", response.CurrentPath)
	}
	if len(response.Entries) != 2 {
		t.Fatalf("expected 2 root entries, got %d", len(response.Entries))
	}
	if !response.Entries[0].IsDir || response.Entries[0].RelativePath != "incoming" {
		t.Fatalf("first entry = %+v", response.Entries[0])
	}
	if !response.Entries[1].IsDir || response.Entries[1].RelativePath != "schemas" {
		t.Fatalf("second entry = %+v", response.Entries[1])
	}
}

func TestHandleFileListBrowsesCurrentDirectoryOnly(t *testing.T) {
	server := newSelectionTestServer(t)
	writeTestFile(t, filepath.Join(server.workingRoot, "incoming", "alpha.csv"), "Record ID\n1\n")
	writeTestFile(t, filepath.Join(server.workingRoot, "incoming", "nested", "beta.csv"), "Record ID\n2\n")

	request := httptest.NewRequest(http.MethodGet, "/api/files?kind=csv&path=incoming", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleFileList(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response FileListResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.CurrentPath != "incoming" {
		t.Fatalf("current path = %q", response.CurrentPath)
	}
	if response.ParentPath != "" {
		t.Fatalf("parent path = %q, want root parent", response.ParentPath)
	}
	if len(response.Entries) != 2 {
		t.Fatalf("expected 2 incoming entries, got %d", len(response.Entries))
	}
	if !response.Entries[0].IsDir || response.Entries[0].RelativePath != "incoming/nested" {
		t.Fatalf("nested dir entry = %+v", response.Entries[0])
	}
	if response.Entries[1].IsDir || response.Entries[1].RelativePath != "incoming/alpha.csv" {
		t.Fatalf("file entry = %+v", response.Entries[1])
	}
}

func TestHandleCreateRunFromSelectionSuccessAndInspectability(t *testing.T) {
	server := newSelectionTestServer(t)
	csvPath := filepath.Join(server.workingRoot, "incoming", "input.csv")
	schemaPath := filepath.Join(server.workingRoot, "schemas", "schema.json")
	writeTestFile(t, csvPath, "Record ID,Amount\n1,10\n2,20\n")
	writeTestFile(t, schemaPath, `{"fields":[{"name":"Record ID","type":"string","required":true},{"name":"Amount","type":"int","required":true}]}`)

	requestBody, err := json.Marshal(FileSelectionRunRequest{
		CSVPath:    "incoming/input.csv",
		SchemaPath: "schemas/schema.json",
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, "/api/runs", bytes.NewReader(requestBody))
	request.Header.Set("Content-Type", "application/json")
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleRuns(recorder, request)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d body=%s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	response := decodeCreatedRun(t, recorder.Body.Bytes())
	if response.Run.Workspace == nil {
		t.Fatal("expected workspace in create response")
	}
	if response.Run.Workspace.InputCSVPath != csvPath {
		t.Fatalf("input csv = %q, want %q", response.Run.Workspace.InputCSVPath, csvPath)
	}
	if response.Run.Workspace.SchemaPath != schemaPath {
		t.Fatalf("schema path = %q, want %q", response.Run.Workspace.SchemaPath, schemaPath)
	}

	finalSnapshot := waitForRunState(t, server.runManager, response.Run.RunID, runs.StateCompleted)
	if finalSnapshot.FinalResult == nil {
		t.Fatal("expected final result")
	}

	resultRequest := httptest.NewRequest(http.MethodGet, "/api/runs/"+response.Run.RunID+"/result", nil)
	resultRequest.RemoteAddr = "127.0.0.1:12345"
	resultRecorder := httptest.NewRecorder()
	server.handleRunByID(resultRecorder, resultRequest)
	if resultRecorder.Code != http.StatusOK {
		t.Fatalf("result status = %d, want %d", resultRecorder.Code, http.StatusOK)
	}
}

func TestHandleCreateRunFromSelectionRejectsOutOfRootPath(t *testing.T) {
	server := newSelectionTestServer(t)
	outsideDir := t.TempDir()
	writeTestFile(t, filepath.Join(outsideDir, "escape.csv"), "Record ID\n1\n")
	writeTestFile(t, filepath.Join(server.workingRoot, "schemas", "schema.json"), `{"fields":[]}`)

	requestBody, err := json.Marshal(FileSelectionRunRequest{
		CSVPath:    filepath.Join(outsideDir, "escape.csv"),
		SchemaPath: "schemas/schema.json",
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, "/api/runs", bytes.NewReader(requestBody))
	request.Header.Set("Content-Type", "application/json")
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleRuns(recorder, request)

	assertAPIErrorCode(t, recorder, http.StatusBadRequest, "INVALID_REQUEST")
	if !strings.Contains(recorder.Body.String(), "server working directory") {
		t.Fatalf("expected out-of-root error, got %s", recorder.Body.String())
	}
}

func TestHandleCreateRunRejectsInvalidMultipart(t *testing.T) {
	server := newUploadTestServer(t)
	request := httptest.NewRequest(http.MethodPost, "/api/runs", strings.NewReader("not multipart"))
	request.Header.Set("Content-Type", "text/plain")
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleRuns(recorder, request)

	assertAPIErrorCode(t, recorder, http.StatusBadRequest, "INVALID_MULTIPART")
}

func TestHandleCreateRunValidatesRequiredUploadsAndExtensions(t *testing.T) {
	testCases := []struct {
		name       string
		files      []uploadFile
		statusCode int
		errorCode  string
	}{
		{
			name:       "missing csv",
			files:      []uploadFile{{Field: "schema", Name: "schema.json", Body: `{"fields":[]}`}},
			statusCode: http.StatusBadRequest,
			errorCode:  "MISSING_CSV_UPLOAD",
		},
		{
			name:       "missing schema",
			files:      []uploadFile{{Field: "csv", Name: "input.csv", Body: "Record ID\n1\n"}},
			statusCode: http.StatusBadRequest,
			errorCode:  "MISSING_SCHEMA_UPLOAD",
		},
		{
			name: "invalid csv extension",
			files: []uploadFile{
				{Field: "csv", Name: "input.txt", Body: "Record ID\n1\n"},
				{Field: "schema", Name: "schema.json", Body: `{"fields":[]}`},
			},
			statusCode: http.StatusBadRequest,
			errorCode:  "INVALID_CSV_EXTENSION",
		},
		{
			name: "invalid schema extension",
			files: []uploadFile{
				{Field: "csv", Name: "input.csv", Body: "Record ID\n1\n"},
				{Field: "schema", Name: "schema.txt", Body: `{"fields":[]}`},
			},
			statusCode: http.StatusBadRequest,
			errorCode:  "INVALID_SCHEMA_EXTENSION",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := newUploadTestServer(t)
			request := newMultipartRequest(t, "/api/runs", testCase.files)
			recorder := httptest.NewRecorder()

			server.handleRuns(recorder, request)

			assertAPIErrorCode(t, recorder, testCase.statusCode, testCase.errorCode)
		})
	}
}

func TestHandleCreateRunReturnsBusyWhenAnotherRunIsActive(t *testing.T) {
	server := newUploadTestServer(t)
	runStarted := make(chan struct{}, 1)
	releaseRun := make(chan struct{})
	server.runAuto = func(ctx context.Context, opts service.AutoOptions) (service.AutoResult, error) {
		runStarted <- struct{}{}
		<-releaseRun
		return service.AutoResult{MainInputCSV: opts.MainInputCSV, SchemaPath: opts.SchemaPath}, nil
	}

	firstRequest := newMultipartRequest(t, "/api/runs", validUploadFiles())
	firstRecorder := httptest.NewRecorder()
	server.handleRuns(firstRecorder, firstRequest)
	if firstRecorder.Code != http.StatusCreated {
		t.Fatalf("first create status = %d, want %d", firstRecorder.Code, http.StatusCreated)
	}
	<-runStarted

	secondRequest := newMultipartRequest(t, "/api/runs", validUploadFiles())
	secondRecorder := httptest.NewRecorder()
	server.handleRuns(secondRecorder, secondRequest)
	assertAPIErrorCode(t, secondRecorder, http.StatusConflict, "BUSY")

	close(releaseRun)
	waitForRunState(t, server.runManager, decodeCreatedRun(t, firstRecorder.Body.Bytes()).Run.RunID, runs.StateCompleted)
}

func TestHandleCreateRunSuccessAndSnapshotLifecycle(t *testing.T) {
	server := newUploadTestServer(t)
	request := newMultipartRequest(t, "/api/runs", validUploadFiles())
	recorder := httptest.NewRecorder()

	server.handleRuns(recorder, request)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d", recorder.Code, http.StatusCreated)
	}
	response := decodeCreatedRun(t, recorder.Body.Bytes())
	if !response.OK {
		t.Fatalf("expected ok response: %+v", response)
	}
	if response.Run.State != runs.StateRunning {
		t.Fatalf("initial state = %q, want %q", response.Run.State, runs.StateRunning)
	}
	if response.Run.Workspace == nil {
		t.Fatal("expected workspace in create response")
	}
	if location := recorder.Header().Get("Location"); location != "/api/runs/"+response.Run.RunID {
		t.Fatalf("Location header = %q", location)
	}

	finalSnapshot := waitForRunState(t, server.runManager, response.Run.RunID, runs.StateCompleted)
	if finalSnapshot.FinalResult == nil {
		t.Fatal("expected final result")
	}

	snapshotRequest := httptest.NewRequest(http.MethodGet, "/api/runs/"+response.Run.RunID, nil)
	snapshotRequest.RemoteAddr = "127.0.0.1:12345"
	snapshotRecorder := httptest.NewRecorder()
	server.handleRunByID(snapshotRecorder, snapshotRequest)
	if snapshotRecorder.Code != http.StatusOK {
		t.Fatalf("snapshot status = %d, want %d", snapshotRecorder.Code, http.StatusOK)
	}

	var snapshotResponse RunSnapshotResponse
	if err := json.Unmarshal(snapshotRecorder.Body.Bytes(), &snapshotResponse); err != nil {
		t.Fatalf("decode snapshot response: %v", err)
	}
	if snapshotResponse.Run.State != runs.StateCompleted {
		t.Fatalf("snapshot state = %q", snapshotResponse.Run.State)
	}

	resultRequest := httptest.NewRequest(http.MethodGet, "/api/runs/"+response.Run.RunID+"/result", nil)
	resultRequest.RemoteAddr = "127.0.0.1:12345"
	resultRecorder := httptest.NewRecorder()
	server.handleRunByID(resultRecorder, resultRequest)
	if resultRecorder.Code != http.StatusOK {
		t.Fatalf("result status = %d, want %d", resultRecorder.Code, http.StatusOK)
	}

	var resultResponse RunResultResponse
	if err := json.Unmarshal(resultRecorder.Body.Bytes(), &resultResponse); err != nil {
		t.Fatalf("decode result response: %v", err)
	}
	if resultResponse.State != runs.StateCompleted {
		t.Fatalf("result state = %q", resultResponse.State)
	}

	metadata, err := os.ReadFile(response.Run.Workspace.MetadataPath)
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	if !strings.Contains(string(metadata), response.Run.RunID) {
		t.Fatalf("metadata did not include run id: %s", string(metadata))
	}
}

func TestHandleCreateRunExecutionFailureRemainsInspectable(t *testing.T) {
	server := newUploadTestServer(t)
	request := newMultipartRequest(t, "/api/runs", []uploadFile{
		{Field: "csv", Name: "input.csv", Body: "Record ID,Amount\n1,10\n"},
		{Field: "schema", Name: "schema.json", Body: `{"fields":[{"name":"Record ID","type":"bogus"}]}`},
	})
	recorder := httptest.NewRecorder()

	server.handleRuns(recorder, request)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d", recorder.Code, http.StatusCreated)
	}
	runID := decodeCreatedRun(t, recorder.Body.Bytes()).Run.RunID
	snapshot := waitForRunState(t, server.runManager, runID, runs.StateFailed)
	if snapshot.FinalError == "" {
		t.Fatal("expected final error")
	}

	resultRequest := httptest.NewRequest(http.MethodGet, "/api/runs/"+runID+"/result", nil)
	resultRequest.RemoteAddr = "127.0.0.1:12345"
	resultRecorder := httptest.NewRecorder()
	server.handleRunByID(resultRecorder, resultRequest)
	if resultRecorder.Code != http.StatusOK {
		t.Fatalf("result status = %d, want %d", resultRecorder.Code, http.StatusOK)
	}
	if !strings.Contains(resultRecorder.Body.String(), "final_error") {
		t.Fatalf("result body missing final_error: %s", resultRecorder.Body.String())
	}
}

func TestHandleRunByIDUnknownRun(t *testing.T) {
	server := newUploadTestServer(t)

	snapshotRequest := httptest.NewRequest(http.MethodGet, "/api/runs/run-missing", nil)
	snapshotRequest.RemoteAddr = "127.0.0.1:12345"
	snapshotRecorder := httptest.NewRecorder()
	server.handleRunByID(snapshotRecorder, snapshotRequest)
	assertAPIErrorCode(t, snapshotRecorder, http.StatusNotFound, "RUN_NOT_FOUND")

	resultRequest := httptest.NewRequest(http.MethodGet, "/api/runs/run-missing/result", nil)
	resultRequest.RemoteAddr = "127.0.0.1:12345"
	resultRecorder := httptest.NewRecorder()
	server.handleRunByID(resultRecorder, resultRequest)
	assertAPIErrorCode(t, resultRecorder, http.StatusNotFound, "RUN_NOT_FOUND")
}

func TestHandleRunEventsUnknownRun(t *testing.T) {
	server := newUploadTestServer(t)
	request := httptest.NewRequest(http.MethodGet, "/api/runs/run-missing/events", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleRunByID(recorder, request)

	assertAPIErrorCode(t, recorder, http.StatusNotFound, "RUN_NOT_FOUND")
}

func TestHandleRunEventsStreamsReplayAndLiveEvents(t *testing.T) {
	server := newUploadTestServer(t)
	ws := mustCreateWorkspace(t, server)
	if _, err := server.runManager.Create("run-events", &ws); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := server.runManager.Start("run-events"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if _, err := server.runManager.AppendEvent("run-events", progress.Event{
		RunID:   "run-events",
		Time:    time.Now().UTC(),
		Phase:   progress.PhaseRun,
		Type:    progress.TypeStarted,
		Message: "replay event",
	}); err != nil {
		t.Fatalf("AppendEvent(replay) error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := httptest.NewRequest(http.MethodGet, "/api/runs/run-events/events", nil).WithContext(ctx)
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		server.handleRunByID(recorder, request)
		close(done)
	}()

	waitForCondition(t, func() bool {
		return strings.Contains(recorder.Body.String(), "replay event")
	})

	if _, err := server.runManager.AppendEvent("run-events", progress.Event{
		RunID:   "run-events",
		Time:    time.Now().UTC(),
		Phase:   progress.PhaseValidate,
		Type:    progress.TypeProgress,
		Message: "live event",
	}); err != nil {
		t.Fatalf("AppendEvent(live) error = %v", err)
	}

	waitForCondition(t, func() bool {
		return strings.Contains(recorder.Body.String(), "live event")
	})

	cancel()
	<-done

	body := recorder.Body.String()
	if recorder.Code != http.StatusOK {
		t.Fatalf("events status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "text/event-stream" {
		t.Fatalf("Content-Type = %q", contentType)
	}
	if !strings.Contains(body, "event: progress") {
		t.Fatalf("expected SSE event lines in body: %s", body)
	}
}

func TestHandleValidateAutoPreservesExistingPathBasedBehavior(t *testing.T) {
	tempDir := t.TempDir()
	inputPath := filepath.Join(tempDir, "input.csv")
	schemaPath := filepath.Join(tempDir, "schema.json")
	writeTestFile(t, inputPath, "Record ID,Amount\n1,10\n2,20\n")
	writeTestFile(t, schemaPath, `{"fields":[{"name":"Record ID","type":"string","required":true},{"name":"Amount","type":"int","required":true}]}`)

	requestBody, err := json.Marshal(ValidateAutoRequest{
		InputCSV:   inputPath,
		SchemaPath: schemaPath,
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	server := NewServer("127.0.0.1", 8080, service.New())
	request := httptest.NewRequest(http.MethodPost, "/run/validate-auto", bytes.NewReader(requestBody))
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()

	server.handleValidateAuto(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}

	var response ValidateAutoSuccessResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !response.OK || response.Mode != "auto" {
		t.Fatalf("unexpected response: %+v", response)
	}
	if response.Result.Validation.FileCount == 0 {
		t.Fatalf("expected validation output: %+v", response.Result.Validation)
	}
}

type uploadFile struct {
	Field string
	Name  string
	Body  string
}

func newUploadTestServer(t *testing.T) *Server {
	t.Helper()
	server := NewServer("127.0.0.1", 8080, service.New())
	server.workspaceBaseDir = t.TempDir()
	server.runAuto = server.service.RunAuto
	server.detectPrimaryKey = service.DetectPrimaryKey
	return server
}

func newSelectionTestServer(t *testing.T) *Server {
	t.Helper()
	server := NewServer("127.0.0.1", 8080, service.New())
	server.workingRoot = t.TempDir()
	server.workingRootReal = resolveRealPath(server.workingRoot)
	server.workspaceBaseDir = filepath.Join(server.workingRoot, ".gvy", "runs")
	server.runAuto = server.service.RunAuto
	server.detectPrimaryKey = service.DetectPrimaryKey
	return server
}

func validUploadFiles() []uploadFile {
	return []uploadFile{
		{Field: "csv", Name: "input.csv", Body: "Record ID,Amount\n1,10\n2,20\n"},
		{Field: "schema", Name: "schema.json", Body: `{"fields":[{"name":"Record ID","type":"string","required":true},{"name":"Amount","type":"int","required":true}]}`},
	}
}

func newMultipartRequest(t *testing.T, path string, files []uploadFile) *http.Request {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for _, file := range files {
		part, err := writer.CreateFormFile(file.Field, file.Name)
		if err != nil {
			t.Fatalf("CreateFormFile(%q) error = %v", file.Field, err)
		}
		if _, err := io.Copy(part, strings.NewReader(file.Body)); err != nil {
			t.Fatalf("write multipart %q: %v", file.Field, err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, path, &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	request.RemoteAddr = "127.0.0.1:12345"
	return request
}

func decodeCreatedRun(t *testing.T, body []byte) RunCreateResponse {
	t.Helper()
	var response RunCreateResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	return response
}

func assertAPIErrorCode(t *testing.T, recorder *httptest.ResponseRecorder, statusCode int, errorCode string) {
	t.Helper()
	if recorder.Code != statusCode {
		t.Fatalf("status = %d, want %d body=%s", recorder.Code, statusCode, recorder.Body.String())
	}
	var response ErrorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response.ErrorCode != errorCode {
		t.Fatalf("error_code = %q, want %q body=%s", response.ErrorCode, errorCode, recorder.Body.String())
	}
}

func servicePhasesEqual(actual, expected []service.PipelinePhase) bool {
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

func waitForRunState(t *testing.T, manager *runs.Manager, runID string, state runs.State) runs.Snapshot {
	t.Helper()
	var snapshot runs.Snapshot
	waitForCondition(t, func() bool {
		var ok bool
		snapshot, ok = manager.Snapshot(runID)
		return ok && snapshot.State == state
	})
	return snapshot
}

func waitForCondition(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func mustCreateWorkspace(t *testing.T, server *Server) workspace.RunWorkspace {
	t.Helper()
	ws, err := workspace.NewUnder(server.workspaceBaseDir, "run-events")
	if err != nil {
		t.Fatalf("workspace.NewUnder() error = %v", err)
	}
	if err := ws.Prepare(); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	return ws
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
}

func TestHandleGetRunResultRejectsNonTerminalRun(t *testing.T) {
	server := newUploadTestServer(t)
	ws := mustCreateWorkspace(t, server)
	if _, err := server.runManager.Create("run-running", &ws); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "/api/runs/run-running/result", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	recorder := httptest.NewRecorder()
	server.handleRunByID(recorder, request)

	assertAPIErrorCode(t, recorder, http.StatusConflict, "RUN_NOT_FINISHED")
}

func TestParseRunRoute(t *testing.T) {
	testCases := []struct {
		path   string
		runID  string
		suffix string
		ok     bool
	}{
		{path: "/api/runs/run-1", runID: "run-1", suffix: "", ok: true},
		{path: "/api/runs/run-1/events", runID: "run-1", suffix: "/events", ok: true},
		{path: "/api/runs/run-1/result", runID: "run-1", suffix: "/result", ok: true},
		{path: "/api/runs/", ok: false},
		{path: "/api/runs/run-1/events/live", ok: false},
	}

	for _, testCase := range testCases {
		runID, suffix, ok := parseRunRoute(testCase.path)
		if runID != testCase.runID || suffix != testCase.suffix || ok != testCase.ok {
			t.Fatalf("parseRunRoute(%q) = (%q, %q, %v)", testCase.path, runID, suffix, ok)
		}
	}
}
