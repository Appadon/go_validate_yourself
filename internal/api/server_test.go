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

	"go_validate_yourself/internal/progress"
	"go_validate_yourself/internal/runs"
	"go_validate_yourself/internal/service"
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
