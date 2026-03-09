package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"go_validate_yourself/internal/service"
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
