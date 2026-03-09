package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go_validate_yourself/internal/service"
)

const version = "v1"

/* Server provides a localhost-only HTTP API around the workflow service. */
type Server struct {
	host         string
	port         int
	service      service.Service
	httpServer   *http.Server
	executionSem chan struct{}
	shutdownOnce sync.Once
}

/* HealthResponse reports server status and execution availability. */
type HealthResponse struct {
	Status  string `json:"status"`
	Busy    bool   `json:"busy"`
	Version string `json:"version"`
}

/* ErrorResponse is the shared structured error body for API failures. */
type ErrorResponse struct {
	OK        bool   `json:"ok"`
	ErrorCode string `json:"error_code"`
	Message   string `json:"message"`
}

/* ValidateAutoRequest defines inputs accepted by POST /run/validate-auto. */
type ValidateAutoRequest struct {
	InputCSV             string `json:"input_csv"`
	MainInputCSV         string `json:"main_input_csv"`
	SchemaPath           string `json:"schema_path"`
	SplitOutputDir       string `json:"split_output_dir"`
	SplitPrimaryKey      string `json:"split_primary_key"`
	SplitMaxOpen         int    `json:"split_max_open"`
	SplitMissingFile     string `json:"split_missing_file"`
	Threads              int    `json:"threads"`
	WriteEmptyError      *bool  `json:"write_empty_error"`
	ClearValidationCache *bool  `json:"clear_validation_cache"`
	SuccessDir           string `json:"success_dir"`
	ErrorDir             string `json:"error_dir"`
	BatchDir             string `json:"batch_dir"`
	BatchExportDir       string `json:"batch_export_dir"`
	BatchSize            int    `json:"batch_size"`
}

/* ValidateAutoSuccessResponse returns the final result for a completed auto run. */
type ValidateAutoSuccessResponse struct {
	OK      bool               `json:"ok"`
	Mode    string             `json:"mode"`
	Outputs ValidateAutoOutput `json:"outputs"`
	Result  service.AutoResult `json:"result"`
}

/* ValidateAutoOutput exposes the main output directories for the frontend. */
type ValidateAutoOutput struct {
	SplitOutputDir string `json:"split_output_dir"`
	SuccessDir     string `json:"success_dir"`
	ErrorDir       string `json:"error_dir"`
	BatchDir       string `json:"batch_dir"`
	BatchExportDir string `json:"batch_export_dir"`
}

/* NewServer constructs a localhost-only API server instance. */
func NewServer(host string, port int, svc service.Service) *Server {
	server := &Server{
		host:         host,
		port:         port,
		service:      svc,
		executionSem: make(chan struct{}, 1),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", server.handleHealth)
	mux.HandleFunc("/shutdown", server.handleShutdown)
	mux.HandleFunc("/run/validate-auto", server.handleValidateAuto)

	server.httpServer = &http.Server{
		Addr:              fmt.Sprintf("%s:%d", host, port),
		Handler:           server.withSecurityHeaders(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return server
}

/* ListenAndServe starts the HTTP server and blocks until it stops. */
func (s *Server) ListenAndServe() error {
	err := s.httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

/* handleHealth returns basic service health and busy state. */
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if !s.allowMethod(w, r, http.MethodGet) || !s.requireLoopback(w, r) {
		return
	}
	writeJSON(w, http.StatusOK, HealthResponse{
		Status:  "ok",
		Busy:    s.isBusy(),
		Version: version,
	})
}

/* handleShutdown accepts a localhost shutdown request and stops the server. */
func (s *Server) handleShutdown(w http.ResponseWriter, r *http.Request) {
	if !s.allowMethod(w, r, http.MethodPost) || !s.requireLoopback(w, r) {
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"ok":      true,
		"message": "shutdown scheduled",
	})

	s.shutdownOnce.Do(func() {
		go func() {
			time.Sleep(100 * time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = s.httpServer.Shutdown(ctx)
		}()
	})
}

/* handleValidateAuto runs the synchronous auto workflow with structured validation. */
func (s *Server) handleValidateAuto(w http.ResponseWriter, r *http.Request) {
	if !s.allowMethod(w, r, http.MethodPost) || !s.requireLoopback(w, r) {
		return
	}
	if !s.tryAcquireExecution() {
		writeAPIError(w, http.StatusConflict, "BUSY", "another run is already active")
		return
	}
	defer s.releaseExecution()

	req, err := decodeValidateAutoRequest(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}

	opts, err := s.buildAutoOptions(req)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}

	result, err := s.service.RunAuto(opts)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "VALIDATION_FAILED", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, ValidateAutoSuccessResponse{
		OK:   true,
		Mode: "auto",
		Outputs: ValidateAutoOutput{
			SplitOutputDir: opts.SplitOutputDir,
			SuccessDir:     opts.SuccessDir,
			ErrorDir:       opts.ErrorDir,
			BatchDir:       firstNonEmpty(opts.BatchDir, opts.SuccessDir),
			BatchExportDir: opts.BatchExportDir,
		},
		Result: result,
	})
}

/* buildAutoOptions validates the request and maps it into service options. */
func (s *Server) buildAutoOptions(req ValidateAutoRequest) (service.AutoOptions, error) {
	mainInput := firstNonEmpty(strings.TrimSpace(req.MainInputCSV), strings.TrimSpace(req.InputCSV))
	if mainInput == "" {
		return service.AutoOptions{}, fmt.Errorf("main_input_csv or input_csv is required")
	}
	if err := validateAbsoluteCSVPath(mainInput, "main_input_csv"); err != nil {
		return service.AutoOptions{}, err
	}
	if err := validateAbsoluteJSONPath(req.SchemaPath, "schema_path"); err != nil {
		return service.AutoOptions{}, err
	}

	splitOutputDir := firstNonEmpty(req.SplitOutputDir, filepath.Join(filepath.Dir(mainInput), "split"))
	successDir := firstNonEmpty(req.SuccessDir, filepath.Join(filepath.Dir(mainInput), "success"))
	errorDir := firstNonEmpty(req.ErrorDir, filepath.Join(filepath.Dir(mainInput), "errors"))
	batchExportDir := firstNonEmpty(req.BatchExportDir, filepath.Join(filepath.Dir(mainInput), "batch_export"))
	batchDir := firstNonEmpty(req.BatchDir, successDir)

	for field, dir := range map[string]string{
		"split_output_dir": splitOutputDir,
		"success_dir":      successDir,
		"error_dir":        errorDir,
		"batch_dir":        batchDir,
		"batch_export_dir": batchExportDir,
	} {
		if err := validateAbsoluteDirPath(dir, field); err != nil {
			return service.AutoOptions{}, err
		}
	}

	threads := req.Threads
	if threads == 0 {
		threads = service.DefaultThreadCount()
	}
	if threads < 1 {
		return service.AutoOptions{}, fmt.Errorf("threads must be >= 1")
	}

	splitMaxOpen := req.SplitMaxOpen
	if splitMaxOpen == 0 {
		splitMaxOpen = 256
	}
	if splitMaxOpen < 1 {
		return service.AutoOptions{}, fmt.Errorf("split_max_open must be >= 1")
	}

	batchSize := req.BatchSize
	if batchSize == 0 {
		batchSize = 1000
	}
	if batchSize < 1 {
		return service.AutoOptions{}, fmt.Errorf("batch_size must be >= 1")
	}

	writeEmptyError := false
	if req.WriteEmptyError != nil {
		writeEmptyError = *req.WriteEmptyError
	}

	clearValidationCache := true
	if req.ClearValidationCache != nil {
		clearValidationCache = *req.ClearValidationCache
	}

	if samePath(batchDir, batchExportDir) {
		return service.AutoOptions{}, fmt.Errorf("batch_export_dir must differ from batch_dir")
	}

	return service.AutoOptions{
		MainInputCSV:         mainInput,
		SchemaPath:           req.SchemaPath,
		SplitOutputDir:       splitOutputDir,
		SplitPrimaryKey:      strings.TrimSpace(req.SplitPrimaryKey),
		SplitMaxOpen:         splitMaxOpen,
		SplitMissingFile:     firstNonEmpty(req.SplitMissingFile, "missing_keys.csv"),
		Threads:              threads,
		WriteEmptyError:      writeEmptyError,
		ClearValidationCache: clearValidationCache,
		SuccessDir:           successDir,
		ErrorDir:             errorDir,
		BatchDir:             batchDir,
		BatchExportDir:       batchExportDir,
		BatchSize:            batchSize,
	}, nil
}

/* withSecurityHeaders applies basic response hardening headers to every request. */
func (s *Server) withSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

/* allowMethod rejects requests that do not use the expected HTTP method. */
func (s *Server) allowMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	w.Header().Set("Allow", method)
	writeAPIError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", fmt.Sprintf("expected %s", method))
	return false
}

/* requireLoopback rejects any request whose remote address is not loopback. */
func (s *Server) requireLoopback(w http.ResponseWriter, r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		writeAPIError(w, http.StatusForbidden, "FORBIDDEN", "request origin is not loopback")
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		writeAPIError(w, http.StatusForbidden, "FORBIDDEN", "request origin is not loopback")
		return false
	}
	return true
}

/* tryAcquireExecution reserves the single active run slot for a request. */
func (s *Server) tryAcquireExecution() bool {
	select {
	case s.executionSem <- struct{}{}:
		return true
	default:
		return false
	}
}

/* releaseExecution frees the active run slot after a request finishes. */
func (s *Server) releaseExecution() {
	select {
	case <-s.executionSem:
	default:
	}
}

/* isBusy reports whether a run is currently executing. */
func (s *Server) isBusy() bool {
	return len(s.executionSem) > 0
}

/* decodeValidateAutoRequest decodes the JSON request body for auto mode. */
func decodeValidateAutoRequest(r *http.Request) (ValidateAutoRequest, error) {
	defer r.Body.Close()

	var req ValidateAutoRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return ValidateAutoRequest{}, err
	}
	if err := decoder.Decode(new(struct{})); err != io.EOF {
		return ValidateAutoRequest{}, fmt.Errorf("request body must contain a single JSON object")
	}
	return req, nil
}

/* writeJSON writes a JSON response with the provided status code. */
func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

/* writeAPIError writes a structured error response body. */
func writeAPIError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, ErrorResponse{
		OK:        false,
		ErrorCode: code,
		Message:   message,
	})
}

/* validateAbsoluteCSVPath validates a required absolute CSV file path. */
func validateAbsoluteCSVPath(path, field string) error {
	return validateAbsoluteFilePath(path, field, ".csv")
}

/* validateAbsoluteJSONPath validates a required absolute JSON file path. */
func validateAbsoluteJSONPath(path, field string) error {
	return validateAbsoluteFilePath(path, field, ".json")
}

/* validateAbsoluteFilePath validates a required absolute file path and extension. */
func validateAbsoluteFilePath(path, field, ext string) error {
	clean := strings.TrimSpace(path)
	if clean == "" {
		return fmt.Errorf("%s is required", field)
	}
	if !filepath.IsAbs(clean) {
		return fmt.Errorf("%s must be an absolute path", field)
	}
	if !strings.EqualFold(filepath.Ext(clean), ext) {
		return fmt.Errorf("%s must use %s extension", field, ext)
	}
	info, err := os.Stat(clean)
	if err != nil {
		return fmt.Errorf("%s does not exist", field)
	}
	if info.IsDir() {
		return fmt.Errorf("%s must be a file", field)
	}
	return nil
}

/* validateAbsoluteDirPath validates an absolute directory path and parent availability. */
func validateAbsoluteDirPath(path, field string) error {
	clean := strings.TrimSpace(path)
	if clean == "" {
		return fmt.Errorf("%s is required", field)
	}
	if !filepath.IsAbs(clean) {
		return fmt.Errorf("%s must be an absolute path", field)
	}
	parent := filepath.Dir(clean)
	info, err := os.Stat(clean)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%s must be a directory", field)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed checking %s", field)
	}
	parentInfo, parentErr := os.Stat(parent)
	if parentErr != nil || !parentInfo.IsDir() {
		return fmt.Errorf("%s parent directory must exist", field)
	}
	return nil
}

/* samePath compares absolute filesystem targets when possible. */
func samePath(left, right string) bool {
	leftAbs, leftErr := filepath.Abs(left)
	rightAbs, rightErr := filepath.Abs(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	return leftAbs == rightAbs
}

/* firstNonEmpty returns the first non-empty trimmed string from the provided list. */
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
