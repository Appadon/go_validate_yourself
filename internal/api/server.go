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
	"strconv"
	"strings"
	"sync"
	"time"

	"go_validate_yourself/internal/progress"
	"go_validate_yourself/internal/runs"
	"go_validate_yourself/internal/service"
	"go_validate_yourself/internal/workspace"
)

const version = "v1"
const maxUploadBodyBytes = 64 << 20

/* Server provides a localhost-only HTTP API around the workflow service. */
type Server struct {
	host             string
	port             int
	service          service.Service
	runManager       *runs.Manager
	workspaceBaseDir string
	runAuto          func(context.Context, service.AutoOptions) (service.AutoResult, error)
	detectPrimaryKey func(string) (string, error)
	httpServer       *http.Server
	shutdownOnce     sync.Once
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

/* RunCreateResponse returns the snapshot for a newly created upload-driven run. */
type RunCreateResponse struct {
	OK  bool          `json:"ok"`
	Run runs.Snapshot `json:"run"`
}

/* RunSnapshotResponse returns the current snapshot for one run. */
type RunSnapshotResponse struct {
	OK  bool          `json:"ok"`
	Run runs.Snapshot `json:"run"`
}

/* RunResultResponse returns the terminal result or error for one run. */
type RunResultResponse struct {
	OK         bool       `json:"ok"`
	RunID      string     `json:"run_id"`
	State      runs.State `json:"state"`
	Result     any        `json:"result,omitempty"`
	FinalError string     `json:"final_error,omitempty"`
}

/* NewServer constructs a localhost-only API server instance. */
func NewServer(host string, port int, svc service.Service) *Server {
	server := &Server{
		host:             host,
		port:             port,
		service:          svc,
		runManager:       runs.NewManager(),
		workspaceBaseDir: workspace.DefaultBaseDir,
	}
	server.runAuto = server.service.RunAuto
	server.detectPrimaryKey = service.DetectPrimaryKey

	mux := http.NewServeMux()
	mux.HandleFunc("/health", server.handleHealth)
	mux.HandleFunc("/shutdown", server.handleShutdown)
	mux.HandleFunc("/run/validate-auto", server.handleValidateAuto)
	mux.HandleFunc("/api/runs", server.handleRuns)
	mux.HandleFunc("/api/runs/", server.handleRunByID)

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
		Busy:    s.runManager.HasActive(),
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

	runID := progress.NewRunID()
	if _, err := s.runManager.Create(runID, nil); err != nil {
		if errors.Is(err, runs.ErrActiveRunExists) {
			writeAPIError(w, http.StatusConflict, "BUSY", "another run is already active")
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "RUN_INIT_FAILED", err.Error())
		return
	}
	if _, err := s.runManager.Start(runID); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "RUN_START_FAILED", err.Error())
		return
	}

	opts.RunID = runID
	opts.Reporter = progress.Combine(opts.Reporter, s.runManager.Reporter(runID))

	result, err := s.service.RunAuto(r.Context(), opts)
	if err != nil {
		_, _ = s.runManager.Fail(runID, err)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			writeAPIError(w, http.StatusRequestTimeout, "REQUEST_CANCELED", err.Error())
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "VALIDATION_FAILED", err.Error())
		return
	}
	_, _ = s.runManager.Complete(runID, result)

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

/* handleRuns dispatches collection routes for upload-driven browser runs. */
func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	if !s.requireLoopback(w, r) {
		return
	}
	if r.URL.Path != "/api/runs" {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "route not found")
		return
	}
	if !s.allowMethod(w, r, http.MethodPost) {
		return
	}
	s.handleCreateRun(w, r)
}

/* handleRunByID dispatches run snapshot, result, and SSE routes. */
func (s *Server) handleRunByID(w http.ResponseWriter, r *http.Request) {
	if !s.requireLoopback(w, r) {
		return
	}
	runID, suffix, ok := parseRunRoute(r.URL.Path)
	if !ok {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "route not found")
		return
	}

	switch suffix {
	case "":
		if !s.allowMethod(w, r, http.MethodGet) {
			return
		}
		s.handleGetRun(w, r, runID)
	case "/events":
		if !s.allowMethod(w, r, http.MethodGet) {
			return
		}
		s.handleRunEvents(w, r, runID)
	case "/result":
		if !s.allowMethod(w, r, http.MethodGet) {
			return
		}
		s.handleGetRunResult(w, r, runID)
	default:
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "route not found")
	}
}

func (s *Server) handleCreateRun(w http.ResponseWriter, r *http.Request) {
	runID := progress.NewRunID()
	ws, err := workspace.NewUnder(s.workspaceBaseDir, runID)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "WORKSPACE_INIT_FAILED", err.Error())
		return
	}
	if err := ws.Prepare(); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "WORKSPACE_INIT_FAILED", err.Error())
		return
	}

	if err := s.decodeRunUploads(w, r, ws); err != nil {
		_ = os.RemoveAll(ws.RootDir)
		status, code := classifyUploadError(err)
		writeAPIError(w, status, code, err.Error())
		return
	}

	snapshot, err := s.runManager.Create(runID, &ws)
	if err != nil {
		_ = os.RemoveAll(ws.RootDir)
		if errors.Is(err, runs.ErrActiveRunExists) {
			writeAPIError(w, http.StatusConflict, "BUSY", "another run is already active")
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "RUN_INIT_FAILED", err.Error())
		return
	}
	snapshot, err = s.runManager.Start(runID)
	if err != nil {
		_ = os.RemoveAll(ws.RootDir)
		writeAPIError(w, http.StatusInternalServerError, "RUN_START_FAILED", err.Error())
		return
	}

	go s.executeUploadRun(runID, ws)

	w.Header().Set("Location", "/api/runs/"+runID)
	writeJSON(w, http.StatusCreated, RunCreateResponse{
		OK:  true,
		Run: snapshot,
	})
}

func (s *Server) handleGetRun(w http.ResponseWriter, _ *http.Request, runID string) {
	snapshot, ok := s.runManager.Snapshot(runID)
	if !ok {
		writeAPIError(w, http.StatusNotFound, "RUN_NOT_FOUND", "run not found")
		return
	}
	writeJSON(w, http.StatusOK, RunSnapshotResponse{
		OK:  true,
		Run: snapshot,
	})
}

func (s *Server) handleGetRunResult(w http.ResponseWriter, _ *http.Request, runID string) {
	snapshot, ok := s.runManager.Snapshot(runID)
	if !ok {
		writeAPIError(w, http.StatusNotFound, "RUN_NOT_FOUND", "run not found")
		return
	}
	if snapshot.State != runs.StateCompleted && snapshot.State != runs.StateFailed {
		writeAPIError(w, http.StatusConflict, "RUN_NOT_FINISHED", "run has not finished")
		return
	}
	writeJSON(w, http.StatusOK, RunResultResponse{
		OK:         true,
		RunID:      snapshot.RunID,
		State:      snapshot.State,
		Result:     snapshot.FinalResult,
		FinalError: snapshot.FinalError,
	})
}

func (s *Server) handleRunEvents(w http.ResponseWriter, r *http.Request, runID string) {
	snapshot, events, cancel, err := s.runManager.Subscribe(runID)
	if err != nil {
		if errors.Is(err, runs.ErrRunNotFound) {
			writeAPIError(w, http.StatusNotFound, "RUN_NOT_FOUND", "run not found")
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "SSE_INIT_FAILED", err.Error())
		return
	}
	defer cancel()

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeAPIError(w, http.StatusInternalServerError, "SSE_UNSUPPORTED", "streaming not supported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	if _, err := io.WriteString(w, ": connected\n\n"); err != nil {
		return
	}
	flusher.Flush()

	for idx, event := range snapshot.Events {
		if err := writeSSEEvent(w, "progress", strconv.Itoa(idx), event); err != nil {
			return
		}
		flusher.Flush()
	}
	if snapshot.State == runs.StateCompleted || snapshot.State == runs.StateFailed {
		return
	}

	pingTicker := time.NewTicker(25 * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-pingTicker.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case event, ok := <-events:
			if !ok {
				return
			}
			if err := writeSSEEvent(w, "progress", "", event); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) decodeRunUploads(w http.ResponseWriter, r *http.Request, ws workspace.RunWorkspace) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadBodyBytes)
	if err := r.ParseMultipartForm(maxUploadBodyBytes); err != nil {
		return fmt.Errorf("invalid multipart request")
	}
	defer func() {
		if r.MultipartForm != nil {
			_ = r.MultipartForm.RemoveAll()
		}
	}()

	if err := writeMultipartFile(r, "csv", ".csv", ws.InputCSVPath); err != nil {
		return err
	}
	if err := writeMultipartFile(r, "schema", ".json", ws.SchemaPath); err != nil {
		return err
	}
	return nil
}

func (s *Server) executeUploadRun(runID string, ws workspace.RunWorkspace) {
	reporter := s.runManager.Reporter(runID)
	primaryKey, err := s.detectPrimaryKey(ws.InputCSVPath)
	if err != nil {
		reporter.Report(progress.Event{
			RunID:   runID,
			Time:    time.Now().UTC(),
			Phase:   progress.PhaseRun,
			Type:    progress.TypeFailed,
			Message: err.Error(),
		})
		_, _ = s.runManager.Fail(runID, err)
		return
	}

	result, err := s.runAuto(context.Background(), service.AutoOptions{
		MainInputCSV:         ws.InputCSVPath,
		SchemaPath:           ws.SchemaPath,
		SplitOutputDir:       ws.SplitDir,
		SplitPrimaryKey:      primaryKey,
		SplitMaxOpen:         256,
		SplitMissingFile:     "missing_keys.csv",
		Threads:              service.DefaultThreadCount(),
		WriteEmptyError:      false,
		ClearValidationCache: true,
		SuccessDir:           ws.SuccessDir,
		ErrorDir:             ws.ErrorDir,
		BatchDir:             ws.SuccessDir,
		BatchExportDir:       ws.BatchExportDir,
		BatchSize:            1000,
		RunID:                runID,
		Reporter:             reporter,
	})
	if err != nil {
		_, _ = s.runManager.Fail(runID, err)
		return
	}
	_, _ = s.runManager.Complete(runID, result)
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

func parseRunRoute(path string) (string, string, bool) {
	trimmed := strings.TrimPrefix(path, "/api/runs/")
	if trimmed == path || trimmed == "" {
		return "", "", false
	}
	parts := strings.Split(trimmed, "/")
	if parts[0] == "" {
		return "", "", false
	}
	runID := parts[0]
	if len(parts) == 1 {
		return runID, "", true
	}
	if len(parts) == 2 && parts[1] != "" {
		return runID, "/" + parts[1], true
	}
	return "", "", false
}

/* writeJSON writes a JSON response with the provided status code. */
func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeSSEEvent(w io.Writer, eventType, id string, payload any) error {
	if id != "" {
		if _, err := fmt.Fprintf(w, "id: %s\n", id); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "event: %s\n", eventType); err != nil {
		return err
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		return err
	}
	return nil
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

func writeMultipartFile(r *http.Request, field, ext, destination string) error {
	file, header, err := r.FormFile(field)
	if err != nil {
		if errors.Is(err, http.ErrMissingFile) {
			switch field {
			case "csv":
				return fmt.Errorf("missing csv upload")
			case "schema":
				return fmt.Errorf("missing schema upload")
			}
			return fmt.Errorf("missing %s upload", field)
		}
		return fmt.Errorf("invalid multipart request")
	}
	defer file.Close()

	filename := strings.TrimSpace(header.Filename)
	if !strings.EqualFold(filepath.Ext(filename), ext) {
		switch field {
		case "csv":
			return fmt.Errorf("csv upload must use .csv extension")
		case "schema":
			return fmt.Errorf("schema upload must use .json extension")
		}
		return fmt.Errorf("%s upload must use %s extension", field, ext)
	}

	out, err := os.Create(destination)
	if err != nil {
		return fmt.Errorf("create upload file %q: %w", destination, err)
	}
	defer out.Close()

	if _, err := io.Copy(out, file); err != nil {
		return fmt.Errorf("write upload file %q: %w", destination, err)
	}
	return nil
}

func classifyUploadError(err error) (int, string) {
	message := err.Error()
	switch {
	case message == "invalid multipart request":
		return http.StatusBadRequest, "INVALID_MULTIPART"
	case message == "missing csv upload":
		return http.StatusBadRequest, "MISSING_CSV_UPLOAD"
	case message == "missing schema upload":
		return http.StatusBadRequest, "MISSING_SCHEMA_UPLOAD"
	case message == "csv upload must use .csv extension":
		return http.StatusBadRequest, "INVALID_CSV_EXTENSION"
	case message == "schema upload must use .json extension":
		return http.StatusBadRequest, "INVALID_SCHEMA_EXTENSION"
	default:
		return http.StatusInternalServerError, "UPLOAD_WRITE_FAILED"
	}
}
