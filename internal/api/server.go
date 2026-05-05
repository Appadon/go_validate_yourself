package api

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	gvyconfig "go_validate_yourself/internal/config"
	"go_validate_yourself/internal/progress"
	"go_validate_yourself/internal/runs"
	"go_validate_yourself/internal/schemaeditor"
	"go_validate_yourself/internal/schemainfer"
	"go_validate_yourself/internal/service"
	"go_validate_yourself/internal/workspace"
	"go_validate_yourself/web"
)

const version = "v2"
const maxUploadBodyBytes = 64 << 20
const defaultErrorReportLimit = 50
const maxErrorReportLimit = 250

var quotedErrorValuePattern = regexp.MustCompile(`"[^"]*"`)

/* Server provides a localhost-only HTTP API around the workflow service. */
type Server struct {
	host             string
	port             int
	service          service.Service
	runManager       *runs.Manager
	workingRoot      string
	workingRootReal  string
	workspaceBaseDir string
	templates        *template.Template
	runAuto          func(context.Context, service.AutoOptions) (service.AutoResult, error)
	runPipeline      func(context.Context, service.PipelineOptions) (service.PipelineResult, error)
	detectPrimaryKey func(string) (string, error)
	httpServer       *http.Server
	shutdownOnce     sync.Once
}

/* HealthResponse reports server status and execution availability. */
type HealthResponse struct {
	Status         string     `json:"status"`
	Busy           bool       `json:"busy"`
	Version        string     `json:"version"`
	WorkingRoot    string     `json:"working_root,omitempty"`
	LatestRunID    string     `json:"latest_run_id,omitempty"`
	LatestRunState runs.State `json:"latest_run_state,omitempty"`
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
	OK             bool                     `json:"ok"`
	Mode           string                   `json:"mode"`
	Outputs        ValidateAutoOutput       `json:"outputs"`
	Result         service.AutoResult       `json:"result"`
	ResolvedConfig gvyconfig.ResolvedConfig `json:"resolved_config"`
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

/* ConfigDefaultsResponse returns the canonical built-in config defaults. */
type ConfigDefaultsResponse struct {
	OK       bool             `json:"ok"`
	Defaults gvyconfig.Config `json:"defaults"`
}

/* ConfigResolveResponse returns a config after server-side normalization. */
type ConfigResolveResponse struct {
	OK             bool                     `json:"ok"`
	ResolvedConfig gvyconfig.ResolvedConfig `json:"resolved_config"`
}

/* ConfigRunResponse returns metadata and the resolved config for a config-first run. */
type ConfigRunResponse struct {
	OK             bool                     `json:"ok"`
	Run            runs.Snapshot            `json:"run"`
	ResolvedConfig gvyconfig.ResolvedConfig `json:"resolved_config"`
	Result         service.PipelineResult   `json:"result,omitempty"`
}

/* ConfigRunResult is stored on run snapshots for config-first executions. */
type ConfigRunResult struct {
	Mode           string                   `json:"mode"`
	ResolvedConfig gvyconfig.ResolvedConfig `json:"resolved_config"`
	Result         service.PipelineResult   `json:"result"`
}

/* FileSelectionRunRequest defines JSON inputs for server-side file selection runs. */
type FileSelectionRunRequest struct {
	CSVPath    string `json:"csv_path"`
	SchemaPath string `json:"schema_path"`
}

/* FileListEntry exposes one selectable file under the server working root. */
type FileListEntry struct {
	Name         string `json:"name"`
	RelativePath string `json:"relative_path"`
	IsDir        bool   `json:"is_dir"`
	SizeBytes    int64  `json:"size_bytes"`
}

/* FileListResponse returns eligible files scoped to the server working root. */
type FileListResponse struct {
	OK          bool            `json:"ok"`
	Kind        string          `json:"kind"`
	WorkingRoot string          `json:"working_root"`
	CurrentPath string          `json:"current_path"`
	ParentPath  string          `json:"parent_path,omitempty"`
	Entries     []FileListEntry `json:"entries"`
}

/* SchemaDocumentResponse returns an editable schema JSON document. */
type SchemaDocumentResponse struct {
	OK           bool                  `json:"ok"`
	Path         string                `json:"path"`
	RelativePath string                `json:"relative_path"`
	Schema       schemaeditor.Document `json:"schema"`
}

/* SchemaSaveRequest defines a schema JSON document to validate and write. */
type SchemaSaveRequest struct {
	Path   string                `json:"path"`
	Schema schemaeditor.Document `json:"schema"`
}

/* SchemaSaveResponse returns the saved schema path and normalized document. */
type SchemaSaveResponse struct {
	OK           bool                  `json:"ok"`
	Path         string                `json:"path"`
	RelativePath string                `json:"relative_path"`
	Schema       schemaeditor.Document `json:"schema"`
	Message      string                `json:"message"`
}

/* SchemaInferRequest defines inputs accepted by POST /api/schema/infer. */
type SchemaInferRequest struct {
	CSVPath            string `json:"csv_path"`
	SampleSize         int    `json:"sample_size"`
	Strategy           string `json:"strategy"`
	KeepSamples        *bool  `json:"keep_samples"`
	WriteSampleParquet *bool  `json:"write_sample_parquet"`
	SampleOutputPath   string `json:"sample_output_path"`
}

/* SchemaInferResponse returns an inferred schema plus optional sample parquet metadata. */
type SchemaInferResponse struct {
	OK                        bool               `json:"ok"`
	CSVPath                   string             `json:"csv_path"`
	CSVRelativePath           string             `json:"csv_relative_path"`
	SampleParquetPath         string             `json:"sample_parquet_path,omitempty"`
	SampleParquetRelativePath string             `json:"sample_parquet_relative_path,omitempty"`
	Inference                 schemainfer.Result `json:"inference"`
}

/* ErrorReportResponse summarizes validation error CSVs without returning full files. */
type ErrorReportResponse struct {
	OK           bool                 `json:"ok"`
	ErrorDir     string               `json:"error_dir"`
	RelativePath string               `json:"relative_path"`
	FileCount    int                  `json:"file_count"`
	ScannedFiles int                  `json:"scanned_files"`
	ScannedRows  int                  `json:"scanned_rows"`
	MatchedRows  int                  `json:"matched_rows"`
	Limit        int                  `json:"limit"`
	Offset       int                  `json:"offset"`
	Query        string               `json:"query,omitempty"`
	Field        string               `json:"field,omitempty"`
	File         string               `json:"file,omitempty"`
	Fields       []ErrorReportBucket  `json:"fields"`
	Messages     []ErrorReportMessage `json:"messages"`
	Files        []ErrorReportBucket  `json:"files"`
	Samples      []ErrorReportSample  `json:"samples"`
}

/* ErrorReportBucket is a counted field or file group. */
type ErrorReportBucket struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

/* ErrorReportMessage is a counted field/message pair. */
type ErrorReportMessage struct {
	Field   string `json:"field"`
	Message string `json:"message"`
	Count   int    `json:"count"`
}

/* ErrorReportSample is one bounded row sample from an error CSV. */
type ErrorReportSample struct {
	File        string              `json:"file"`
	RowNumber   string              `json:"row_number"`
	Errors      string              `json:"errors"`
	ErrorFields []string            `json:"error_fields"`
	Columns     []ErrorReportColumn `json:"columns"`
	Values      map[string]string   `json:"values"`
}

/* ErrorReportColumn preserves one original row column for ordered sample display. */
type ErrorReportColumn struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	Errored bool   `json:"errored"`
}

type uiPageData struct {
	Title             string
	Version           string
	ServerBusy        bool
	WorkingRoot       string
	LatestRunID       string
	LatestRunState    runs.State
	SchemaEditorEmbed bool
	BootstrapJSON     template.JS
}

type uiBootstrap struct {
	Server      HealthResponse `json:"server"`
	LatestRunID string         `json:"latest_run_id,omitempty"`
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
	workingRoot := mustAbsDir(".")
	workingRootReal := resolveRealPath(workingRoot)
	if workingRootReal == "" {
		workingRootReal = workingRoot
	}
	workspaceBaseDir := filepath.Join(workingRoot, ".gvy", "runs")
	templates := template.Must(template.ParseFS(web.Files, "templates/*.html"))
	staticFiles := mustSubFS(web.Files, "static")

	server := &Server{
		host:             host,
		port:             port,
		service:          svc,
		runManager:       runs.NewManager(),
		workingRoot:      workingRoot,
		workingRootReal:  workingRootReal,
		workspaceBaseDir: workspaceBaseDir,
		templates:        templates,
	}
	server.runAuto = server.service.RunAuto
	server.runPipeline = server.service.RunPipeline
	server.detectPrimaryKey = service.DetectPrimaryKey

	mux := http.NewServeMux()
	mux.HandleFunc("/", server.handleUI)
	mux.HandleFunc("/schema-infer", server.handleSchemaInferUI)
	mux.HandleFunc("/schema-editor", server.handleSchemaEditorUI)
	mux.HandleFunc("/schema-workbench", server.handleSchemaWorkbenchUI)
	mux.HandleFunc("/error-explorer", server.handleErrorExplorerUI)
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFiles))))
	mux.HandleFunc("/health", server.handleHealth)
	mux.HandleFunc("/shutdown", server.handleShutdown)
	mux.HandleFunc("/run/validate-auto", server.handleValidateAuto)
	mux.HandleFunc("/api/config/defaults", server.handleConfigDefaults)
	mux.HandleFunc("/api/config/resolve", server.handleConfigResolve)
	mux.HandleFunc("/api/files", server.handleFileList)
	mux.HandleFunc("/api/errors/report", server.handleErrorReport)
	mux.HandleFunc("/api/schema/infer", server.handleSchemaInfer)
	mux.HandleFunc("/api/schema", server.handleSchemaDocument)
	mux.HandleFunc("/api/runs", server.handleRuns)
	mux.HandleFunc("/api/runs/config", server.handleConfigRun)
	mux.HandleFunc("/api/runs/", server.handleRunByID)

	server.httpServer = &http.Server{
		Addr:              fmt.Sprintf("%s:%d", host, port),
		Handler:           server.withSecurityHeaders(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return server
}

/* handleErrorExplorerUI renders the validation error explorer proof of concept. */
func (s *Server) handleErrorExplorerUI(w http.ResponseWriter, r *http.Request) {
	if !s.requireLoopback(w, r) {
		return
	}
	if r.URL.Path != "/error-explorer" {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "route not found")
		return
	}
	if !s.allowMethod(w, r, http.MethodGet) {
		return
	}

	health := s.currentHealth()
	bootstrap, err := json.Marshal(uiBootstrap{
		Server:      health,
		LatestRunID: health.LatestRunID,
	})
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "UI_BOOTSTRAP_FAILED", err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.templates.ExecuteTemplate(w, "error_explorer.html", uiPageData{
		Title:             "GVY Error Explorer",
		Version:           version,
		ServerBusy:        health.Busy,
		WorkingRoot:       s.workingRoot,
		LatestRunID:       health.LatestRunID,
		LatestRunState:    health.LatestRunState,
		SchemaEditorEmbed: strings.TrimSpace(r.URL.Query().Get("embed")) == "1",
		BootstrapJSON:     template.JS(bootstrap),
	}); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "UI_RENDER_FAILED", err.Error())
		return
	}
}

/* handleSchemaInferUI renders the standalone schema inference proof of concept. */
func (s *Server) handleSchemaInferUI(w http.ResponseWriter, r *http.Request) {
	if !s.requireLoopback(w, r) {
		return
	}
	if r.URL.Path != "/schema-infer" {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "route not found")
		return
	}
	if !s.allowMethod(w, r, http.MethodGet) {
		return
	}

	health := s.currentHealth()
	bootstrap, err := json.Marshal(uiBootstrap{
		Server:      health,
		LatestRunID: health.LatestRunID,
	})
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "UI_BOOTSTRAP_FAILED", err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.templates.ExecuteTemplate(w, "schema_infer.html", uiPageData{
		Title:             "GVY Schema Inference",
		Version:           version,
		ServerBusy:        health.Busy,
		WorkingRoot:       s.workingRoot,
		LatestRunID:       health.LatestRunID,
		LatestRunState:    health.LatestRunState,
		SchemaEditorEmbed: strings.TrimSpace(r.URL.Query().Get("embed")) == "1",
		BootstrapJSON:     template.JS(bootstrap),
	}); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "UI_RENDER_FAILED", err.Error())
		return
	}
}

/* handleSchemaWorkbenchUI renders the combined schema editor workbench scaffold. */
func (s *Server) handleSchemaWorkbenchUI(w http.ResponseWriter, r *http.Request) {
	if !s.requireLoopback(w, r) {
		return
	}
	if r.URL.Path != "/schema-workbench" {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "route not found")
		return
	}
	if !s.allowMethod(w, r, http.MethodGet) {
		return
	}

	health := s.currentHealth()
	bootstrap, err := json.Marshal(uiBootstrap{
		Server:      health,
		LatestRunID: health.LatestRunID,
	})
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "UI_BOOTSTRAP_FAILED", err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.templates.ExecuteTemplate(w, "schema_workbench.html", uiPageData{
		Title:             "GVY Schema Editor",
		Version:           version,
		ServerBusy:        health.Busy,
		WorkingRoot:       s.workingRoot,
		LatestRunID:       health.LatestRunID,
		LatestRunState:    health.LatestRunState,
		SchemaEditorEmbed: strings.TrimSpace(r.URL.Query().Get("embed")) == "1",
		BootstrapJSON:     template.JS(bootstrap),
	}); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "UI_RENDER_FAILED", err.Error())
		return
	}
}

/* handleSchemaEditorUI renders the standalone schema editor proof of concept. */
func (s *Server) handleSchemaEditorUI(w http.ResponseWriter, r *http.Request) {
	if !s.requireLoopback(w, r) {
		return
	}
	if r.URL.Path != "/schema-editor" {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "route not found")
		return
	}
	if !s.allowMethod(w, r, http.MethodGet) {
		return
	}

	health := s.currentHealth()
	bootstrap, err := json.Marshal(uiBootstrap{
		Server:      health,
		LatestRunID: health.LatestRunID,
	})
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "UI_BOOTSTRAP_FAILED", err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.templates.ExecuteTemplate(w, "schema_editor.html", uiPageData{
		Title:             "GVY Schema Editor",
		Version:           version,
		ServerBusy:        health.Busy,
		WorkingRoot:       s.workingRoot,
		LatestRunID:       health.LatestRunID,
		LatestRunState:    health.LatestRunState,
		SchemaEditorEmbed: strings.TrimSpace(r.URL.Query().Get("embed")) == "1",
		BootstrapJSON:     template.JS(bootstrap),
	}); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "UI_RENDER_FAILED", err.Error())
		return
	}
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
	writeJSON(w, http.StatusOK, s.currentHealth())
}

/* handleUI renders the Stage 5 browser UI. */
func (s *Server) handleUI(w http.ResponseWriter, r *http.Request) {
	if !s.requireLoopback(w, r) {
		return
	}
	if r.URL.Path != "/" {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "route not found")
		return
	}
	if !s.allowMethod(w, r, http.MethodGet) {
		return
	}

	health := s.currentHealth()
	bootstrap, err := json.Marshal(uiBootstrap{
		Server:      health,
		LatestRunID: health.LatestRunID,
	})
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "UI_BOOTSTRAP_FAILED", err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.templates.ExecuteTemplate(w, "index.html", uiPageData{
		Title:          "GVY Validation Console",
		Version:        version,
		ServerBusy:     health.Busy,
		WorkingRoot:    s.workingRoot,
		LatestRunID:    health.LatestRunID,
		LatestRunState: health.LatestRunState,
		BootstrapJSON:  template.JS(bootstrap),
	}); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "UI_RENDER_FAILED", err.Error())
		return
	}
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

	resolved, opts, err := s.buildLegacyAutoPipeline(req)
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

	pipelineResult, err := s.runPipeline(r.Context(), opts)
	if err != nil {
		_, _ = s.runManager.Fail(runID, err)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			writeAPIError(w, http.StatusRequestTimeout, "REQUEST_CANCELED", err.Error())
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "VALIDATION_FAILED", err.Error())
		return
	}
	result := autoResultFromPipeline(resolved, pipelineResult)
	_, _ = s.runManager.Complete(runID, ConfigRunResult{
		Mode:           "auto",
		ResolvedConfig: resolved,
		Result:         pipelineResult,
	})

	writeJSON(w, http.StatusOK, ValidateAutoSuccessResponse{
		OK:   true,
		Mode: "auto",
		Outputs: ValidateAutoOutput{
			SplitOutputDir: resolved.Outputs.SplitDir,
			SuccessDir:     resolved.Outputs.SuccessDir,
			ErrorDir:       resolved.Outputs.ErrorDir,
			BatchDir:       firstNonEmpty(resolved.Batch.InputDir, resolved.Outputs.SuccessDir),
			BatchExportDir: resolved.Outputs.BatchExportDir,
		},
		Result:         result,
		ResolvedConfig: resolved,
	})
}

/* handleConfigDefaults returns canonical configuration defaults for clients. */
func (s *Server) handleConfigDefaults(w http.ResponseWriter, r *http.Request) {
	if !s.allowMethod(w, r, http.MethodGet) || !s.requireLoopback(w, r) {
		return
	}
	if r.URL.Path != "/api/config/defaults" {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "route not found")
		return
	}
	writeJSON(w, http.StatusOK, ConfigDefaultsResponse{
		OK:       true,
		Defaults: gvyconfig.Defaults(),
	})
}

/* handleConfigResolve strictly decodes and resolves a config without executing it. */
func (s *Server) handleConfigResolve(w http.ResponseWriter, r *http.Request) {
	if !s.allowMethod(w, r, http.MethodPost) || !s.requireLoopback(w, r) {
		return
	}
	if r.URL.Path != "/api/config/resolve" {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "route not found")
		return
	}
	cfg, err := decodeConfigRequest(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}
	resolved, err := gvyconfig.Normalize(cfg, gvyconfig.NormalizeOptions{})
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_CONFIG", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, ConfigResolveResponse{
		OK:             true,
		ResolvedConfig: resolved,
	})
}

/* handleConfigRun executes a strict config-first run through the pipeline orchestrator. */
func (s *Server) handleConfigRun(w http.ResponseWriter, r *http.Request) {
	if !s.allowMethod(w, r, http.MethodPost) || !s.requireLoopback(w, r) {
		return
	}
	if r.URL.Path != "/api/runs/config" {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "route not found")
		return
	}
	cfg, err := decodeConfigRequest(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}
	resolved, err := gvyconfig.Normalize(cfg, gvyconfig.NormalizeOptions{})
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_CONFIG", err.Error())
		return
	}
	if err := validateResolvedExecutionInputs(resolved); err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_CONFIG", err.Error())
		return
	}

	runID := progress.NewRunID()
	snapshot, err := s.runManager.Create(runID, nil)
	if err != nil {
		if errors.Is(err, runs.ErrActiveRunExists) {
			writeAPIError(w, http.StatusConflict, "BUSY", "another run is already active")
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "RUN_INIT_FAILED", err.Error())
		return
	}
	snapshot, err = s.runManager.Start(runID)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "RUN_START_FAILED", err.Error())
		return
	}

	opts := pipelineOptionsFromResolved(resolved)
	opts.RunID = runID
	opts.Reporter = progress.Combine(opts.Reporter, s.runManager.Reporter(runID))
	result, err := s.runPipeline(r.Context(), opts)
	if err != nil {
		_, _ = s.runManager.Fail(runID, err)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			writeAPIError(w, http.StatusRequestTimeout, "REQUEST_CANCELED", err.Error())
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "RUN_FAILED", err.Error())
		return
	}
	finalResult := ConfigRunResult{
		Mode:           resolvedPipelineMode(resolved),
		ResolvedConfig: resolved,
		Result:         result,
	}
	snapshot, _ = s.runManager.Complete(runID, finalResult)

	writeJSON(w, http.StatusOK, ConfigRunResponse{
		OK:             true,
		Run:            snapshot,
		ResolvedConfig: resolved,
		Result:         result,
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

/* handleFileList returns eligible selectable files under the server working root. */
func (s *Server) handleFileList(w http.ResponseWriter, r *http.Request) {
	if !s.requireLoopback(w, r) {
		return
	}
	if r.URL.Path != "/api/files" {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "route not found")
		return
	}
	if !s.allowMethod(w, r, http.MethodGet) {
		return
	}

	kind := strings.TrimSpace(r.URL.Query().Get("kind"))
	ext := extForKind(kind)
	if ext == "" {
		writeAPIError(w, http.StatusBadRequest, "INVALID_KIND", "kind must be csv or schema")
		return
	}

	currentPath, parentPath, entries, err := s.listWorkingRootEntries(strings.TrimSpace(r.URL.Query().Get("path")), ext)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "FILE_LIST_FAILED", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, FileListResponse{
		OK:          true,
		Kind:        kind,
		WorkingRoot: s.workingRoot,
		CurrentPath: currentPath,
		ParentPath:  parentPath,
		Entries:     entries,
	})
}

/* handleErrorReport summarizes validation error CSVs under a working-root directory. */
func (s *Server) handleErrorReport(w http.ResponseWriter, r *http.Request) {
	if !s.requireLoopback(w, r) {
		return
	}
	if r.URL.Path != "/api/errors/report" {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "route not found")
		return
	}
	if !s.allowMethod(w, r, http.MethodGet) {
		return
	}

	errorDir, relativePath, err := s.resolveSelectedDirectory(r.URL.Query().Get("path"), "path")
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_ERROR_DIR", err.Error())
		return
	}
	report, err := buildErrorReport(errorDir, relativePath, ErrorReportOptions{
		Query:  strings.TrimSpace(r.URL.Query().Get("q")),
		Field:  strings.TrimSpace(r.URL.Query().Get("field")),
		File:   strings.TrimSpace(r.URL.Query().Get("file")),
		Limit:  boundedQueryInt(r, "limit", defaultErrorReportLimit, 1, maxErrorReportLimit),
		Offset: boundedQueryInt(r, "offset", 0, 0, 1_000_000),
	})
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "ERROR_REPORT_FAILED", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, report)
}

/* handleSchemaDocument loads or saves a working-root-scoped schema document. */
func (s *Server) handleSchemaDocument(w http.ResponseWriter, r *http.Request) {
	if !s.requireLoopback(w, r) {
		return
	}
	if r.URL.Path != "/api/schema" {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "route not found")
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleSchemaRead(w, r)
	case http.MethodPut:
		s.handleSchemaSave(w, r)
	default:
		w.Header().Set("Allow", strings.Join([]string{http.MethodGet, http.MethodPut}, ", "))
		writeAPIError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "expected GET or PUT")
	}
}

/* handleSchemaRead returns a validated schema JSON document for editing. */
func (s *Server) handleSchemaRead(w http.ResponseWriter, r *http.Request) {
	path, err := s.resolveSelectedFile(r.URL.Query().Get("path"), ".json", "path")
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_SCHEMA_PATH", err.Error())
		return
	}
	schema, err := schemaeditor.Load(path)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_SCHEMA", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, SchemaDocumentResponse{
		OK:           true,
		Path:         path,
		RelativePath: s.relativeWorkingPath(path),
		Schema:       schema,
	})
}

/* handleSchemaSave validates and writes an editable schema JSON document. */
func (s *Server) handleSchemaSave(w http.ResponseWriter, r *http.Request) {
	req, err := decodeSchemaSaveRequest(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}
	path, err := s.resolveSchemaSavePath(req.Path)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_SCHEMA_PATH", err.Error())
		return
	}
	if err := schemaeditor.Save(path, req.Schema); err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_SCHEMA", err.Error())
		return
	}
	schema, err := schemaeditor.Load(path)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "SCHEMA_RELOAD_FAILED", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, SchemaSaveResponse{
		OK:           true,
		Path:         path,
		RelativePath: s.relativeWorkingPath(path),
		Schema:       schema,
		Message:      "schema saved",
	})
}

/* handleSchemaInfer samples a working-root-scoped CSV and returns a draft schema. */
func (s *Server) handleSchemaInfer(w http.ResponseWriter, r *http.Request) {
	if !s.requireLoopback(w, r) {
		return
	}
	if r.URL.Path != "/api/schema/infer" {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "route not found")
		return
	}
	if !s.allowMethod(w, r, http.MethodPost) {
		return
	}

	req, err := decodeSchemaInferRequest(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}
	csvPath, err := s.resolveSelectedFile(req.CSVPath, ".csv", "csv_path")
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_CSV_PATH", err.Error())
		return
	}

	keepSamples := true
	if req.KeepSamples != nil {
		keepSamples = *req.KeepSamples
	}
	writeSampleParquet := false
	if req.WriteSampleParquet != nil {
		writeSampleParquet = *req.WriteSampleParquet
		if writeSampleParquet {
			keepSamples = true
		}
	}

	result, err := schemainfer.Infer(r.Context(), csvPath, schemainfer.Options{
		SampleSize:  req.SampleSize,
		Strategy:    strings.TrimSpace(req.Strategy),
		KeepSamples: keepSamples,
	})
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "SCHEMA_INFERENCE_FAILED", err.Error())
		return
	}

	sampleParquetPath := ""
	if writeSampleParquet {
		sampleParquetPath, err = s.resolveSchemaSampleParquetPath(req.SampleOutputPath, csvPath)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "INVALID_SAMPLE_OUTPUT_PATH", err.Error())
			return
		}
		if err := schemainfer.WriteSamplesParquet(sampleParquetPath, result); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "SAMPLE_PARQUET_FAILED", err.Error())
			return
		}
	}

	response := SchemaInferResponse{
		OK:              true,
		CSVPath:         csvPath,
		CSVRelativePath: s.relativeWorkingPath(csvPath),
		Inference:       result,
	}
	if sampleParquetPath != "" {
		response.SampleParquetPath = sampleParquetPath
		response.SampleParquetRelativePath = s.relativeWorkingPath(sampleParquetPath)
	}
	writeJSON(w, http.StatusOK, response)
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

/* handleCreateRun accepts upload-driven browser requests and starts a background run. */
func (s *Server) handleCreateRun(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/json") {
		s.handleCreateRunFromSelection(w, r)
		return
	}

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

/* handleCreateRunFromSelection resolves working-root-scoped file selections and starts a background run. */
func (s *Server) handleCreateRunFromSelection(w http.ResponseWriter, r *http.Request) {
	req, err := decodeFileSelectionRunRequest(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}

	inputCSVPath, err := s.resolveSelectedFile(req.CSVPath, ".csv", "csv_path")
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	schemaPath, err := s.resolveSelectedFile(req.SchemaPath, ".json", "schema_path")
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}

	runID := progress.NewRunID()
	ws, err := workspace.NewUnder(s.workspaceBaseDir, runID)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "WORKSPACE_INIT_FAILED", err.Error())
		return
	}
	ws.InputCSVPath = inputCSVPath
	ws.SchemaPath = schemaPath
	if err := ws.Prepare(); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "WORKSPACE_INIT_FAILED", err.Error())
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

	go s.executeWorkspaceRun(runID, ws)

	w.Header().Set("Location", "/api/runs/"+runID)
	writeJSON(w, http.StatusCreated, RunCreateResponse{
		OK:  true,
		Run: snapshot,
	})
}

/* handleGetRun returns the latest in-memory snapshot for the requested run id. */
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

/* handleGetRunResult returns the stored terminal result or final error for a finished run. */
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

/* handleRunEvents replays retained events and then streams live progress over SSE. */
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

/* decodeRunUploads validates and stores multipart browser uploads inside the run workspace. */
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

/* executeUploadRun maps a prepared workspace into auto-run options and completes the run asynchronously. */
func (s *Server) executeUploadRun(runID string, ws workspace.RunWorkspace) {
	s.executeWorkspaceRun(runID, ws)
}

/* executeWorkspaceRun maps a prepared workspace into auto-run options and completes the run asynchronously. */
func (s *Server) executeWorkspaceRun(runID string, ws workspace.RunWorkspace) {
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

/* buildAutoOptions is a compatibility-only adapter for legacy tests and SDK callers. */
func (s *Server) buildAutoOptions(req ValidateAutoRequest) (service.AutoOptions, error) {
	resolved, err := s.resolveLegacyAutoConfig(req)
	if err != nil {
		return service.AutoOptions{}, err
	}
	return autoOptionsFromResolved(resolved), nil
}

/* buildLegacyAutoPipeline converts /run/validate-auto requests into resolved pipeline options. */
func (s *Server) buildLegacyAutoPipeline(req ValidateAutoRequest) (gvyconfig.ResolvedConfig, service.PipelineOptions, error) {
	resolved, err := s.resolveLegacyAutoConfig(req)
	if err != nil {
		return gvyconfig.ResolvedConfig{}, service.PipelineOptions{}, err
	}
	return resolved, pipelineOptionsFromResolved(resolved), nil
}

/* resolveLegacyAutoConfig preserves edge compatibility while using the shared config resolver. */
func (s *Server) resolveLegacyAutoConfig(req ValidateAutoRequest) (gvyconfig.ResolvedConfig, error) {
	mainInput := firstNonEmpty(strings.TrimSpace(req.MainInputCSV), strings.TrimSpace(req.InputCSV))
	if mainInput == "" {
		return gvyconfig.ResolvedConfig{}, fmt.Errorf("main_input_csv or input_csv is required")
	}
	if err := validateAbsoluteCSVPath(mainInput, "main_input_csv"); err != nil {
		return gvyconfig.ResolvedConfig{}, err
	}
	if err := validateAbsoluteJSONPath(req.SchemaPath, "schema_path"); err != nil {
		return gvyconfig.ResolvedConfig{}, err
	}

	defaults := gvyconfig.Defaults()
	inputDir := filepath.Dir(mainInput)
	writeEmptyError := false
	if req.WriteEmptyError != nil {
		writeEmptyError = *req.WriteEmptyError
	}

	clearValidationCache := true
	if req.ClearValidationCache != nil {
		clearValidationCache = *req.ClearValidationCache
	}

	cfg := gvyconfig.Config{
		Mode: "auto",
		Inputs: gvyconfig.InputsConfig{
			MainCSV: mainInput,
			Schema:  strings.TrimSpace(req.SchemaPath),
		},
		Outputs: gvyconfig.OutputsConfig{
			SplitDir:       firstNonEmpty(req.SplitOutputDir, filepath.Join(inputDir, defaults.Outputs.SplitDir)),
			SuccessDir:     firstNonEmpty(req.SuccessDir, filepath.Join(inputDir, defaults.Outputs.SuccessDir)),
			ErrorDir:       firstNonEmpty(req.ErrorDir, filepath.Join(inputDir, defaults.Outputs.ErrorDir)),
			BatchExportDir: firstNonEmpty(req.BatchExportDir, filepath.Join(inputDir, defaults.Outputs.BatchExportDir)),
		},
		Split: gvyconfig.SplitConfig{
			PrimaryKey:      strings.TrimSpace(req.SplitPrimaryKey),
			MaxOpenWriters:  req.SplitMaxOpen,
			MissingKeysFile: strings.TrimSpace(req.SplitMissingFile),
			ReuseCache:      true,
		},
		Validation: gvyconfig.ValidationConfig{
			WriteEmptyError: writeEmptyError,
			ClearOutputs:    clearValidationCache,
		},
		Batch: gvyconfig.BatchConfig{
			InputDir: strings.TrimSpace(req.BatchDir),
			Size:     req.BatchSize,
		},
		Runtime: gvyconfig.RuntimeConfig{
			Workers: req.Threads,
		},
	}

	resolved, err := gvyconfig.Normalize(cfg, gvyconfig.NormalizeOptions{})
	if err != nil {
		return gvyconfig.ResolvedConfig{}, err
	}
	if err := validateLegacyResolvedPaths(resolved); err != nil {
		return gvyconfig.ResolvedConfig{}, err
	}
	return resolved, nil
}

/* pipelineOptionsFromResolved maps canonical resolved config into service execution options. */
func pipelineOptionsFromResolved(resolved gvyconfig.ResolvedConfig) service.PipelineOptions {
	fullAutoPipeline := configPhasesEqual(resolved.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseSplit, gvyconfig.PhaseValidate, gvyconfig.PhaseBatch})
	batchClearOutput := resolved.Batch.ClearOutput
	if fullAutoPipeline {
		batchClearOutput = false
	}
	return service.PipelineOptions{
		Phases:       servicePipelinePhases(resolved.Plan.Phases),
		ResumePolicy: service.PipelineResumePolicy(resolved.Plan.ResumePolicy),
		Split: service.SplitOptions{
			InputPath:       resolved.Inputs.MainCSV,
			OutputDir:       resolved.Outputs.SplitDir,
			PrimaryKey:      resolved.Split.PrimaryKey,
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
		Mode:                      resolvedPipelineMode(resolved),
	}
}

/* autoOptionsFromResolved keeps the legacy options shape available at the API edge only. */
func autoOptionsFromResolved(resolved gvyconfig.ResolvedConfig) service.AutoOptions {
	return service.AutoOptions{
		MainInputCSV:         resolved.Inputs.MainCSV,
		SchemaPath:           resolved.Inputs.Schema,
		SplitOutputDir:       resolved.Outputs.SplitDir,
		SplitPrimaryKey:      resolved.Split.PrimaryKey,
		SplitMaxOpen:         resolved.Split.MaxOpenWriters,
		SplitMissingFile:     resolved.Split.MissingKeysFile,
		Threads:              resolved.EffectiveWorkers,
		WriteEmptyError:      resolved.Validation.WriteEmptyError,
		ClearValidationCache: resolved.Validation.ClearOutputs,
		SuccessDir:           resolved.Outputs.SuccessDir,
		ErrorDir:             resolved.Outputs.ErrorDir,
		BatchDir:             resolved.Batch.InputDir,
		BatchExportDir:       resolved.Outputs.BatchExportDir,
		BatchSize:            resolved.Batch.Size,
	}
}

/* autoResultFromPipeline preserves the legacy /run/validate-auto response shape. */
func autoResultFromPipeline(resolved gvyconfig.ResolvedConfig, result service.PipelineResult) service.AutoResult {
	out := service.AutoResult{
		MainInputCSV:    resolved.Inputs.MainCSV,
		SchemaPath:      resolved.Inputs.Schema,
		SplitPrimaryKey: result.SplitPrimaryKey,
		SplitReused:     result.SplitReused,
		SplitSummary:    result.SplitSummary,
		BatchSummary:    result.BatchSummary,
	}
	if result.ValidationDir != nil {
		out.Validation = *result.ValidationDir
	}
	return out
}

func servicePipelinePhases(phases []gvyconfig.Phase) []service.PipelinePhase {
	out := make([]service.PipelinePhase, 0, len(phases))
	for _, phase := range phases {
		out = append(out, service.PipelinePhase(phase))
	}
	return out
}

func resolvedPipelineMode(resolved gvyconfig.ResolvedConfig) string {
	switch {
	case configPhasesEqual(resolved.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseSplit, gvyconfig.PhaseValidate, gvyconfig.PhaseBatch}):
		return "auto"
	case configPhasesEqual(resolved.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseSplit}):
		return "split"
	case configPhasesEqual(resolved.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseValidate}):
		if strings.TrimSpace(resolved.Inputs.ValidateCSV) != "" {
			return "validate-file"
		}
		return "validate-dir"
	case configPhasesEqual(resolved.Plan.Phases, []gvyconfig.Phase{gvyconfig.PhaseBatch}):
		return "batch"
	default:
		return "pipeline"
	}
}

func configPhasesEqual(actual, expected []gvyconfig.Phase) bool {
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

/* validateLegacyResolvedPaths preserves absolute-path requirements on /run/validate-auto. */
func validateLegacyResolvedPaths(resolved gvyconfig.ResolvedConfig) error {
	if err := validateAbsoluteCSVPath(resolved.Inputs.MainCSV, "main_input_csv"); err != nil {
		return err
	}
	if err := validateAbsoluteJSONPath(resolved.Inputs.Schema, "schema_path"); err != nil {
		return err
	}
	for field, dir := range map[string]string{
		"split_output_dir": resolved.Outputs.SplitDir,
		"success_dir":      resolved.Outputs.SuccessDir,
		"error_dir":        resolved.Outputs.ErrorDir,
		"batch_dir":        resolved.Batch.InputDir,
		"batch_export_dir": resolved.Outputs.BatchExportDir,
	} {
		if err := validateAbsoluteDirPath(dir, field); err != nil {
			return err
		}
	}
	if samePath(resolved.Batch.InputDir, resolved.Outputs.BatchExportDir) {
		return fmt.Errorf("batch_export_dir must differ from batch_dir")
	}
	return nil
}

/* validateResolvedExecutionInputs checks only source inputs required before execution starts. */
func validateResolvedExecutionInputs(resolved gvyconfig.ResolvedConfig) error {
	for _, phase := range resolved.Plan.Phases {
		switch phase {
		case gvyconfig.PhaseSplit:
			if err := requireExistingFileWithExtension(resolved.Inputs.MainCSV, "inputs.main_csv", ".csv"); err != nil {
				return err
			}
		case gvyconfig.PhaseValidate:
			if err := requireExistingFileWithExtension(resolved.Inputs.Schema, "inputs.schema", ".json"); err != nil {
				return err
			}
			if strings.TrimSpace(resolved.Inputs.ValidateCSV) != "" {
				if err := requireExistingFileWithExtension(resolved.Inputs.ValidateCSV, "inputs.validate_csv", ".csv"); err != nil {
					return err
				}
			} else if !configPhaseBefore(resolved.Plan.Phases, gvyconfig.PhaseSplit, gvyconfig.PhaseValidate) {
				if err := requireExistingDir(resolved.Inputs.ValidateDir, "inputs.validate_dir"); err != nil {
					return err
				}
			}
		case gvyconfig.PhaseBatch:
			if !configPhaseBefore(resolved.Plan.Phases, gvyconfig.PhaseValidate, gvyconfig.PhaseBatch) {
				if err := requireExistingDir(resolved.Batch.InputDir, "batch.input_dir"); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func configPhaseBefore(phases []gvyconfig.Phase, before, after gvyconfig.Phase) bool {
	beforeIndex := -1
	afterIndex := -1
	for i, phase := range phases {
		if phase == before {
			beforeIndex = i
		}
		if phase == after {
			afterIndex = i
		}
	}
	return beforeIndex >= 0 && afterIndex >= 0 && beforeIndex < afterIndex
}

func requireExistingFileWithExtension(path, field, ext string) error {
	clean := strings.TrimSpace(path)
	if clean == "" {
		return fmt.Errorf("%s is required", field)
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

func requireExistingDir(path, field string) error {
	clean := strings.TrimSpace(path)
	if clean == "" {
		return fmt.Errorf("%s is required", field)
	}
	info, err := os.Stat(clean)
	if err != nil {
		return fmt.Errorf("%s does not exist", field)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s must be a directory", field)
	}
	return nil
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

/* decodeConfigRequest decodes a strict GVY config JSON object. */
func decodeConfigRequest(r *http.Request) (gvyconfig.Config, error) {
	defer r.Body.Close()

	var cfg gvyconfig.Config
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return gvyconfig.Config{}, err
	}
	if err := decoder.Decode(new(struct{})); err != io.EOF {
		return gvyconfig.Config{}, fmt.Errorf("request body must contain a single JSON object")
	}
	return cfg, nil
}

/* decodeFileSelectionRunRequest decodes the JSON request body for a UI-selected run. */
func decodeFileSelectionRunRequest(r *http.Request) (FileSelectionRunRequest, error) {
	defer r.Body.Close()

	var req FileSelectionRunRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return FileSelectionRunRequest{}, err
	}
	if err := decoder.Decode(new(struct{})); err != io.EOF {
		return FileSelectionRunRequest{}, fmt.Errorf("request body must contain a single JSON object")
	}
	return req, nil
}

/* decodeSchemaSaveRequest decodes the JSON request body for schema saves. */
func decodeSchemaSaveRequest(r *http.Request) (SchemaSaveRequest, error) {
	defer r.Body.Close()

	var req SchemaSaveRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return SchemaSaveRequest{}, err
	}
	if err := decoder.Decode(new(struct{})); err != io.EOF {
		return SchemaSaveRequest{}, fmt.Errorf("request body must contain a single JSON object")
	}
	return req, nil
}

/* decodeSchemaInferRequest decodes the JSON request body for schema inference. */
func decodeSchemaInferRequest(r *http.Request) (SchemaInferRequest, error) {
	defer r.Body.Close()

	var req SchemaInferRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return SchemaInferRequest{}, err
	}
	if err := decoder.Decode(new(struct{})); err != io.EOF {
		return SchemaInferRequest{}, fmt.Errorf("request body must contain a single JSON object")
	}
	return req, nil
}

/* parseRunRoute extracts a run id and supported sub-route suffix from /api/runs paths. */
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

/* writeSSEEvent serializes one SSE frame with a JSON payload body. */
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

func boundedQueryInt(r *http.Request, name string, fallback, minValue, maxValue int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
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

type ErrorReportOptions struct {
	Query  string
	Field  string
	File   string
	Limit  int
	Offset int
}

type parsedErrorMessage struct {
	Field   string
	Message string
}

/* buildErrorReport streams error CSVs once and returns bounded aggregates and samples. */
func buildErrorReport(errorDir, relativePath string, opts ErrorReportOptions) (ErrorReportResponse, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultErrorReportLimit
	}
	if limit > maxErrorReportLimit {
		limit = maxErrorReportLimit
	}
	offset := opts.Offset
	if offset < 0 {
		offset = 0
	}

	items, err := os.ReadDir(errorDir)
	if err != nil {
		return ErrorReportResponse{}, err
	}

	files := make([]fs.DirEntry, 0, len(items))
	fileFilter := strings.ToLower(strings.TrimSpace(opts.File))
	for _, item := range items {
		if item.IsDir() {
			continue
		}
		name := item.Name()
		if !strings.EqualFold(filepath.Ext(name), ".csv") || !strings.HasSuffix(strings.ToLower(name), "_error.csv") {
			continue
		}
		if fileFilter != "" && !strings.Contains(strings.ToLower(name), fileFilter) {
			continue
		}
		files = append(files, item)
	}
	sort.Slice(files, func(i, j int) bool {
		return strings.ToLower(files[i].Name()) < strings.ToLower(files[j].Name())
	})

	fieldCounts := make(map[string]int)
	messageCounts := make(map[string]ErrorReportMessage)
	fileCounts := make(map[string]int)
	samples := make([]ErrorReportSample, 0, limit)
	query := strings.ToLower(strings.TrimSpace(opts.Query))
	fieldFilter := strings.ToLower(strings.TrimSpace(opts.Field))
	matchedRows := 0
	scannedRows := 0
	scannedFiles := 0

	for _, item := range files {
		name := item.Name()
		path := filepath.Join(errorDir, name)
		file, err := os.Open(path)
		if err != nil {
			return ErrorReportResponse{}, fmt.Errorf("%s: %w", name, err)
		}
		fileMatched, fileScanned, err := scanErrorCSV(file, name, query, fieldFilter, offset, limit, &matchedRows, fieldCounts, messageCounts, &samples)
		closeErr := file.Close()
		if err != nil {
			return ErrorReportResponse{}, err
		}
		if closeErr != nil {
			return ErrorReportResponse{}, closeErr
		}
		if fileScanned > 0 {
			scannedFiles++
			scannedRows += fileScanned
		}
		if fileMatched > 0 {
			fileCounts[name] = fileCounts[name] + fileMatched
		}
	}

	return ErrorReportResponse{
		OK:           true,
		ErrorDir:     errorDir,
		RelativePath: filepath.ToSlash(relativePath),
		FileCount:    len(files),
		ScannedFiles: scannedFiles,
		ScannedRows:  scannedRows,
		MatchedRows:  matchedRows,
		Limit:        limit,
		Offset:       offset,
		Query:        opts.Query,
		Field:        opts.Field,
		File:         opts.File,
		Fields:       topBuckets(fieldCounts, 20),
		Messages:     topMessages(messageCounts, 30),
		Files:        topBuckets(fileCounts, 30),
		Samples:      samples,
	}, nil
}

func scanErrorCSV(file *os.File, name, query, fieldFilter string, offset, limit int, matchedRows *int, fieldCounts map[string]int, messageCounts map[string]ErrorReportMessage, samples *[]ErrorReportSample) (int, int, error) {
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if errors.Is(err, io.EOF) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("%s: read header: %w", name, err)
	}

	rowIndex := indexOfHeader(header, "__row_number")
	errorIndex := indexOfHeader(header, "__errors")
	if rowIndex < 0 || errorIndex < 0 {
		return 0, 0, fmt.Errorf("%s: expected __row_number and __errors columns", name)
	}

	fileMatched := 0
	fileScanned := 0
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, 0, fmt.Errorf("%s: read row: %w", name, err)
		}
		fileScanned++

		errorsText := csvCell(record, errorIndex)
		parsed := parseErrorMessages(errorsText)
		if fieldFilter != "" && !parsedErrorsContainField(parsed, fieldFilter) {
			continue
		}
		if query != "" && !errorRecordContains(record, name, query) {
			continue
		}

		*matchedRows = *matchedRows + 1
		fileMatched++
		for _, parsedError := range parsed {
			field := parsedError.Field
			if field == "" {
				field = "Unspecified"
			}
			fieldCounts[field]++
			messagePattern := normalizeErrorPattern(parsedError.Message)
			key := field + "\x00" + messagePattern
			current := messageCounts[key]
			if current.Field == "" {
				current.Field = field
				current.Message = messagePattern
			}
			current.Count++
			messageCounts[key] = current
		}

		if *matchedRows <= offset || len(*samples) >= limit {
			continue
		}
		*samples = append(*samples, ErrorReportSample{
			File:        name,
			RowNumber:   csvCell(record, rowIndex),
			Errors:      truncateText(errorsText, 500),
			ErrorFields: errorFieldNames(parsed),
			Columns:     sampleRecordColumns(header, record, parsed),
			Values:      sampleRecordValues(header, record),
		})
	}

	return fileMatched, fileScanned, nil
}

func parseErrorMessages(value string) []parsedErrorMessage {
	parts := strings.Split(value, " | ")
	parsed := make([]parsedErrorMessage, 0, len(parts))
	for _, part := range parts {
		clean := strings.TrimSpace(part)
		if clean == "" {
			continue
		}
		field, message, ok := strings.Cut(clean, ":")
		if !ok {
			parsed = append(parsed, parsedErrorMessage{Field: "Unspecified", Message: clean})
			continue
		}
		parsed = append(parsed, parsedErrorMessage{
			Field:   strings.TrimSpace(field),
			Message: strings.TrimSpace(message),
		})
	}
	return parsed
}

func parsedErrorsContainField(parsed []parsedErrorMessage, fieldFilter string) bool {
	for _, item := range parsed {
		if strings.Contains(strings.ToLower(item.Field), fieldFilter) {
			return true
		}
	}
	return false
}

func normalizeErrorPattern(message string) string {
	clean := strings.TrimSpace(message)
	if clean == "" {
		return "Validation error"
	}
	return quotedErrorValuePattern.ReplaceAllString(clean, "<value>")
}

func errorRecordContains(record []string, fileName, query string) bool {
	if strings.Contains(strings.ToLower(fileName), query) {
		return true
	}
	for _, cell := range record {
		if strings.Contains(strings.ToLower(cell), query) {
			return true
		}
	}
	return false
}

func errorFieldNames(parsed []parsedErrorMessage) []string {
	seen := make(map[string]struct{})
	fields := make([]string, 0, len(parsed))
	for _, item := range parsed {
		field := strings.TrimSpace(item.Field)
		if field == "" || field == "Unspecified" {
			continue
		}
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		fields = append(fields, field)
	}
	return fields
}

func sampleRecordColumns(header, record []string, parsed []parsedErrorMessage) []ErrorReportColumn {
	errorFields := make(map[string]struct{})
	for _, field := range errorFieldNames(parsed) {
		errorFields[field] = struct{}{}
	}
	columns := make([]ErrorReportColumn, 0, len(header))
	for index, name := range header {
		if name == "__row_number" || name == "__errors" {
			continue
		}
		_, errored := errorFields[name]
		columns = append(columns, ErrorReportColumn{
			Name:    name,
			Value:   truncateText(csvCell(record, index), 300),
			Errored: errored,
		})
	}
	return columns
}

func sampleRecordValues(header, record []string) map[string]string {
	values := make(map[string]string)
	for index, name := range header {
		if name == "__row_number" || name == "__errors" {
			continue
		}
		values[name] = truncateText(csvCell(record, index), 160)
	}
	return values
}

func topBuckets(counts map[string]int, limit int) []ErrorReportBucket {
	buckets := make([]ErrorReportBucket, 0, len(counts))
	for name, count := range counts {
		buckets = append(buckets, ErrorReportBucket{Name: name, Count: count})
	}
	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i].Count != buckets[j].Count {
			return buckets[i].Count > buckets[j].Count
		}
		return strings.ToLower(buckets[i].Name) < strings.ToLower(buckets[j].Name)
	})
	if len(buckets) > limit {
		return buckets[:limit]
	}
	return buckets
}

func topMessages(counts map[string]ErrorReportMessage, limit int) []ErrorReportMessage {
	messages := make([]ErrorReportMessage, 0, len(counts))
	for _, message := range counts {
		messages = append(messages, message)
	}
	sort.Slice(messages, func(i, j int) bool {
		if messages[i].Count != messages[j].Count {
			return messages[i].Count > messages[j].Count
		}
		if strings.ToLower(messages[i].Field) != strings.ToLower(messages[j].Field) {
			return strings.ToLower(messages[i].Field) < strings.ToLower(messages[j].Field)
		}
		return strings.ToLower(messages[i].Message) < strings.ToLower(messages[j].Message)
	})
	if len(messages) > limit {
		return messages[:limit]
	}
	return messages
}

func indexOfHeader(header []string, name string) int {
	for index, value := range header {
		if value == name {
			return index
		}
	}
	return -1
}

func csvCell(record []string, index int) string {
	if index < 0 || index >= len(record) {
		return ""
	}
	return record[index]
}

func truncateText(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	if limit <= 1 {
		return value[:limit]
	}
	return value[:limit-1] + "..."
}

func (s *Server) currentHealth() HealthResponse {
	response := HealthResponse{
		Status:      "ok",
		Busy:        s.runManager.HasActive(),
		Version:     version,
		WorkingRoot: s.workingRoot,
	}
	if latest, ok := s.runManager.LatestSnapshot(); ok {
		response.LatestRunID = latest.RunID
		response.LatestRunState = latest.State
	}
	return response
}

func (s *Server) listWorkingRootEntries(rawPath, ext string) (string, string, []FileListEntry, error) {
	currentDir, currentRelativePath, err := s.resolveBrowseDirectory(rawPath)
	if err != nil {
		return "", "", nil, err
	}

	items, err := os.ReadDir(currentDir)
	if err != nil {
		return "", "", nil, err
	}

	entries := make([]FileListEntry, 0, len(items))
	for _, item := range items {
		absolutePath := filepath.Join(currentDir, item.Name())
		resolved := resolveRealPath(absolutePath)
		if resolved == "" || !isWithinRoot(s.workingRootReal, resolved) {
			continue
		}

		relativePath, err := filepath.Rel(s.workingRoot, absolutePath)
		if err != nil {
			return "", "", nil, err
		}
		if item.IsDir() {
			if s.shouldHideBrowsePath(absolutePath) {
				continue
			}
			entries = append(entries, FileListEntry{
				Name:         item.Name(),
				RelativePath: filepath.ToSlash(relativePath),
				IsDir:        true,
			})
			continue
		}

		if !strings.EqualFold(filepath.Ext(item.Name()), ext) {
			continue
		}
		info, err := item.Info()
		if err != nil {
			return "", "", nil, err
		}
		entries = append(entries, FileListEntry{
			Name:         item.Name(),
			RelativePath: filepath.ToSlash(relativePath),
			IsDir:        false,
			SizeBytes:    info.Size(),
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDir != entries[j].IsDir {
			return entries[i].IsDir
		}
		return strings.ToLower(entries[i].Name) < strings.ToLower(entries[j].Name)
	})

	parentPath := ""
	if currentRelativePath != "." {
		parentPath = filepath.ToSlash(filepath.Dir(currentRelativePath))
		if parentPath == "." {
			parentPath = ""
		}
	}

	displayPath := ""
	if currentRelativePath != "." {
		displayPath = filepath.ToSlash(currentRelativePath)
	}
	return displayPath, parentPath, entries, nil
}

func (s *Server) shouldHideBrowsePath(absolutePath string) bool {
	internalRoot := filepath.Dir(s.workspaceBaseDir)
	return samePath(absolutePath, s.workspaceBaseDir) || samePath(absolutePath, internalRoot)
}

func (s *Server) resolveBrowseDirectory(rawPath string) (string, string, error) {
	clean := strings.TrimSpace(rawPath)
	if clean == "" {
		return s.workingRoot, ".", nil
	}

	candidate := filepath.Join(s.workingRoot, filepath.FromSlash(clean))
	absolutePath, err := filepath.Abs(candidate)
	if err != nil {
		return "", "", fmt.Errorf("path must resolve to a valid directory")
	}
	info, err := os.Stat(absolutePath)
	if err != nil {
		return "", "", fmt.Errorf("path does not exist")
	}
	if !info.IsDir() {
		return "", "", fmt.Errorf("path must be a directory")
	}
	resolved := resolveRealPath(absolutePath)
	if resolved == "" || !isWithinRoot(s.workingRootReal, resolved) {
		return "", "", fmt.Errorf("path must stay within the server working directory")
	}
	relativePath, err := filepath.Rel(s.workingRoot, absolutePath)
	if err != nil {
		return "", "", err
	}
	return absolutePath, relativePath, nil
}

func (s *Server) resolveSelectedFile(rawPath, expectedExt, field string) (string, error) {
	clean := strings.TrimSpace(rawPath)
	if clean == "" {
		return "", fmt.Errorf("%s is required", field)
	}

	candidate := clean
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(s.workingRoot, filepath.FromSlash(clean))
	}
	absolutePath, err := filepath.Abs(candidate)
	if err != nil {
		return "", fmt.Errorf("%s must resolve to a valid path", field)
	}
	if !strings.EqualFold(filepath.Ext(absolutePath), expectedExt) {
		return "", fmt.Errorf("%s must use %s extension", field, expectedExt)
	}

	info, err := os.Stat(absolutePath)
	if err != nil {
		return "", fmt.Errorf("%s does not exist", field)
	}
	if info.IsDir() {
		return "", fmt.Errorf("%s must be a file", field)
	}

	resolved := resolveRealPath(absolutePath)
	if resolved == "" || !isWithinRoot(s.workingRootReal, resolved) {
		return "", fmt.Errorf("%s must stay within the server working directory", field)
	}
	return absolutePath, nil
}

func (s *Server) resolveSelectedDirectory(rawPath, field string) (string, string, error) {
	clean := strings.TrimSpace(rawPath)
	if clean == "" {
		return "", "", fmt.Errorf("%s is required", field)
	}

	candidate := clean
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(s.workingRoot, filepath.FromSlash(clean))
	}
	absolutePath, err := filepath.Abs(candidate)
	if err != nil {
		return "", "", fmt.Errorf("%s must resolve to a valid directory", field)
	}
	info, err := os.Stat(absolutePath)
	if err != nil {
		return "", "", fmt.Errorf("%s does not exist", field)
	}
	if !info.IsDir() {
		return "", "", fmt.Errorf("%s must be a directory", field)
	}

	resolved := resolveRealPath(absolutePath)
	if resolved == "" || !isWithinRoot(s.workingRootReal, resolved) {
		return "", "", fmt.Errorf("%s must stay within the server working directory", field)
	}
	relativePath, err := filepath.Rel(s.workingRoot, absolutePath)
	if err != nil {
		return "", "", err
	}
	return absolutePath, relativePath, nil
}

func (s *Server) resolveSchemaSavePath(rawPath string) (string, error) {
	clean := strings.TrimSpace(rawPath)
	if clean == "" {
		return "", fmt.Errorf("path is required")
	}

	candidate := clean
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(s.workingRoot, filepath.FromSlash(clean))
	}
	absolutePath, err := filepath.Abs(candidate)
	if err != nil {
		return "", fmt.Errorf("path must resolve to a valid file")
	}
	if !strings.EqualFold(filepath.Ext(absolutePath), ".json") {
		return "", fmt.Errorf("path must use .json extension")
	}

	if info, err := os.Stat(absolutePath); err == nil {
		if info.IsDir() {
			return "", fmt.Errorf("path must be a file")
		}
		resolved := resolveRealPath(absolutePath)
		if resolved == "" || !isWithinRoot(s.workingRootReal, resolved) {
			return "", fmt.Errorf("path must stay within the server working directory")
		}
		return absolutePath, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("failed checking path")
	}

	parent := filepath.Dir(absolutePath)
	parentInfo, err := os.Stat(parent)
	if err != nil {
		return "", fmt.Errorf("parent directory does not exist")
	}
	if !parentInfo.IsDir() {
		return "", fmt.Errorf("parent path must be a directory")
	}
	parentResolved := resolveRealPath(parent)
	if parentResolved == "" || !isWithinRoot(s.workingRootReal, parentResolved) {
		return "", fmt.Errorf("path must stay within the server working directory")
	}
	return absolutePath, nil
}

func (s *Server) resolveSchemaSampleParquetPath(rawPath, csvPath string) (string, error) {
	clean := strings.TrimSpace(rawPath)
	if clean == "" {
		base := strings.TrimSuffix(filepath.Base(csvPath), filepath.Ext(csvPath))
		clean = filepath.Join(".gvy", "schema_samples", base+".sample.parquet")
	}

	candidate := clean
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(s.workingRoot, filepath.FromSlash(clean))
	}
	absolutePath, err := filepath.Abs(candidate)
	if err != nil {
		return "", fmt.Errorf("sample_output_path must resolve to a valid file")
	}
	if !strings.EqualFold(filepath.Ext(absolutePath), ".parquet") {
		return "", fmt.Errorf("sample_output_path must use .parquet extension")
	}

	if info, err := os.Stat(absolutePath); err == nil {
		if info.IsDir() {
			return "", fmt.Errorf("sample_output_path must be a file")
		}
		resolved := resolveRealPath(absolutePath)
		if resolved == "" || !isWithinRoot(s.workingRootReal, resolved) {
			return "", fmt.Errorf("sample_output_path must stay within the server working directory")
		}
		return absolutePath, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("failed checking sample_output_path")
	}

	parent := filepath.Dir(absolutePath)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", fmt.Errorf("create sample output directory: %w", err)
	}
	parentResolved := resolveRealPath(parent)
	if parentResolved == "" || !isWithinRoot(s.workingRootReal, parentResolved) {
		return "", fmt.Errorf("sample_output_path must stay within the server working directory")
	}
	return absolutePath, nil
}

func (s *Server) relativeWorkingPath(path string) string {
	relativePath, err := filepath.Rel(s.workingRoot, path)
	if err != nil {
		return path
	}
	return filepath.ToSlash(relativePath)
}

func extForKind(kind string) string {
	switch kind {
	case "csv":
		return ".csv"
	case "schema":
		return ".json"
	default:
		return ""
	}
}

func isWithinRoot(root, candidate string) bool {
	relativePath, err := filepath.Rel(root, candidate)
	if err != nil {
		return false
	}
	return relativePath == "." || (relativePath != ".." && !strings.HasPrefix(relativePath, ".."+string(os.PathSeparator)))
}

func resolveRealPath(path string) string {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return ""
	}
	absolutePath, err := filepath.Abs(resolved)
	if err != nil {
		return ""
	}
	return absolutePath
}

func mustAbsDir(path string) string {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		panic(err)
	}
	return absolutePath
}

func mustSubFS(source fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(source, dir)
	if err != nil {
		panic(err)
	}
	return sub
}

/* writeMultipartFile copies one uploaded multipart file into the workspace after extension checks. */
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

/* classifyUploadError maps upload parsing and validation failures into stable API error codes. */
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
