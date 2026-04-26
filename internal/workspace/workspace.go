package workspace

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const DefaultBaseDir = "/tmp/gvy-ui"

/* RunWorkspace defines the on-disk layout reserved for one upload-driven run. */
type RunWorkspace struct {
	RunID          string `json:"run_id"`
	RootDir        string `json:"root_dir"`
	InputCSVPath   string `json:"input_csv_path"`
	SchemaPath     string `json:"schema_path"`
	SplitDir       string `json:"split_dir"`
	SuccessDir     string `json:"success_dir"`
	ErrorDir       string `json:"error_dir"`
	BatchExportDir string `json:"batch_export_dir"`
	MetadataPath   string `json:"metadata_path,omitempty"`
}

/* New returns the default per-run workspace rooted under /tmp/gvy-ui. */
func New(runID string) (RunWorkspace, error) {
	return NewUnder(DefaultBaseDir, runID)
}

/* NewUnder builds a deterministic per-run workspace rooted under the provided base dir. */
func NewUnder(baseDir, runID string) (RunWorkspace, error) {
	normalizedRunID := normalizeRunID(runID)
	if normalizedRunID == "" {
		return RunWorkspace{}, fmt.Errorf("run id is required")
	}

	rootBase := strings.TrimSpace(baseDir)
	if rootBase == "" {
		rootBase = DefaultBaseDir
	}

	rootDir := filepath.Join(filepath.Clean(rootBase), normalizedRunID)
	return RunWorkspace{
		RunID:          normalizedRunID,
		RootDir:        rootDir,
		InputCSVPath:   filepath.Join(rootDir, "input.csv"),
		SchemaPath:     filepath.Join(rootDir, "schema.json"),
		SplitDir:       filepath.Join(rootDir, "split"),
		SuccessDir:     filepath.Join(rootDir, "success"),
		ErrorDir:       filepath.Join(rootDir, "errors"),
		BatchExportDir: filepath.Join(rootDir, "batch_export"),
		MetadataPath:   filepath.Join(rootDir, "run.json"),
	}, nil
}

/* Prepare ensures the workspace root and output directories exist. */
func (w RunWorkspace) Prepare() error {
	for _, dir := range []string{w.RootDir, w.SplitDir, w.SuccessDir, w.ErrorDir, w.BatchExportDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create workspace dir %q: %w", dir, err)
		}
	}
	return nil
}

func normalizeRunID(runID string) string {
	trimmed := strings.TrimSpace(runID)
	if trimmed == "" {
		return ""
	}

	var builder strings.Builder
	builder.Grow(len(trimmed))
	lastDash := false
	for _, r := range trimmed {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'):
			builder.WriteRune(r)
			lastDash = false
		case r == '-' || r == '_':
			builder.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				builder.WriteByte('-')
				lastDash = true
			}
		}
	}
	return strings.Trim(builder.String(), "-")
}
