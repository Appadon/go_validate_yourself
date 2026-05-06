package workspace

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
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
	InputSHA256    string `json:"input_sha256,omitempty"`
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

/* NewForInput builds a per-file workspace rooted under runs/<snake_file_stem>. */
func NewForInput(baseDir, runID, inputPath string) (RunWorkspace, error) {
	normalizedRunID := normalizeRunID(runID)
	if normalizedRunID == "" {
		return RunWorkspace{}, fmt.Errorf("run id is required")
	}
	slug := SnakeCaseStem(inputPath)
	if slug == "" {
		return RunWorkspace{}, fmt.Errorf("input file name is required")
	}

	rootBase := strings.TrimSpace(baseDir)
	if rootBase == "" {
		rootBase = DefaultBaseDir
	}
	rootDir := filepath.Join(filepath.Clean(rootBase), slug)
	return RunWorkspace{
		RunID:          normalizedRunID,
		RootDir:        rootDir,
		InputCSVPath:   strings.TrimSpace(inputPath),
		SchemaPath:     filepath.Join(rootDir, "schema.json"),
		SplitDir:       filepath.Join(rootDir, "split"),
		SuccessDir:     filepath.Join(rootDir, "success"),
		ErrorDir:       filepath.Join(rootDir, "errors"),
		BatchExportDir: filepath.Join(rootDir, "batch_export"),
		MetadataPath:   filepath.Join(rootDir, "run.json"),
	}, nil
}

/* SnakeCaseStem returns a filesystem-friendly snake_case file stem. */
func SnakeCaseStem(path string) string {
	name := strings.TrimSpace(filepath.Base(path))
	if name == "." || name == string(filepath.Separator) {
		return ""
	}
	stem := strings.TrimSuffix(name, filepath.Ext(name))
	stem = strings.TrimSpace(stem)
	if stem == "" {
		return ""
	}

	var builder strings.Builder
	builder.Grow(len(stem))
	lastUnderscore := false
	for _, r := range stem {
		switch {
		case r >= 'A' && r <= 'Z':
			builder.WriteRune(r + ('a' - 'A'))
			lastUnderscore = false
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			builder.WriteRune(r)
			lastUnderscore = false
		default:
			if !lastUnderscore {
				builder.WriteByte('_')
				lastUnderscore = true
			}
		}
	}
	return strings.Trim(builder.String(), "_")
}

/* HashFile returns the SHA-256 hex digest for the provided file. */
func HashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

/* WithInputHash returns a copy of the workspace with InputSHA256 populated. */
func (w RunWorkspace) WithInputHash() (RunWorkspace, error) {
	hash, err := HashFile(w.InputCSVPath)
	if err != nil {
		return RunWorkspace{}, fmt.Errorf("hash input file %q: %w", w.InputCSVPath, err)
	}
	w.InputSHA256 = hash
	return w, nil
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
