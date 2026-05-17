package splitcsv

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"go_validate_yourself/internal/inputrows"
	"go_validate_yourself/internal/progress"
)

const cacheMetadataFileName = ".gvy-split-cache.json"

/* Config defines behavior for splitting one CSV or Parquet file into many files by key. */
type Config struct {
	InputPath       string
	OutputDir       string
	PrimaryKey      string
	MaxOpenWriters  int
	MissingKeysFile string
	Progress        progress.Emitter
}

/* Summary captures split run metrics. */
type Summary struct {
	TotalRows      int
	SplitRows      int
	MissingKeyRows int
	OutputFiles    int
}

/* CacheMetadata captures the input signature for a reusable split directory. */
type CacheMetadata struct {
	InputPath       string    `json:"input_path"`
	InputHash       string    `json:"input_hash"`
	PrimaryKey      string    `json:"primary_key"`
	MissingKeysFile string    `json:"missing_keys_file"`
	CreatedAt       time.Time `json:"created_at"`
}

type writerEntry struct {
	key    string
	file   *os.File
	writer *csv.Writer
	elem   *list.Element
}

type writerCache struct {
	outputDir    string
	header       []string
	maxOpen      int
	order        *list.List
	entries      map[string]*writerEntry
	createdFiles int
}

/* SplitByPrimaryKey streams one CSV or Parquet file and writes each row to one output file per primary-key value. */
func SplitByPrimaryKey(ctx context.Context, cfg Config) (Summary, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cfg, err := normalizeSplitConfig(cfg)
	if err != nil {
		return Summary{}, err
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return Summary{}, fmt.Errorf("create output dir: %w", err)
	}

	source, err := inputrows.Open(cfg.InputPath)
	if err != nil {
		return Summary{}, err
	}
	defer source.Close()

	header := source.Header()
	keyIdx, err := resolveKeyIndex(header, cfg.PrimaryKey)
	if err != nil {
		return Summary{}, err
	}

	cache := newWriterCache(cfg, header)
	defer cache.closeAll()

	var totalRows atomic.Int64
	var missingRows atomic.Int64
	startedAt := time.Now()
	doneProgress := startSplitProgressReporter(ctx, &totalRows, &missingRows, source, startedAt, cfg.Progress)
	defer close(doneProgress)

	missing := newMissingRowWriter(cfg.OutputDir, cfg.MissingKeysFile, header)
	defer missing.Close()

	summary, err := processSplitRows(ctx, source, header, keyIdx, cache, missing, &totalRows, &missingRows)
	if err != nil {
		return summary, err
	}
	if err := missing.Close(); err != nil {
		return summary, err
	}
	if err := cache.closeAll(); err != nil {
		return summary, err
	}

	summary.OutputFiles = cache.createdFiles
	printSplitFinalProgress(totalRows.Load(), missingRows.Load(), source.BytesRead(), source.InputSize(), startedAt, cfg.Progress)
	return summary, nil
}

/* HashFile returns a stable SHA-256 hash for one input file. */
func HashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open input for hashing: %w", err)
	}
	defer f.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return "", fmt.Errorf("hash input: %w", err)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

/* Header returns the source header for a supported split input file. */
func Header(path string) ([]string, error) {
	source, err := inputrows.Open(path)
	if err != nil {
		return nil, err
	}
	defer source.Close()
	return append([]string(nil), source.Header()...), nil
}

/* CacheMetadataPath returns the metadata file path stored in a split output directory. */
func CacheMetadataPath(outputDir string) string {
	return filepath.Join(outputDir, cacheMetadataFileName)
}

/* ReadCacheMetadata loads split cache metadata from the output directory. */
func ReadCacheMetadata(outputDir string) (CacheMetadata, error) {
	data, err := os.ReadFile(CacheMetadataPath(outputDir))
	if err != nil {
		return CacheMetadata{}, err
	}

	var meta CacheMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return CacheMetadata{}, fmt.Errorf("decode split cache metadata: %w", err)
	}
	return meta, nil
}

/* WriteCacheMetadata persists split cache metadata into the output directory. */
func WriteCacheMetadata(outputDir string, meta CacheMetadata) error {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("create split output dir for cache metadata: %w", err)
	}

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("encode split cache metadata: %w", err)
	}
	data = append(data, '\n')

	path := CacheMetadataPath(outputDir)
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0o644); err != nil {
		return fmt.Errorf("write split cache metadata temp file: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("commit split cache metadata: %w", err)
	}
	return nil
}

/* Matches reports whether metadata matches the current split-relevant inputs. */
func (m CacheMetadata) Matches(inputHash, primaryKey, missingKeysFile string) bool {
	return strings.TrimSpace(m.InputHash) == strings.TrimSpace(inputHash) &&
		strings.TrimSpace(m.PrimaryKey) == strings.TrimSpace(primaryKey) &&
		strings.TrimSpace(m.MissingKeysFile) == strings.TrimSpace(missingKeysFile)
}

/* normalizeSplitConfig validates mandatory options and fills default values. */
func normalizeSplitConfig(cfg Config) (Config, error) {
	if strings.TrimSpace(cfg.InputPath) == "" {
		return Config{}, errors.New("input path is required")
	}
	if strings.TrimSpace(cfg.OutputDir) == "" {
		return Config{}, errors.New("output dir is required")
	}
	if strings.TrimSpace(cfg.PrimaryKey) == "" {
		return Config{}, errors.New("primary key is required")
	}
	if cfg.MaxOpenWriters <= 0 {
		cfg.MaxOpenWriters = 256
	}
	if strings.TrimSpace(cfg.MissingKeysFile) == "" {
		cfg.MissingKeysFile = "missing_keys.csv"
	}
	return cfg, nil
}

/* resolveKeyIndex resolves the index of the configured primary key from the input header. */
func resolveKeyIndex(header []string, primaryKey string) (int, error) {
	keyIdx := -1
	for i, h := range header {
		if strings.TrimSpace(h) == strings.TrimSpace(primaryKey) {
			keyIdx = i
			break
		}
	}
	if keyIdx < 0 {
		return -1, fmt.Errorf("primary key %q not found in input header", primaryKey)
	}
	return keyIdx, nil
}

/* newWriterCache creates the file-writer LRU cache for split outputs. */
func newWriterCache(cfg Config, header []string) *writerCache {
	return &writerCache{
		outputDir: cfg.OutputDir,
		header:    append([]string(nil), header...),
		maxOpen:   cfg.MaxOpenWriters,
		order:     list.New(),
		entries:   make(map[string]*writerEntry, cfg.MaxOpenWriters),
	}
}

/* startSplitProgressReporter starts periodic progress logs for split mode. */
func startSplitProgressReporter(ctx context.Context, totalRows, missingRows *atomic.Int64, source inputrows.Source, startedAt time.Time, emitter progress.Emitter) chan struct{} {
	done := make(chan struct{})
	go reportSplitProgress(ctx, done, totalRows, missingRows, source, startedAt, emitter)
	return done
}

/* reportSplitProgress emits split progress snapshots until done is closed. */
func reportSplitProgress(ctx context.Context, done <-chan struct{}, totalRows, missingRows *atomic.Int64, source inputrows.Source, startedAt time.Time, emitter progress.Emitter) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rows := totalRows.Load()
			missing := missingRows.Load()
			readBytes := source.BytesRead()
			inputSize := source.InputSize()
			pct := splitReadPercent(readBytes, inputSize)
			elapsed := time.Since(startedAt)
			rowRate, byteRate := splitRates(rows, readBytes, elapsed)
			estimatedTotalRows := estimateTotalRows(rows, pct)
			readMB := bytesToMiB(readBytes)
			totalMB := bytesToMiB(inputSize)
			emitter.Progress(progress.PhaseSplit, pct, map[string]any{
				"read_mib":        readMB,
				"input_mib":       totalMB,
				"rows_total":      rows,
				"rows_estimated":  estimatedTotalRows,
				"rows_per_sec":    rowRate,
				"read_bytes":      readBytes,
				"io_mib_per_sec":  byteRate / (1024 * 1024),
				"missing_rows":    missing,
				"eta_seconds":     splitETASeconds(readBytes, inputSize, byteRate),
				"elapsed_seconds": elapsed.Seconds(),
			})
		case <-done:
			return
		case <-ctx.Done():
			return
		}
	}
}

/* splitReadPercent returns input read completion percentage. */
func splitReadPercent(readBytes, inputSize int64) float64 {
	if inputSize <= 0 {
		return 0
	}
	pct := float64(readBytes) * 100.0 / float64(inputSize)
	if pct > 100 {
		return 100
	}
	return pct
}

/* splitRates computes row and byte throughput over elapsed time. */
func splitRates(rows, readBytes int64, elapsed time.Duration) (float64, float64) {
	if elapsed <= 0 {
		return 0, 0
	}
	seconds := elapsed.Seconds()
	return float64(rows) / seconds, float64(readBytes) / seconds
}

/* splitETASeconds estimates remaining time from current byte throughput. */
func splitETASeconds(readBytes, inputSize int64, byteRate float64) float64 {
	if byteRate <= 0 || inputSize <= 0 || readBytes > inputSize {
		return -1
	}
	return float64(inputSize-readBytes) / byteRate
}

/* printSplitFinalProgress logs one final completed progress line. */
func printSplitFinalProgress(rows, missing, readBytes, inputSize int64, startedAt time.Time, emitter progress.Emitter) {
	elapsed := time.Since(startedAt)
	rowRate, byteRate := splitRates(rows, readBytes, elapsed)
	readMB := bytesToMiB(readBytes)
	totalMB := bytesToMiB(inputSize)
	emitter.Progress(progress.PhaseSplit, 100, map[string]any{
		"read_mib":        readMB,
		"input_mib":       totalMB,
		"rows_total":      rows,
		"rows_estimated":  fmt.Sprintf("%d", rows),
		"rows_per_sec":    rowRate,
		"read_bytes":      readBytes,
		"io_mib_per_sec":  byteRate / (1024 * 1024),
		"missing_rows":    missing,
		"eta_seconds":     0.0,
		"elapsed_seconds": elapsed.Seconds(),
	})
}

/* processSplitRows performs the input streaming loop and dispatches records by key. */
func processSplitRows(ctx context.Context, source inputrows.Source, header []string, keyIdx int, cache *writerCache, missing *missingRowWriter, totalRows, missingRows *atomic.Int64) (Summary, error) {
	summary := Summary{}
	for {
		if err := ctx.Err(); err != nil {
			return summary, err
		}
		record, _, err := source.Next()
		if errors.Is(err, io.EOF) {
			return summary, nil
		}
		if err != nil {
			return summary, fmt.Errorf("read row: %w", err)
		}

		summary.TotalRows++
		totalRows.Add(1)

		key := splitKey(record, keyIdx)
		if key == "" {
			if err := missing.WriteRecord(padToHeader(record, len(header))); err != nil {
				return summary, err
			}
			summary.MissingKeyRows++
			missingRows.Add(1)
			continue
		}

		if err := writeSplitRecord(cache, key, padToHeader(record, len(header))); err != nil {
			return summary, err
		}
		summary.SplitRows++
	}
}

/* splitKey extracts and trims the split key value from a record. */
func splitKey(record []string, keyIdx int) string {
	if keyIdx >= len(record) {
		return ""
	}
	return strings.TrimSpace(record[keyIdx])
}

/* writeSplitRecord writes one normalized record to the key-specific writer. */
func writeSplitRecord(cache *writerCache, key string, record []string) error {
	entry, err := cache.get(key)
	if err != nil {
		return err
	}
	if err := entry.writer.Write(record); err != nil {
		return fmt.Errorf("write row for key %q: %w", key, err)
	}
	return nil
}

/* missingRowWriter lazily creates and writes the split output for missing keys. */
type missingRowWriter struct {
	path   string
	header []string
	file   *os.File
	writer *csv.Writer
	opened bool
}

/* newMissingRowWriter builds a lazy writer for rows with a blank split key. */
func newMissingRowWriter(outputDir, fileName string, header []string) *missingRowWriter {
	return &missingRowWriter{
		path:   filepath.Join(outputDir, fileName),
		header: append([]string(nil), header...),
	}
}

/* WriteRecord appends one row to the missing-keys output file. */
func (m *missingRowWriter) WriteRecord(record []string) error {
	if err := m.ensureOpen(); err != nil {
		return err
	}
	if err := m.writer.Write(record); err != nil {
		return fmt.Errorf("write missing key row: %w", err)
	}
	return nil
}

/* Close flushes and closes the missing-keys output file. */
func (m *missingRowWriter) Close() error {
	if !m.opened {
		return nil
	}
	m.writer.Flush()
	if err := m.writer.Error(); err != nil {
		_ = m.file.Close()
		m.opened = false
		return fmt.Errorf("flush missing keys file: %w", err)
	}
	if err := m.file.Close(); err != nil {
		m.opened = false
		return fmt.Errorf("close missing keys file: %w", err)
	}
	m.file = nil
	m.writer = nil
	m.opened = false
	return nil
}

/* ensureOpen creates missing-keys output on first write and writes the header once. */
func (m *missingRowWriter) ensureOpen() error {
	if m.opened {
		return nil
	}
	f, err := os.Create(m.path)
	if err != nil {
		return fmt.Errorf("create missing keys file: %w", err)
	}
	w := csv.NewWriter(f)
	if err := w.Write(m.header); err != nil {
		_ = f.Close()
		return fmt.Errorf("write missing keys header: %w", err)
	}
	m.file = f
	m.writer = w
	m.opened = true
	return nil
}

/* get returns a writer for key, creating or rotating cached files as needed. */
func (c *writerCache) get(key string) (*writerEntry, error) {
	if e, ok := c.entries[key]; ok {
		c.order.MoveToFront(e.elem)
		return e, nil
	}

	if len(c.entries) >= c.maxOpen {
		oldest := c.order.Back()
		if oldest != nil {
			victim := oldest.Value.(*writerEntry)
			if err := c.close(victim); err != nil {
				return nil, err
			}
		}
	}

	path := filepath.Join(c.outputDir, sanitizeFileName(key)+".csv")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open output for key %q: %w", key, err)
	}

	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat output for key %q: %w", key, err)
	}
	newFile := st.Size() == 0
	w := csv.NewWriter(f)
	if newFile {
		if err := w.Write(c.header); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("write header for key %q: %w", key, err)
		}
		c.createdFiles++
	}

	e := &writerEntry{key: key, file: f, writer: w}
	e.elem = c.order.PushFront(e)
	c.entries[key] = e
	return e, nil
}

/* closeAll flushes and closes every cached writer entry. */
func (c *writerCache) closeAll() error {
	var firstErr error
	for len(c.entries) > 0 {
		oldest := c.order.Back()
		if oldest == nil {
			break
		}
		entry := oldest.Value.(*writerEntry)
		if err := c.close(entry); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

/* close flushes and closes one cached writer entry and removes it from cache. */
func (c *writerCache) close(e *writerEntry) error {
	e.writer.Flush()
	if err := e.writer.Error(); err != nil {
		_ = e.file.Close()
		delete(c.entries, e.key)
		c.order.Remove(e.elem)
		return fmt.Errorf("flush output for key %q: %w", e.key, err)
	}
	if err := e.file.Close(); err != nil {
		delete(c.entries, e.key)
		c.order.Remove(e.elem)
		return fmt.Errorf("close output for key %q: %w", e.key, err)
	}
	delete(c.entries, e.key)
	c.order.Remove(e.elem)
	return nil
}

/* padToHeader right-pads records with empty values to match header width. */
func padToHeader(record []string, size int) []string {
	if len(record) == size {
		return record
	}
	out := make([]string, size)
	copy(out, record)
	return out
}

/* sanitizeFileName replaces unsafe path separators and null bytes in key values. */
func sanitizeFileName(v string) string {
	r := strings.NewReplacer("/", "_", "\\", "_", "\x00", "_")
	return r.Replace(v)
}

/* estimateTotalRows projects total rows from current rows and completion percent. */
func estimateTotalRows(rows int64, pct float64) string {
	if pct <= 0 || pct > 100 {
		return "?"
	}
	estimated := int64(math.Round(float64(rows) * 100.0 / pct))
	if estimated < rows {
		estimated = rows
	}
	return fmt.Sprintf("%d", estimated)
}

/* bytesToMiB converts byte count to mebibytes. */
func bytesToMiB(v int64) float64 {
	return float64(v) / (1024 * 1024)
}
