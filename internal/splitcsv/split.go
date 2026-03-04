package splitcsv

import (
	"container/list"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"rt_policies_validator/internal/console"
)

/* Config defines behavior for splitting one CSV into many files by key. */
type Config struct {
	InputPath       string
	OutputDir       string
	PrimaryKey      string
	MaxOpenWriters  int
	MissingKeysFile string
}

/* Summary captures split run metrics. */
type Summary struct {
	TotalRows      int
	SplitRows      int
	MissingKeyRows int
	OutputFiles    int
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

/* SplitByPrimaryKey streams one CSV and writes each row to one output file per primary-key value. */
func SplitByPrimaryKey(cfg Config) (Summary, error) {
	cfg, err := normalizeSplitConfig(cfg)
	if err != nil {
		return Summary{}, err
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return Summary{}, fmt.Errorf("create output dir: %w", err)
	}

	in, reader, counter, inputSize, err := openInputCSV(cfg.InputPath)
	if err != nil {
		return Summary{}, err
	}
	defer in.Close()

	header, keyIdx, err := readHeaderAndKeyIndex(reader, cfg.PrimaryKey)
	if err != nil {
		return Summary{}, err
	}

	cache := newWriterCache(cfg, header)
	defer cache.closeAll()

	var totalRows atomic.Int64
	var missingRows atomic.Int64
	startedAt := time.Now()
	doneProgress := startSplitProgressReporter(&totalRows, &missingRows, counter, inputSize, startedAt)
	defer close(doneProgress)

	missing := newMissingRowWriter(cfg.OutputDir, cfg.MissingKeysFile, header)
	defer missing.Close()

	summary, err := processSplitRows(reader, header, keyIdx, cache, missing, &totalRows, &missingRows)
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
	printSplitFinalProgress(totalRows.Load(), missingRows.Load(), counter.bytesRead.Load(), startedAt)
	return summary, nil
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

/* openInputCSV opens an input file and returns a CSV reader with byte-count tracking. */
func openInputCSV(path string) (*os.File, *csv.Reader, *countingReader, int64, error) {
	in, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, 0, fmt.Errorf("open input: %w", err)
	}
	stat, err := in.Stat()
	if err != nil {
		_ = in.Close()
		return nil, nil, nil, 0, fmt.Errorf("stat input: %w", err)
	}

	counter := &countingReader{r: in}
	reader := csv.NewReader(counter)
	reader.FieldsPerRecord = -1
	return in, reader, counter, stat.Size(), nil
}

/* readHeaderAndKeyIndex reads the header and resolves the index of the configured primary key. */
func readHeaderAndKeyIndex(reader *csv.Reader, primaryKey string) ([]string, int, error) {
	header, err := reader.Read()
	if err != nil {
		return nil, -1, fmt.Errorf("read header: %w", err)
	}

	keyIdx := -1
	for i, h := range header {
		if strings.TrimSpace(h) == strings.TrimSpace(primaryKey) {
			keyIdx = i
			break
		}
	}
	if keyIdx < 0 {
		return nil, -1, fmt.Errorf("primary key %q not found in CSV header", primaryKey)
	}
	return header, keyIdx, nil
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
func startSplitProgressReporter(totalRows, missingRows *atomic.Int64, counter *countingReader, inputSize int64, startedAt time.Time) chan struct{} {
	done := make(chan struct{})
	go reportSplitProgress(done, totalRows, missingRows, counter, inputSize, startedAt)
	return done
}

/* reportSplitProgress emits split progress snapshots until done is closed. */
func reportSplitProgress(done <-chan struct{}, totalRows, missingRows *atomic.Int64, counter *countingReader, inputSize int64, startedAt time.Time) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rows := totalRows.Load()
			missing := missingRows.Load()
			readBytes := counter.bytesRead.Load()
			pct := splitReadPercent(readBytes, inputSize)
			elapsed := time.Since(startedAt)
			rowRate, byteRate := splitRates(rows, readBytes, elapsed)
			eta := splitETA(readBytes, inputSize, byteRate)
			estimatedTotalRows := estimateTotalRows(rows, pct)
			readMB := bytesToMiB(readBytes)
			totalMB := bytesToMiB(inputSize)
			console.Progressf(console.ProgressSnapshot{
				Segments: []string{
					fmt.Sprintf("%.2f/%.2f mb", readMB, totalMB),
					fmt.Sprintf("%d/%s rows", rows, estimatedTotalRows),
					fmt.Sprintf("%.2f%%", pct),
					fmt.Sprintf("%.2f rows/s", rowRate),
					fmt.Sprintf("read=%s io=%.2f MiB", console.FormatBytes(readBytes), byteRate/(1024*1024)),
					fmt.Sprintf("%d missing", missing),
					fmt.Sprintf("eta %s", eta),
					fmt.Sprintf("elapsed %s", console.FormatDuration(elapsed)),
				},
			})
		case <-done:
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

/* splitETA estimates remaining time from current byte throughput. */
func splitETA(readBytes, inputSize int64, byteRate float64) string {
	if byteRate <= 0 || inputSize <= 0 || readBytes > inputSize {
		return "unknown"
	}
	etaSeconds := float64(inputSize-readBytes) / byteRate
	return console.FormatDuration(time.Duration(etaSeconds) * time.Second)
}

/* printSplitFinalProgress logs one final completed progress line. */
func printSplitFinalProgress(rows, missing, readBytes int64, startedAt time.Time) {
	elapsed := time.Since(startedAt)
	rowRate, byteRate := splitRates(rows, readBytes, elapsed)
	readMB := bytesToMiB(readBytes)
	console.Progressf(console.ProgressSnapshot{
		Segments: []string{
			fmt.Sprintf("%.2f/%.2f mb", readMB, readMB),
			fmt.Sprintf("%d/%d rows", rows, rows),
			"100.00%",
			fmt.Sprintf("%.2f rows/s", rowRate),
			fmt.Sprintf("read=%s io=%.2f MiB", console.FormatBytes(readBytes), byteRate/(1024*1024)),
			fmt.Sprintf("%d missing", missing),
			"eta 0s",
			fmt.Sprintf("elapsed %s", console.FormatDuration(elapsed)),
		},
	})
}

/* processSplitRows performs the input streaming loop and dispatches records by key. */
func processSplitRows(reader *csv.Reader, header []string, keyIdx int, cache *writerCache, missing *missingRowWriter, totalRows, missingRows *atomic.Int64) (Summary, error) {
	summary := Summary{}
	for {
		record, err := reader.Read()
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

func padToHeader(record []string, size int) []string {
	if len(record) == size {
		return record
	}
	out := make([]string, size)
	copy(out, record)
	return out
}

func sanitizeFileName(v string) string {
	r := strings.NewReplacer("/", "_", "\\", "_", "\x00", "_")
	return r.Replace(v)
}

type countingReader struct {
	r         io.Reader
	bytesRead atomic.Int64
}

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

func bytesToMiB(v int64) float64 {
	return float64(v) / (1024 * 1024)
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.bytesRead.Add(int64(n))
	}
	return n, err
}
