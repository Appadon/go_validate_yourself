package splitcsv

import (
	"container/list"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

// Config defines behavior for splitting one CSV into many files by key.
type Config struct {
	InputPath       string
	OutputDir       string
	PrimaryKey      string
	MaxOpenWriters  int
	MissingKeysFile string
}

// Summary captures split run metrics.
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

// SplitByPrimaryKey streams input rows and writes each row to <primaryKey>.csv in one output folder.
func SplitByPrimaryKey(cfg Config) (Summary, error) {
	if strings.TrimSpace(cfg.InputPath) == "" {
		return Summary{}, errors.New("input path is required")
	}
	if strings.TrimSpace(cfg.OutputDir) == "" {
		return Summary{}, errors.New("output dir is required")
	}
	if strings.TrimSpace(cfg.PrimaryKey) == "" {
		return Summary{}, errors.New("primary key is required")
	}
	if cfg.MaxOpenWriters <= 0 {
		cfg.MaxOpenWriters = 256
	}
	if strings.TrimSpace(cfg.MissingKeysFile) == "" {
		cfg.MissingKeysFile = "missing_keys.csv"
	}

	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return Summary{}, fmt.Errorf("create output dir: %w", err)
	}

	in, err := os.Open(cfg.InputPath)
	if err != nil {
		return Summary{}, fmt.Errorf("open input: %w", err)
	}
	defer in.Close()
	inStat, err := in.Stat()
	if err != nil {
		return Summary{}, fmt.Errorf("stat input: %w", err)
	}

	cr := &countingReader{r: in}
	r := csv.NewReader(cr)
	r.FieldsPerRecord = -1

	header, err := r.Read()
	if err != nil {
		return Summary{}, fmt.Errorf("read header: %w", err)
	}

	keyIdx := -1
	for i, h := range header {
		if strings.TrimSpace(h) == strings.TrimSpace(cfg.PrimaryKey) {
			keyIdx = i
			break
		}
	}
	if keyIdx < 0 {
		return Summary{}, fmt.Errorf("primary key %q not found in CSV header", cfg.PrimaryKey)
	}

	cache := &writerCache{
		outputDir: cfg.OutputDir,
		header:    append([]string(nil), header...),
		maxOpen:   cfg.MaxOpenWriters,
		order:     list.New(),
		entries:   make(map[string]*writerEntry, cfg.MaxOpenWriters),
	}
	defer cache.closeAll()

	var totalRows atomic.Int64
	var missingRows atomic.Int64
	doneProgress := make(chan struct{})
	startedAt := time.Now()
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				rows := totalRows.Load()
				missing := missingRows.Load()
				readBytes := cr.bytesRead.Load()
				pct := 0.0
				if inStat.Size() > 0 {
					pct = float64(readBytes) * 100.0 / float64(inStat.Size())
					if pct > 100 {
						pct = 100
					}
				}
				elapsed := time.Since(startedAt)
				rowRate := 0.0
				byteRate := 0.0
				if elapsed > 0 {
					rowRate = float64(rows) / elapsed.Seconds()
					byteRate = float64(readBytes) / elapsed.Seconds()
				}
				eta := "unknown"
				if byteRate > 0 && inStat.Size() > 0 && readBytes <= inStat.Size() {
					etaSeconds := float64(inStat.Size()-readBytes) / byteRate
					eta = formatDuration(time.Duration(etaSeconds) * time.Second)
				}
				fmt.Fprintf(
					os.Stderr,
					"split progress: rows=%d missing=%d read=%.2f%% bytes=%s row_rate=%.2f rows/s io_rate=%.2f MiB/s eta=%s elapsed=%s\n",
					rows,
					missing,
					pct,
					formatBytes(readBytes),
					rowRate,
					byteRate/(1024*1024),
					eta,
					formatDuration(elapsed),
				)
			case <-doneProgress:
				return
			}
		}
	}()
	defer close(doneProgress)

	var missingFile *os.File
	var missingWriter *csv.Writer
	missingPath := filepath.Join(cfg.OutputDir, cfg.MissingKeysFile)
	missingOpened := false
	openMissing := func() error {
		if missingOpened {
			return nil
		}
		f, e := os.Create(missingPath)
		if e != nil {
			return fmt.Errorf("create missing keys file: %w", e)
		}
		w := csv.NewWriter(f)
		if e := w.Write(header); e != nil {
			_ = f.Close()
			return fmt.Errorf("write missing keys header: %w", e)
		}
		missingFile = f
		missingWriter = w
		missingOpened = true
		return nil
	}
	defer func() {
		if missingWriter != nil {
			missingWriter.Flush()
		}
		if missingFile != nil {
			_ = missingFile.Close()
		}
	}()

	summary := Summary{}
	for {
		record, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return summary, fmt.Errorf("read row: %w", err)
		}

		summary.TotalRows++
		totalRows.Add(1)
		key := ""
		if keyIdx < len(record) {
			key = strings.TrimSpace(record[keyIdx])
		}
		if key == "" {
			if err := openMissing(); err != nil {
				return summary, err
			}
			if err := missingWriter.Write(padToHeader(record, len(header))); err != nil {
				return summary, fmt.Errorf("write missing key row: %w", err)
			}
			summary.MissingKeyRows++
			missingRows.Add(1)
			continue
		}

		entry, err := cache.get(key)
		if err != nil {
			return summary, err
		}
		if err := entry.writer.Write(padToHeader(record, len(header))); err != nil {
			return summary, fmt.Errorf("write row for key %q: %w", key, err)
		}
		summary.SplitRows++
	}

	if missingWriter != nil {
		missingWriter.Flush()
		if err := missingWriter.Error(); err != nil {
			return summary, fmt.Errorf("flush missing keys file: %w", err)
		}
	}

	if err := cache.closeAll(); err != nil {
		return summary, err
	}
	summary.OutputFiles = cache.createdFiles
	fmt.Fprintf(
		os.Stderr,
		"split progress: rows=%d missing=%d read=100.00%% bytes=%s elapsed=%s\n",
		totalRows.Load(),
		missingRows.Load(),
		formatBytes(cr.bytesRead.Load()),
		formatDuration(time.Since(startedAt)),
	)
	return summary, nil
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

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.bytesRead.Add(int64(n))
	}
	return n, err
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	seconds := int(d.Seconds())
	h := seconds / 3600
	m := (seconds % 3600) / 60
	s := seconds % 60
	if h > 0 {
		return fmt.Sprintf("%dh%02dm%02ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%02ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

func formatBytes(v int64) string {
	const (
		ki = 1024
		mi = 1024 * ki
		gi = 1024 * mi
	)
	switch {
	case v >= gi:
		return fmt.Sprintf("%.2f GiB", float64(v)/gi)
	case v >= mi:
		return fmt.Sprintf("%.2f MiB", float64(v)/mi)
	case v >= ki:
		return fmt.Sprintf("%.2f KiB", float64(v)/ki)
	default:
		return fmt.Sprintf("%d B", v)
	}
}
