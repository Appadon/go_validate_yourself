package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

// SchemaConfig defines the full validation contract loaded from JSON.
type SchemaConfig struct {
	Fields []FieldRule `json:"fields"`
}

// FieldRule describes validation and output behavior for one CSV column.
type FieldRule struct {
	Name             string      `json:"name"`
	ParquetName      string      `json:"parquet_name"`
	Type             string      `json:"type"`
	Required         bool        `json:"required"`
	ExcludeIfMissing bool        `json:"exclude_if_missing"`
	MinLength        int         `json:"min_length"`
	Lower            bool        `json:"lower"`
	AllowedValues    []string    `json:"allowed_values"`
	Default          interface{} `json:"default"`
	NonZero          bool        `json:"non_zero"`
	DateFormats      []string    `json:"date_formats"`
	parsedAllowed    map[string]struct{}
}

// ValidationError captures one field-level validation error for a row.
type ValidationError struct {
	Row   int
	Field string
	Msg   string
}

// InvalidRow stores original data plus validation errors for error CSV output.
type InvalidRow struct {
	RowNum int
	Record []string
	Errs   []ValidationError
}

// FileResult is the outcome of processing one input file in directory mode.
type FileResult struct {
	Input       string
	ParquetPath string
	ErrorPath   string
	Stats       Stats
	Err         error
}

// DirectorySummary contains aggregated metrics for directory processing.
type DirectorySummary struct {
	Files       int
	FailedFiles int
	TotalRows   int
	ValidRows   int
	InvalidRows int
}

var defaultDateFormats = []string{
	"2006-01-02",
	"2006-01-02 15:04:05",
	time.RFC3339,
}

func main() {
	schemaPath := flag.String("schema", "", "Schema JSON file (required)")
	inputDir := flag.String("dir", "", "Directory containing CSV files to validate")
	threads := flag.Int("t", 1, "Number of concurrent workers for -dir mode")
	successDir := flag.String("success-dir", "success", "Directory for valid parquet output")
	errorDir := flag.String("error-dir", "errors", "Directory for validation error CSV output")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage:\n  %s [flags] <input.csv>\n  %s [flags] -dir <input_dir>\n", os.Args[0], os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if *threads < 1 {
		*threads = 1
	}
	if *inputDir != "" && flag.NArg() > 0 {
		exitf("use either positional <input.csv> or -dir, not both")
	}
	if *inputDir == "" && flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	if *inputDir != "" && flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	if strings.TrimSpace(*schemaPath) == "" {
		exitf("missing required -schema <path>")
	}

	if err := os.MkdirAll(*successDir, 0o755); err != nil {
		exitf("failed creating success dir: %v", err)
	}
	if err := os.MkdirAll(*errorDir, 0o755); err != nil {
		exitf("failed creating errors dir: %v", err)
	}

	schema, err := loadSchema(*schemaPath)
	if err != nil {
		exitf("failed loading schema: %v", err)
	}

	if err := validateSchema(schema); err != nil {
		exitf("invalid schema: %v", err)
	}

	if *inputDir == "" {
		input := flag.Arg(0)
		parquetPath, errorCSVPath := outputPaths(input, *successDir, *errorDir)
		stats, err := runValidationAndWriteParquet(input, parquetPath, errorCSVPath, schema)
		if err != nil {
			exitf("processing failed: %v", err)
		}
		fmt.Printf("done: total=%d valid=%d invalid=%d written=%s errors=%s\n", stats.TotalRows, stats.ValidRows, stats.InvalidRows, parquetPath, errorCSVPath)
		return
	}

	files, err := listCSVFiles(*inputDir)
	if err != nil {
		exitf("failed listing csv files: %v", err)
	}
	if len(files) == 0 {
		exitf("no csv files found in directory: %s", *inputDir)
	}

	fmt.Fprintf(os.Stderr, "starting directory run: files=%d workers=%d\n", len(files), *threads)
	summary := processDirectory(files, *threads, *successDir, *errorDir, schema)
	fmt.Printf("done: files=%d failed_files=%d total=%d valid=%d invalid=%d workers=%d\n", summary.Files, summary.FailedFiles, summary.TotalRows, summary.ValidRows, summary.InvalidRows, *threads)
	if summary.FailedFiles > 0 {
		os.Exit(1)
	}
}

type Stats struct {
	TotalRows   int
	ValidRows   int
	InvalidRows int
}

// runValidationAndWriteParquet validates a single CSV and writes:
// 1) validated rows to parquet, and
// 2) invalid rows to an error CSV.
func runValidationAndWriteParquet(input, successOutput, errorOutput string, schema SchemaConfig) (Stats, error) {
	writeCompleted := false
	defer func() {
		if !writeCompleted {
			_ = os.Remove(successOutput)
			_ = os.Remove(errorOutput)
		}
	}()

	f, err := os.Open(input)
	if err != nil {
		return Stats{}, fmt.Errorf("open input: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return Stats{}, fmt.Errorf("read header: %w", err)
	}

	headerIdx := make(map[string]int, len(header))
	for i, h := range header {
		headerIdx[strings.TrimSpace(h)] = i
	}

	for _, field := range schema.Fields {
		if _, ok := headerIdx[field.Name]; !ok {
			if field.Required || field.Default != nil {
				return Stats{}, fmt.Errorf("required schema field %q not found in CSV header", field.Name)
			}
		}
	}

	pw, err := local.NewLocalFileWriter(successOutput)
	if err != nil {
		return Stats{}, fmt.Errorf("create output: %w", err)
	}
	defer pw.Close()

	parquetSchema, err := buildParquetSchemaMetadata(schema)
	if err != nil {
		return Stats{}, fmt.Errorf("parquet schema build: %w", err)
	}

	pWriter, err := writer.NewCSVWriter(parquetSchema, pw, 4)
	if err != nil {
		return Stats{}, fmt.Errorf("new parquet writer: %w", err)
	}
	pWriter.RowGroupSize = 128 * 1024 * 1024
	pWriter.CompressionType = 1 // SNAPPY

	stats := Stats{}
	invalidRows := make([]InvalidRow, 0)
	rowNum := 1

	for {
		rowNum++
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return stats, fmt.Errorf("read row %d: %w", rowNum, err)
		}

		stats.TotalRows++
		outRow, rowErrs := validateRow(rowNum, record, headerIdx, schema)
		if len(rowErrs) > 0 {
			stats.InvalidRows++
			invalidRows = append(invalidRows, InvalidRow{
				RowNum: rowNum,
				Record: append([]string(nil), record...),
				Errs:   rowErrs,
			})
			continue
		}

		if err := pWriter.WriteString(outRow); err != nil {
			return stats, fmt.Errorf("write parquet row %d: %w", rowNum, err)
		}
		stats.ValidRows++
	}

	if err := pWriter.WriteStop(); err != nil {
		return stats, fmt.Errorf("close parquet writer: %w", err)
	}
	if err := writeErrorCSV(errorOutput, header, invalidRows); err != nil {
		return stats, fmt.Errorf("write errors csv: %w", err)
	}

	writeCompleted = true
	return stats, nil
}

// writeErrorCSV writes one row per rejected CSV row.
func writeErrorCSV(path string, header []string, rows []InvalidRow) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	outHeader := append([]string{"__row_number", "__errors"}, header...)
	if err := w.Write(outHeader); err != nil {
		return err
	}

	for _, r := range rows {
		rec := make([]string, len(header))
		copy(rec, r.Record)
		for i := len(r.Record); i < len(header); i++ {
			rec[i] = ""
		}
		rec = append([]string{strconv.Itoa(r.RowNum), formatRowErrors(r.Errs)}, rec...)
		if err := w.Write(rec); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func formatRowErrors(errs []ValidationError) string {
	parts := make([]string, 0, len(errs))
	for _, e := range errs {
		parts = append(parts, fmt.Sprintf("%s: %s", e.Field, e.Msg))
	}
	return strings.Join(parts, " | ")
}

// validateRow validates and normalizes one CSV record into parquet field values.
func validateRow(rowNum int, record []string, headerIdx map[string]int, schema SchemaConfig) ([]*string, []ValidationError) {
	out := make([]*string, 0, len(schema.Fields))
	errs := make([]ValidationError, 0)

	for _, field := range schema.Fields {
		idx, hasCol := headerIdx[field.Name]
		raw := ""
		if hasCol && idx < len(record) {
			raw = strings.TrimSpace(record[idx])
		}

		value, ferr := normalizeAndValidateValue(raw, field)
		if ferr != nil {
			errs = append(errs, ValidationError{Row: rowNum, Field: field.Name, Msg: ferr.Error()})
			continue
		}

		out = append(out, value)
	}

	if len(errs) > 0 {
		return nil, errs
	}
	return out, nil
}

// normalizeAndValidateValue converts one raw CSV value to a typed parquet-ready value.
func normalizeAndValidateValue(raw string, field FieldRule) (*string, error) {
	if isMissing(raw) {
		if field.ExcludeIfMissing {
			return nil, errors.New("missing value excluded by rule")
		}
		if field.Default != nil {
			raw = fmt.Sprint(field.Default)
		} else if field.Required {
			return nil, errors.New("value is required")
		} else {
			return nil, nil
		}
	}

	switch field.Type {
	case "string":
		v := raw
		if field.Lower {
			v = strings.ToLower(v)
		}
		if field.MinLength > 0 && len(v) < field.MinLength {
			return nil, fmt.Errorf("min_length=%d violated", field.MinLength)
		}
		if len(field.parsedAllowed) > 0 {
			if _, ok := field.parsedAllowed[v]; !ok {
				return nil, fmt.Errorf("value %q not in allowed_values", v)
			}
		}
		return stringPtr(v), nil

	case "float":
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float: %q", raw)
		}
		return stringPtr(strconv.FormatFloat(v, 'f', -1, 64)), nil

	case "int":
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			fv, ferr := strconv.ParseFloat(raw, 64)
			if ferr != nil {
				return nil, fmt.Errorf("invalid int: %q", raw)
			}
			if math.Mod(fv, 1.0) != 0 {
				return nil, fmt.Errorf("invalid int (fractional): %q", raw)
			}
			v = int64(fv)
		}
		if field.NonZero && v == 0 {
			return nil, errors.New("must be non-zero")
		}
		return stringPtr(strconv.FormatInt(v, 10)), nil

	case "date":
		for _, layout := range field.DateFormats {
			t, err := time.Parse(layout, raw)
			if err == nil {
				return stringPtr(strconv.FormatInt(int64(daysSinceEpoch(t)), 10)), nil
			}
		}
		return nil, fmt.Errorf("invalid date: %q", raw)

	default:
		return nil, fmt.Errorf("unsupported type: %s", field.Type)
	}
}

func daysSinceEpoch(t time.Time) int32 {
	y, m, d := t.Date()
	utcDate := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	return int32(utcDate.Unix() / 86400)
}

func loadSchema(path string) (SchemaConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return SchemaConfig{}, err
	}
	var cfg SchemaConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return SchemaConfig{}, err
	}
	return cfg, nil
}

// validateSchema normalizes and verifies schema config once at startup.
func validateSchema(cfg SchemaConfig) error {
	if len(cfg.Fields) == 0 {
		return errors.New("schema.fields cannot be empty")
	}

	seenSrc := map[string]struct{}{}
	seenParquet := map[string]struct{}{}

	for i := range cfg.Fields {
		f := &cfg.Fields[i]
		f.Name = strings.TrimSpace(f.Name)
		f.ParquetName = strings.TrimSpace(f.ParquetName)
		f.Type = strings.ToLower(strings.TrimSpace(f.Type))
		if f.Required == false && f.Default == nil {
			// Keep user-provided false value; this line keeps behavior explicit.
		}

		if f.Name == "" {
			return errors.New("field name cannot be empty")
		}
		if f.ParquetName == "" {
			f.ParquetName = toSnakeCase(f.Name)
		}
		if _, ok := seenSrc[f.Name]; ok {
			return fmt.Errorf("duplicate schema field name: %s", f.Name)
		}
		if _, ok := seenParquet[f.ParquetName]; ok {
			return fmt.Errorf("duplicate parquet_name: %s", f.ParquetName)
		}
		seenSrc[f.Name] = struct{}{}
		seenParquet[f.ParquetName] = struct{}{}

		switch f.Type {
		case "string", "float", "int", "date":
		default:
			return fmt.Errorf("field %q has unsupported type %q", f.Name, f.Type)
		}

		if f.Type == "date" {
			if len(f.DateFormats) == 0 {
				f.DateFormats = append([]string{}, defaultDateFormats...)
			}
		}

		if len(f.AllowedValues) > 0 {
			f.parsedAllowed = make(map[string]struct{}, len(f.AllowedValues))
			for _, v := range f.AllowedValues {
				vv := strings.TrimSpace(v)
				if f.Lower {
					vv = strings.ToLower(vv)
				}
				f.parsedAllowed[vv] = struct{}{}
			}
		}
	}

	return nil
}

// buildParquetSchemaMetadata builds parquet-go CSV metadata from schema fields.
func buildParquetSchemaMetadata(cfg SchemaConfig) ([]string, error) {
	md := make([]string, 0, len(cfg.Fields))
	for _, f := range cfg.Fields {
		tagParts := []string{"name=" + f.ParquetName}
		repetition := "REQUIRED"
		if !f.Required {
			repetition = "OPTIONAL"
		}
		tagParts = append(tagParts, "repetitiontype="+repetition)

		switch f.Type {
		case "string":
			tagParts = append(tagParts, "type=BYTE_ARRAY", "convertedtype=UTF8", "encoding=PLAIN_DICTIONARY")
		case "float":
			tagParts = append(tagParts, "type=DOUBLE")
		case "int":
			tagParts = append(tagParts, "type=INT64")
		case "date":
			tagParts = append(tagParts, "type=INT32", "convertedtype=DATE")
		default:
			return nil, fmt.Errorf("unsupported type in parquet schema: %s", f.Type)
		}
		md = append(md, strings.Join(tagParts, ", "))
	}
	return md, nil
}

func toSnakeCase(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	replacer := strings.NewReplacer(" ", "_", "-", "_", "/", "_", ".", "_")
	s = replacer.Replace(s)
	for strings.Contains(s, "__") {
		s = strings.ReplaceAll(s, "__", "_")
	}
	return s
}

func exitf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func stringPtr(s string) *string {
	return &s
}

func baseNameWithoutExt(path string) string {
	name := filepath.Base(path)
	ext := filepath.Ext(name)
	return strings.TrimSuffix(name, ext)
}

func isMissing(s string) bool {
	v := strings.TrimSpace(strings.ToLower(s))
	return v == "" || v == "none" || v == "null" || v == "nan" || v == "na" || v == "n/a"
}

func outputPaths(input, successDir, errorDir string) (string, string) {
	base := baseNameWithoutExt(input)
	return filepath.Join(successDir, base+".parquet"), filepath.Join(errorDir, base+"_error.csv")
}

func listCSVFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.EqualFold(filepath.Ext(name), ".csv") {
			files = append(files, filepath.Join(dir, name))
		}
	}
	// Stable ordering improves reproducibility and makes operational debugging easier.
	sort.Strings(files)
	return files, nil
}

// processDirectory runs parallel file validation using a worker pool.
func processDirectory(files []string, workers int, successDir, errorDir string, schema SchemaConfig) DirectorySummary {
	jobs := make(chan string)
	results := make(chan FileResult, workers*4)
	var wg sync.WaitGroup
	total := len(files)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for input := range jobs {
				parquetPath, errorPath := outputPaths(input, successDir, errorDir)
				stats, err := runValidationAndWriteParquet(input, parquetPath, errorPath, schema)
				results <- FileResult{
					Input:       input,
					ParquetPath: parquetPath,
					ErrorPath:   errorPath,
					Stats:       stats,
					Err:         err,
				}
			}
		}()
	}

	go func() {
		for _, f := range files {
			jobs <- f
		}
		close(jobs)
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	// Heartbeat progress for large runs where per-file logs are too noisy.
	doneProgress := make(chan struct{})
	var received atomic.Int64
	startedAt := time.Now()
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c := received.Load()
				pct := float64(c) * 100.0 / float64(total)
				if total == 0 {
					pct = 100
				}
				elapsed := time.Since(startedAt)
				rate := 0.0
				if elapsed > 0 {
					rate = float64(c) / elapsed.Seconds()
				}
				remaining := total - int(c)
				eta := "unknown"
				if rate > 0 && remaining >= 0 {
					eta = formatDuration(time.Duration(float64(remaining)/rate) * time.Second)
				}
				fmt.Fprintf(os.Stderr, "progress: %d/%d files (%.2f%%) rate=%.2f files/s eta=%s elapsed=%s\n", c, total, pct, rate, eta, formatDuration(elapsed))
			case <-doneProgress:
				return
			}
		}
	}()

	summary := DirectorySummary{Files: len(files)}
	for r := range results {
		received.Add(1)
		if r.Err != nil {
			summary.FailedFiles++
			fmt.Fprintf(os.Stderr, "file=%s status=failed error=%v\n", r.Input, r.Err)
			continue
		}
		summary.TotalRows += r.Stats.TotalRows
		summary.ValidRows += r.Stats.ValidRows
		summary.InvalidRows += r.Stats.InvalidRows
	}
	close(doneProgress)
	fmt.Fprintf(os.Stderr, "progress: %d/%d files (100.00%%)\n", received.Load(), total)
	return summary
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
