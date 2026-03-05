package validator

import (
	"encoding/csv"
	"encoding/json"
	"errors"
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

	"go_validate_yourself/internal/console"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

/* SchemaConfig defines the full validation contract loaded from JSON. */
type SchemaConfig struct {
	Fields []FieldRule `json:"fields"`
}

/* FieldRule describes validation and output behavior for one CSV column. */
type FieldRule struct {
	Name                string            `json:"name"`
	ParquetName         string            `json:"parquet_name"`
	Type                string            `json:"type"`
	Required            bool              `json:"required"`
	ExcludeIfMissing    bool              `json:"exclude_if_missing"`
	MinLength           int               `json:"min_length"`
	Lower               bool              `json:"lower"`
	AllowedValues       []string          `json:"allowed_values"`
	InlineReplace       map[string]string `json:"inline_replace"`
	Default             interface{}       `json:"default"`
	NonZero             bool              `json:"non_zero"`
	DateFormats         []string          `json:"date_formats"`
	parsedAllowed       map[string]struct{}
	parsedInlineReplace map[string]string
}

/* Stats captures row-level counts for one file. */
type Stats struct {
	TotalRows   int
	ValidRows   int
	InvalidRows int
}

/* FileResult is the outcome of processing one input file in directory mode. */
type FileResult struct {
	Input       string
	ParquetPath string
	ErrorPath   string
	Stats       Stats
	Err         error
}

/* DirectorySummary contains aggregated metrics for directory processing. */
type DirectorySummary struct {
	Files       int
	FailedFiles int
	TotalRows   int
	ValidRows   int
	InvalidRows int
}

/* ValidationError captures one field-level validation error for a row. */
type ValidationError struct {
	Row   int
	Field string
	Msg   string
}

/* InvalidRow stores original data plus validation errors for error CSV output. */
type InvalidRow struct {
	RowNum int
	Record []string
	Errs   []ValidationError
}

var defaultDateFormats = []string{
	"2006-01-02",
	"2006-01-02 15:04:05",
	time.RFC3339,
}

/* LoadSchema reads schema configuration from JSON. */
func LoadSchema(path string) (SchemaConfig, error) {
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

/* ValidateSchema normalizes and verifies schema config once at startup. */
func ValidateSchema(cfg *SchemaConfig) error {
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

		if len(f.InlineReplace) > 0 {
			f.parsedInlineReplace = make(map[string]string, len(f.InlineReplace))
			for from, to := range f.InlineReplace {
				key := strings.TrimSpace(from)
				if key == "" {
					return fmt.Errorf("field %q has empty inline_replace key", f.Name)
				}
				if f.Lower {
					key = strings.ToLower(key)
					to = strings.ToLower(to)
				}
				f.parsedInlineReplace[key] = strings.TrimSpace(to)
			}
		}
	}

	return nil
}

/* RunValidationAndWriteParquet validates one CSV and writes parquet + error CSV outputs. */
func RunValidationAndWriteParquet(input, successOutput, errorOutput string, schema SchemaConfig, writeEmptyError bool) (Stats, error) {
	state := &validationOutputState{
		successOutput: successOutput,
		errorOutput:   errorOutput,
	}
	defer state.cleanupOnFailure()

	f, reader, header, headerIdx, err := openCSVWithHeaderIndex(input)
	if err != nil {
		return Stats{}, err
	}
	defer f.Close()

	if err := validateSchemaFieldsAgainstHeader(schema, headerIdx); err != nil {
		return Stats{}, err
	}

	pWriter, parquetFile, err := openParquetWriter(successOutput, schema)
	if err != nil {
		return Stats{}, err
	}
	defer parquetFile.Close()

	stats, invalidRows, err := validateAndWriteRows(reader, headerIdx, schema, pWriter)
	if err != nil {
		return stats, err
	}

	if err := pWriter.WriteStop(); err != nil {
		return stats, fmt.Errorf("close parquet writer: %w", err)
	}
	if err := finalizeErrorOutput(errorOutput, header, invalidRows, writeEmptyError); err != nil {
		return stats, err
	}

	state.writeCompleted = true
	return stats, nil
}

/* validationOutputState tracks output paths and removes partial files when processing fails. */
type validationOutputState struct {
	successOutput  string
	errorOutput    string
	writeCompleted bool
}

/* cleanupOnFailure removes partial success and error files when a run exits early. */
func (s *validationOutputState) cleanupOnFailure() {
	if s.writeCompleted {
		return
	}
	_ = os.Remove(s.successOutput)
	_ = os.Remove(s.errorOutput)
}

/* openCSVWithHeaderIndex opens input CSV and returns header plus a header-index map. */
func openCSVWithHeaderIndex(input string) (*os.File, *csv.Reader, []string, map[string]int, error) {
	f, err := os.Open(input)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("open input: %w", err)
	}

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		_ = f.Close()
		return nil, nil, nil, nil, fmt.Errorf("read header: %w", err)
	}

	headerIdx := make(map[string]int, len(header))
	for i, h := range header {
		headerIdx[strings.TrimSpace(h)] = i
	}
	return f, reader, header, headerIdx, nil
}

/* validateSchemaFieldsAgainstHeader verifies required/defaulted schema fields exist in the input header. */
func validateSchemaFieldsAgainstHeader(schema SchemaConfig, headerIdx map[string]int) error {
	for _, field := range schema.Fields {
		if _, ok := headerIdx[field.Name]; !ok && (field.Required || field.Default != nil) {
			return fmt.Errorf("required schema field %q not found in CSV header", field.Name)
		}
	}
	return nil
}

/* openParquetWriter creates a parquet CSV writer configured for validation output. */
func openParquetWriter(successOutput string, schema SchemaConfig) (*writer.CSVWriter, interface{ Close() error }, error) {
	pw, err := local.NewLocalFileWriter(successOutput)
	if err != nil {
		return nil, nil, fmt.Errorf("create output: %w", err)
	}

	parquetSchema, err := buildParquetSchemaMetadata(schema)
	if err != nil {
		_ = pw.Close()
		return nil, nil, fmt.Errorf("parquet schema build: %w", err)
	}
	pWriter, err := writer.NewCSVWriter(parquetSchema, pw, 4)
	if err != nil {
		_ = pw.Close()
		return nil, nil, fmt.Errorf("new parquet writer: %w", err)
	}
	pWriter.RowGroupSize = 128 * 1024 * 1024
	pWriter.CompressionType = 1
	return pWriter, pw, nil
}

/* validateAndWriteRows validates each CSV row, writes valid rows to parquet, and captures invalid rows. */
func validateAndWriteRows(reader *csv.Reader, headerIdx map[string]int, schema SchemaConfig, pWriter *writer.CSVWriter) (Stats, []InvalidRow, error) {
	stats := Stats{}
	invalidRows := make([]InvalidRow, 0)
	rowNum := 1

	for {
		rowNum++
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return stats, invalidRows, nil
		}
		if err != nil {
			return stats, invalidRows, fmt.Errorf("read row %d: %w", rowNum, err)
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
			return stats, invalidRows, fmt.Errorf("write parquet row %d: %w", rowNum, err)
		}
		stats.ValidRows++
	}
}

/* finalizeErrorOutput writes error rows when needed or removes stale error outputs when none exist. */
func finalizeErrorOutput(errorOutput string, header []string, invalidRows []InvalidRow, writeEmptyError bool) error {
	if len(invalidRows) > 0 || writeEmptyError {
		if err := writeErrorCSV(errorOutput, header, invalidRows); err != nil {
			return fmt.Errorf("write errors csv: %w", err)
		}
		return nil
	}
	if err := os.Remove(errorOutput); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove empty error csv: %w", err)
	}
	return nil
}

/* OutputPaths returns parquet and error paths for one input file. */
func OutputPaths(input, successDir, errorDir string) (string, string) {
	base := baseNameWithoutExt(input)
	return filepath.Join(successDir, base+".parquet"), filepath.Join(errorDir, base+"_error.csv")
}

/* ListCSVFiles finds CSV files in a directory in stable sorted order. */
func ListCSVFiles(dir string) ([]string, error) {
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
	sort.Strings(files)
	return files, nil
}

/* ProcessDirectory runs parallel file validation using a worker pool. */
func ProcessDirectory(files []string, workers int, successDir, errorDir string, schema SchemaConfig, writeEmptyError bool) DirectorySummary {
	if workers < 1 {
		workers = 1
	}
	jobs := make(chan string)
	results := make(chan FileResult, workers*4)
	var wg sync.WaitGroup
	total := len(files)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go directoryWorker(jobs, results, successDir, errorDir, schema, writeEmptyError, &wg)
	}

	go dispatchValidationJobs(files, jobs)
	go closeResultsOnWorkersDone(results, &wg)

	var received atomic.Int64
	startedAt := time.Now()
	doneProgress := startDirectoryProgressReporter(&received, total, startedAt)

	summary := DirectorySummary{Files: len(files)}
	for r := range results {
		received.Add(1)
		if r.Err != nil {
			summary.FailedFiles++
			console.Errorf("file=%s status=failed error=%v", r.Input, r.Err)
			continue
		}
		summary.TotalRows += r.Stats.TotalRows
		summary.ValidRows += r.Stats.ValidRows
		summary.InvalidRows += r.Stats.InvalidRows
	}
	close(doneProgress)
	printDirectoryFinalProgress(received.Load(), total)
	return summary
}

/* directoryWorker processes input files from jobs and sends one result per file. */
func directoryWorker(jobs <-chan string, results chan<- FileResult, successDir, errorDir string, schema SchemaConfig, writeEmptyError bool, wg *sync.WaitGroup) {
	defer wg.Done()
	for input := range jobs {
		parquetPath, errorPath := OutputPaths(input, successDir, errorDir)
		stats, err := RunValidationAndWriteParquet(input, parquetPath, errorPath, schema, writeEmptyError)
		results <- FileResult{
			Input:       input,
			ParquetPath: parquetPath,
			ErrorPath:   errorPath,
			Stats:       stats,
			Err:         err,
		}
	}
}

/* dispatchValidationJobs enqueues all files and closes jobs when dispatch is complete. */
func dispatchValidationJobs(files []string, jobs chan<- string) {
	for _, f := range files {
		jobs <- f
	}
	close(jobs)
}

/* closeResultsOnWorkersDone closes results after all workers finish. */
func closeResultsOnWorkersDone(results chan<- FileResult, wg *sync.WaitGroup) {
	wg.Wait()
	close(results)
}

/* startDirectoryProgressReporter starts periodic progress logs for directory mode. */
func startDirectoryProgressReporter(received *atomic.Int64, total int, startedAt time.Time) chan struct{} {
	done := make(chan struct{})
	go reportDirectoryProgress(done, received, total, startedAt)
	return done
}

/* reportDirectoryProgress emits progress snapshots until done is closed. */
func reportDirectoryProgress(done <-chan struct{}, received *atomic.Int64, total int, startedAt time.Time) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			completed := received.Load()
			pct := directoryPercent(completed, total)
			elapsed := time.Since(startedAt)
			rate := directoryRate(completed, elapsed)
			eta := directoryETA(total-int(completed), rate)
			console.Progressf(console.ProgressSnapshot{
				Segments: []string{
					fmt.Sprintf("%d/%d files", completed, total),
					fmt.Sprintf("%.2f%%", pct),
					fmt.Sprintf("%.2f files/s", rate),
					fmt.Sprintf("eta %s", eta),
					fmt.Sprintf("elapsed %s", console.FormatDuration(elapsed)),
				},
			})
		case <-done:
			return
		}
	}
}

/* directoryPercent returns completion percentage for directory mode. */
func directoryPercent(completed int64, total int) float64 {
	if total == 0 {
		return 100
	}
	return float64(completed) * 100.0 / float64(total)
}

/* directoryRate returns completed files per second. */
func directoryRate(completed int64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(completed) / elapsed.Seconds()
}

/* directoryETA estimates remaining duration from completion rate. */
func directoryETA(remaining int, rate float64) string {
	if rate <= 0 || remaining < 0 {
		return "unknown"
	}
	return console.FormatDuration(time.Duration(float64(remaining)/rate) * time.Second)
}

/* printDirectoryFinalProgress logs one final completed progress line. */
func printDirectoryFinalProgress(completed int64, total int) {
	console.Progressf(console.ProgressSnapshot{
		Segments: []string{
			fmt.Sprintf("%d/%d files", completed, total),
			"100.00%",
			"0.00 files/s",
			"eta 0s",
			"elapsed done",
		},
	})
}

/* writeErrorCSV persists invalid records and their error details to CSV. */
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

/* formatRowErrors joins field-level errors into one serialized message. */
func formatRowErrors(errs []ValidationError) string {
	parts := make([]string, 0, len(errs))
	for _, e := range errs {
		parts = append(parts, fmt.Sprintf("%s: %s", e.Field, e.Msg))
	}
	return strings.Join(parts, " | ")
}

/* validateRow validates one CSV row and returns parquet-ready values or errors. */
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

/* normalizeAndValidateValue applies field rules and returns normalized output value. */
func normalizeAndValidateValue(raw string, field FieldRule) (*string, error) {
	if len(field.parsedInlineReplace) > 0 {
		lookup := raw
		if field.Lower {
			lookup = strings.ToLower(lookup)
		}
		if replacement, ok := field.parsedInlineReplace[lookup]; ok {
			raw = replacement
		}
	}

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

/* buildParquetSchemaMetadata converts schema config into parquet-go CSV metadata tags. */
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

/* toSnakeCase creates a basic snake_case identifier from a source field name. */
func toSnakeCase(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	replacer := strings.NewReplacer(" ", "_", "-", "_", "/", "_", ".", "_")
	s = replacer.Replace(s)
	for strings.Contains(s, "__") {
		s = strings.ReplaceAll(s, "__", "_")
	}
	return s
}

/* daysSinceEpoch converts a date to parquet DATE logical type day count. */
func daysSinceEpoch(t time.Time) int32 {
	y, m, d := t.Date()
	utcDate := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	return int32(utcDate.Unix() / 86400)
}

/* stringPtr returns a pointer to the provided string value. */
func stringPtr(s string) *string {
	return &s
}

/* baseNameWithoutExt returns filename without extension. */
func baseNameWithoutExt(path string) string {
	name := filepath.Base(path)
	ext := filepath.Ext(name)
	return strings.TrimSuffix(name, ext)
}

/* isMissing identifies null-like text values used in input CSV rows. */
func isMissing(s string) bool {
	v := strings.TrimSpace(strings.ToLower(s))
	return v == "" || v == "none" || v == "null" || v == "nan" || v == "na" || v == "n/a"
}
