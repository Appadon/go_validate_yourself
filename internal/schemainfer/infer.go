package schemainfer

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"go_validate_yourself/internal/validator"
)

const (
	StrategyByteSpread = "byte-spread"
	StrategyHead       = "head"

	defaultSampleSize  = 100
	defaultMaxScanRows = 8
	maxSampleValues    = 12
)

var dateLayouts = []string{
	"2006-01-02",
	"2006/01/02",
	"02/01/2006",
	"01/02/2006",
	"20060102",
}

var datetimeLayouts = []string{
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05.999999",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05.999999",
	time.RFC3339,
	time.RFC3339Nano,
}

/* Options controls schema inference sampling. */
type Options struct {
	SampleSize     int    `json:"sample_size"`
	Strategy       string `json:"strategy"`
	KeepSamples    bool   `json:"keep_samples"`
	MaxScanRecords int    `json:"max_scan_records"`
}

/* Result captures inferred schema fields, sampled data, and timing metadata. */
type Result struct {
	InputPath           string                 `json:"input_path"`
	FileSizeBytes       int64                  `json:"file_size_bytes"`
	DataStartOffset     int64                  `json:"data_start_offset"`
	Strategy            string                 `json:"strategy"`
	RequestedSampleSize int                    `json:"requested_sample_size"`
	SampledRows         int                    `json:"sampled_rows"`
	DurationMillis      int64                  `json:"duration_millis"`
	Schema              validator.SchemaConfig `json:"schema"`
	Fields              []FieldInference       `json:"fields"`
	Samples             []SampleRow            `json:"samples,omitempty"`
	Warnings            []string               `json:"warnings,omitempty"`
}

/* FieldInference describes one inferred CSV column. */
type FieldInference struct {
	Name           string         `json:"name"`
	ParquetName    string         `json:"parquet_name"`
	Type           string         `json:"type"`
	Required       bool           `json:"required"`
	Confidence     float64        `json:"confidence"`
	BlankCount     int            `json:"blank_count"`
	NonBlankCount  int            `json:"non_blank_count"`
	MinLength      int            `json:"min_length"`
	MaxLength      int            `json:"max_length"`
	CandidateTypes []string       `json:"candidate_types"`
	SampleValues   []string       `json:"sample_values"`
	TypeCounts     map[string]int `json:"type_counts"`
}

/* SampleRow stores one retained sample record for review or parquet export. */
type SampleRow struct {
	SampleIndex int               `json:"sample_index"`
	OffsetEnd   int64             `json:"offset_end"`
	Values      map[string]string `json:"values"`
}

/* Infer samples a CSV file and infers a GVY validation schema from observed values. */
func Infer(ctx context.Context, inputPath string, opts Options) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	startedAt := time.Now()
	opts = normalizeOptions(opts)

	f, err := os.Open(inputPath)
	if err != nil {
		return Result{}, fmt.Errorf("open input: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return Result{}, fmt.Errorf("stat input: %w", err)
	}
	if info.IsDir() {
		return Result{}, fmt.Errorf("input must be a file")
	}

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		return Result{}, fmt.Errorf("read header: %w", err)
	}
	header = normalizeHeader(header)
	dataStart := reader.InputOffset()

	records, warnings, err := sampleRecords(ctx, inputPath, header, dataStart, info.Size(), opts)
	if err != nil {
		return Result{}, err
	}

	fields := inferFields(header, records)
	schema := schemaFromInference(fields)
	result := Result{
		InputPath:           inputPath,
		FileSizeBytes:       info.Size(),
		DataStartOffset:     dataStart,
		Strategy:            opts.Strategy,
		RequestedSampleSize: opts.SampleSize,
		SampledRows:         len(records),
		DurationMillis:      time.Since(startedAt).Milliseconds(),
		Schema:              schema,
		Fields:              fields,
		Warnings:            warnings,
	}
	if opts.KeepSamples {
		result.Samples = sampleRows(header, records)
	}
	if opts.Strategy == StrategyByteSpread {
		result.Warnings = append(result.Warnings, "byte-spread sampling avoids a full row count, so sample positions are approximate byte positions rather than exact row numbers")
		result.Warnings = append(result.Warnings, "byte-spread sampling assumes common one-record-per-line CSV data; use head strategy for CSV files with embedded newlines in quoted fields")
	}
	result.Warnings = append(result.Warnings, "required fields are inferred only from sampled rows and should be reviewed before saving a production schema")
	return result, nil
}

func normalizeOptions(opts Options) Options {
	if opts.SampleSize < 1 {
		opts.SampleSize = defaultSampleSize
	}
	if opts.Strategy == "" {
		opts.Strategy = StrategyByteSpread
	}
	if opts.MaxScanRecords < 1 {
		opts.MaxScanRecords = defaultMaxScanRows
	}
	return opts
}

func sampleRecords(ctx context.Context, inputPath string, header []string, dataStart, fileSize int64, opts Options) ([]sampleRecord, []string, error) {
	switch opts.Strategy {
	case StrategyHead:
		records, err := sampleHead(ctx, inputPath, header, opts.SampleSize)
		return records, nil, err
	case StrategyByteSpread:
		return sampleByteSpread(ctx, inputPath, header, dataStart, fileSize, opts.SampleSize, opts.MaxScanRecords)
	default:
		return nil, nil, fmt.Errorf("unsupported inference strategy %q", opts.Strategy)
	}
}

type sampleRecord struct {
	offsetEnd int64
	values    []string
}

func sampleHead(ctx context.Context, inputPath string, header []string, sampleSize int) ([]sampleRecord, error) {
	f, err := os.Open(inputPath)
	if err != nil {
		return nil, fmt.Errorf("open input: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1
	if _, err := reader.Read(); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	records := make([]sampleRecord, 0, sampleSize)
	for len(records) < sampleSize {
		if err := ctx.Err(); err != nil {
			return records, err
		}
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return records, fmt.Errorf("read sample row: %w", err)
		}
		records = append(records, sampleRecord{
			offsetEnd: reader.InputOffset(),
			values:    normalizeRecord(record, len(header)),
		})
	}
	return records, nil
}

func sampleByteSpread(ctx context.Context, inputPath string, header []string, dataStart, fileSize int64, sampleSize, maxScanRecords int) ([]sampleRecord, []string, error) {
	if fileSize <= dataStart {
		return nil, nil, nil
	}

	f, err := os.Open(inputPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open input: %w", err)
	}
	defer f.Close()

	targets := spreadTargets(dataStart, fileSize, sampleSize)
	records := make([]sampleRecord, 0, len(targets))
	warnings := make([]string, 0)
	seenOffsets := make(map[int64]struct{}, len(targets))

	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return records, warnings, err
		}
		record, err := readRecordNearOffset(f, target, dataStart, len(header), maxScanRecords)
		if errors.Is(err, io.EOF) {
			continue
		}
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("skipped sample near byte %d: %v", target, err))
			continue
		}
		if _, ok := seenOffsets[record.offsetEnd]; ok {
			continue
		}
		seenOffsets[record.offsetEnd] = struct{}{}
		records = append(records, record)
	}
	if len(records) < sampleSize {
		backfilled, err := sampleHeadSkippingOffsets(ctx, inputPath, header, sampleSize-len(records), seenOffsets)
		if err != nil {
			return records, warnings, err
		}
		records = append(records, backfilled...)
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].offsetEnd < records[j].offsetEnd
	})
	return records, warnings, nil
}

func sampleHeadSkippingOffsets(ctx context.Context, inputPath string, header []string, needed int, seenOffsets map[int64]struct{}) ([]sampleRecord, error) {
	if needed <= 0 {
		return nil, nil
	}
	f, err := os.Open(inputPath)
	if err != nil {
		return nil, fmt.Errorf("open input: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1
	if _, err := reader.Read(); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	records := make([]sampleRecord, 0, needed)
	for len(records) < needed {
		if err := ctx.Err(); err != nil {
			return records, err
		}
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return records, fmt.Errorf("read sample row: %w", err)
		}
		offsetEnd := reader.InputOffset()
		if _, ok := seenOffsets[offsetEnd]; ok {
			continue
		}
		seenOffsets[offsetEnd] = struct{}{}
		records = append(records, sampleRecord{
			offsetEnd: offsetEnd,
			values:    normalizeRecord(record, len(header)),
		})
	}
	return records, nil
}

func spreadTargets(start, end int64, sampleSize int) []int64 {
	if sampleSize == 1 || end-start <= 1 {
		return []int64{start}
	}
	targets := make([]int64, 0, sampleSize)
	span := end - start - 1
	for i := 0; i < sampleSize; i++ {
		offset := start + int64(math.Round(float64(span)*float64(i)/float64(sampleSize-1)))
		if offset >= end {
			offset = end - 1
		}
		targets = append(targets, offset)
	}
	return targets
}

func readRecordNearOffset(f *os.File, target, dataStart int64, headerLen, maxScanRecords int) (sampleRecord, error) {
	if _, err := f.Seek(target, io.SeekStart); err != nil {
		return sampleRecord{}, fmt.Errorf("seek: %w", err)
	}

	readerStart := target
	buffered := bufio.NewReaderSize(f, 64*1024)
	if target > dataStart {
		discarded, err := buffered.ReadString('\n')
		readerStart += int64(len(discarded))
		if err != nil && !errors.Is(err, io.EOF) {
			return sampleRecord{}, fmt.Errorf("resync: %w", err)
		}
		if errors.Is(err, io.EOF) {
			return sampleRecord{}, io.EOF
		}
	}

	reader := csv.NewReader(buffered)
	reader.FieldsPerRecord = -1
	var lastErr error
	for i := 0; i < maxScanRecords; i++ {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return sampleRecord{}, io.EOF
		}
		if err != nil {
			lastErr = err
			continue
		}
		return sampleRecord{
			offsetEnd: readerStart + reader.InputOffset(),
			values:    normalizeRecord(record, headerLen),
		}, nil
	}
	if lastErr != nil {
		return sampleRecord{}, lastErr
	}
	return sampleRecord{}, fmt.Errorf("no parseable record found")
}

func normalizeHeader(header []string) []string {
	out := make([]string, len(header))
	seen := map[string]int{}
	for i, name := range header {
		clean := strings.TrimSpace(name)
		if clean == "" {
			clean = fmt.Sprintf("column_%d", i+1)
		}
		seen[clean]++
		if seen[clean] > 1 {
			clean = fmt.Sprintf("%s_%d", clean, seen[clean])
		}
		out[i] = clean
	}
	return out
}

func normalizeRecord(record []string, headerLen int) []string {
	out := make([]string, headerLen)
	copy(out, record)
	return out
}

func inferFields(header []string, records []sampleRecord) []FieldInference {
	stats := make([]*fieldStats, len(header))
	for i, name := range header {
		stats[i] = newFieldStats(name)
	}

	for _, record := range records {
		for i := range header {
			stats[i].observe(record.values[i])
		}
	}

	fields := make([]FieldInference, 0, len(header))
	for _, stat := range stats {
		fields = append(fields, stat.inference())
	}
	return fields
}

type fieldStats struct {
	name        string
	blanks      int
	nonBlanks   int
	minLen      int
	maxLen      int
	allInt      bool
	allFloat    bool
	allDate     bool
	allDateTime bool
	samples     []string
	seenSample  map[string]struct{}
	typeCounts  map[string]int
}

func newFieldStats(name string) *fieldStats {
	return &fieldStats{
		name:        name,
		allInt:      true,
		allFloat:    true,
		allDate:     true,
		allDateTime: true,
		seenSample:  map[string]struct{}{},
		typeCounts:  map[string]int{"int": 0, "float": 0, "date": 0, "datetime": 0, "string": 0},
	}
}

func (s *fieldStats) observe(raw string) {
	value := strings.TrimSpace(raw)
	if value == "" {
		s.blanks++
		return
	}
	s.nonBlanks++
	valueLen := len(value)
	if s.minLen == 0 || valueLen < s.minLen {
		s.minLen = valueLen
	}
	if valueLen > s.maxLen {
		s.maxLen = valueLen
	}
	if _, ok := s.seenSample[value]; !ok && len(s.samples) < maxSampleValues {
		s.seenSample[value] = struct{}{}
		s.samples = append(s.samples, value)
	}

	isInt := parseInt(value)
	isFloat := parseFloat(value)
	isDate := parseDate(value)
	isDateTime := parseDateTime(value)
	if isInt {
		s.typeCounts["int"]++
	}
	if isFloat {
		s.typeCounts["float"]++
	}
	if isDate {
		s.typeCounts["date"]++
	}
	if isDateTime {
		s.typeCounts["datetime"]++
	}
	if !isInt && !isFloat && !isDate && !isDateTime {
		s.typeCounts["string"]++
	}
	s.allInt = s.allInt && isInt
	s.allFloat = s.allFloat && isFloat
	s.allDate = s.allDate && isDate
	s.allDateTime = s.allDateTime && isDateTime
}

func (s *fieldStats) inference() FieldInference {
	inferredType := "string"
	switch {
	case s.nonBlanks == 0:
		inferredType = "string"
	case s.allInt:
		inferredType = "int"
	case s.allFloat:
		inferredType = "float"
	case s.allDate:
		inferredType = "date"
	case s.allDateTime:
		inferredType = "datetime"
	}

	confidence := 0.0
	if s.nonBlanks > 0 {
		if inferredType == "string" {
			confidence = float64(s.typeCounts["string"]) / float64(s.nonBlanks)
		} else {
			confidence = float64(s.typeCounts[inferredType]) / float64(s.nonBlanks)
		}
	}

	return FieldInference{
		Name:           s.name,
		ParquetName:    toSnakeCase(s.name),
		Type:           inferredType,
		Required:       s.nonBlanks > 0 && s.blanks == 0,
		Confidence:     confidence,
		BlankCount:     s.blanks,
		NonBlankCount:  s.nonBlanks,
		MinLength:      s.minLen,
		MaxLength:      s.maxLen,
		CandidateTypes: s.candidateTypes(),
		SampleValues:   append([]string(nil), s.samples...),
		TypeCounts:     cloneTypeCounts(s.typeCounts),
	}
}

func (s *fieldStats) candidateTypes() []string {
	candidates := make([]string, 0, 5)
	if s.nonBlanks == 0 {
		return []string{"string"}
	}
	if s.allInt {
		candidates = append(candidates, "int")
	}
	if s.allFloat {
		candidates = append(candidates, "float")
	}
	if s.allDate {
		candidates = append(candidates, "date")
	}
	if s.allDateTime {
		candidates = append(candidates, "datetime")
	}
	candidates = append(candidates, "string")
	return candidates
}

func cloneTypeCounts(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func schemaFromInference(fields []FieldInference) validator.SchemaConfig {
	schema := validator.SchemaConfig{Fields: make([]validator.FieldRule, 0, len(fields))}
	for _, field := range fields {
		rule := validator.FieldRule{
			Name:        field.Name,
			ParquetName: field.ParquetName,
			Type:        field.Type,
			Required:    field.Required,
		}
		if field.Type == "date" {
			rule.DateFormats = append([]string(nil), dateLayouts...)
		}
		if field.Type == "datetime" {
			rule.DatetimeFormats = append([]string(nil), datetimeLayouts...)
		}
		schema.Fields = append(schema.Fields, rule)
	}
	return schema
}

func sampleRows(header []string, records []sampleRecord) []SampleRow {
	rows := make([]SampleRow, 0, len(records))
	for i, record := range records {
		values := make(map[string]string, len(header))
		for col, name := range header {
			values[name] = record.values[col]
		}
		rows = append(rows, SampleRow{
			SampleIndex: i + 1,
			OffsetEnd:   record.offsetEnd,
			Values:      values,
		})
	}
	return rows
}

func parseInt(value string) bool {
	if strings.ContainsAny(value, ".eE") {
		return false
	}
	_, err := strconv.ParseInt(value, 10, 64)
	return err == nil
}

func parseFloat(value string) bool {
	parsed, err := strconv.ParseFloat(value, 64)
	return err == nil && !math.IsNaN(parsed) && !math.IsInf(parsed, 0)
}

func parseDate(value string) bool {
	return parseTimeWithLayouts(value, dateLayouts)
}

func parseDateTime(value string) bool {
	return parseTimeWithLayouts(value, datetimeLayouts)
}

func parseTimeWithLayouts(value string, layouts []string) bool {
	for _, layout := range layouts {
		if _, err := time.Parse(layout, value); err == nil {
			return true
		}
	}
	return false
}

func toSnakeCase(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	replacer := strings.NewReplacer(" ", "_", "-", "_", "/", "_", ".", "_")
	s = replacer.Replace(s)
	for strings.Contains(s, "__") {
		s = strings.ReplaceAll(s, "__", "_")
	}
	return strings.Trim(s, "_")
}
