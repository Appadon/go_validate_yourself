package validator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go_validate_yourself/internal/errorstore"
	"go_validate_yourself/internal/progress"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

func TestValidateRowOverrideReplacesInputValue(t *testing.T) {
	schema := SchemaConfig{
		Fields: []FieldRule{
			{
				Name:      "customer_id",
				Type:      "string",
				Required:  true,
				MinLength: 1,
				Override:  "default_text",
			},
		},
	}
	if err := ValidateSchema(&schema); err != nil {
		t.Fatalf("ValidateSchema() error = %v", err)
	}

	out, errs := validateRow(2, []string{"original_value"}, map[string]int{"customer_id": 0}, schema)
	if len(errs) > 0 {
		t.Fatalf("validateRow() unexpected errors = %+v", errs)
	}
	if len(out) != 1 || out[0] == nil || *out[0] != "default_text" {
		t.Fatalf("validateRow() override mismatch, got %#v", out)
	}
}

func TestValidateSchemaFieldsAgainstHeaderAllowsOverrideWithoutSourceColumn(t *testing.T) {
	schema := SchemaConfig{
		Fields: []FieldRule{
			{
				Name:     "customer_id",
				Type:     "string",
				Required: true,
				Override: "default_text",
			},
		},
	}
	if err := ValidateSchema(&schema); err != nil {
		t.Fatalf("ValidateSchema() error = %v", err)
	}

	if err := validateSchemaFieldsAgainstHeader(schema, map[string]int{}); err != nil {
		t.Fatalf("validateSchemaFieldsAgainstHeader() error = %v", err)
	}
}

func TestValidateRowOverrideStillValidatesNormalizedValue(t *testing.T) {
	schema := SchemaConfig{
		Fields: []FieldRule{
			{
				Name:          "status",
				Type:          "string",
				Required:      true,
				Lower:         true,
				AllowedValues: []string{"active"},
				Override:      "ACTIVE",
			},
		},
	}
	if err := ValidateSchema(&schema); err != nil {
		t.Fatalf("ValidateSchema() error = %v", err)
	}

	out, errs := validateRow(2, []string{"inactive"}, map[string]int{"status": 0}, schema)
	if len(errs) > 0 {
		t.Fatalf("validateRow() unexpected errors = %+v", errs)
	}
	if len(out) != 1 || out[0] == nil || *out[0] != "active" {
		t.Fatalf("validateRow() normalized override mismatch, got %#v", out)
	}
}

func TestValidateSchemaDatetimeDefaults(t *testing.T) {
	schema := SchemaConfig{
		Fields: []FieldRule{
			{
				Name:     "created_at",
				Type:     "datetime",
				Required: true,
			},
		},
	}

	if err := ValidateSchema(&schema); err != nil {
		t.Fatalf("ValidateSchema() error = %v", err)
	}
	if len(schema.Fields[0].DatetimeFormats) == 0 {
		t.Fatal("ValidateSchema() expected default datetime formats")
	}
}

func TestNormalizeAndValidateValueDatetime(t *testing.T) {
	field := FieldRule{
		Name:            "created_at",
		Type:            "datetime",
		DatetimeFormats: []string{"2006-01-02 15:04:05"},
	}

	got, err := normalizeAndValidateValue("2026-03-26 14:30:00", field)
	if err != nil {
		t.Fatalf("normalizeAndValidateValue() error = %v", err)
	}
	if got == nil {
		t.Fatal("normalizeAndValidateValue() returned nil for datetime")
	}
	want := "1774535400000000"
	if *got != want {
		t.Fatalf("normalizeAndValidateValue() = %s, want %s", *got, want)
	}
}

func TestNormalizeAndValidateValueDateRejectsDefaultRange(t *testing.T) {
	field := FieldRule{
		Name:        "event_date",
		Type:        "date",
		DateFormats: []string{"2006-01-02"},
	}

	for _, raw := range []string{"1899-12-31", "2101-01-01"} {
		if _, err := normalizeAndValidateValue(raw, field); err == nil || !strings.Contains(err.Error(), "date outside range 1900-01-01 to 2100-12-31") {
			t.Fatalf("normalizeAndValidateValue(%q) error = %v, want default range error", raw, err)
		}
	}
}

func TestNormalizeAndValidateValueDateUsesCustomRange(t *testing.T) {
	schema := SchemaConfig{
		Fields: []FieldRule{
			{
				Name:        "event_date",
				Type:        "date",
				DateFormats: []string{"2006-01-02"},
				MinDate:     "2020-01-01",
				MaxDate:     "2020-12-31",
			},
		},
	}
	if err := ValidateSchema(&schema); err != nil {
		t.Fatalf("ValidateSchema() error = %v", err)
	}

	if _, err := normalizeAndValidateValue("2020-06-30", schema.Fields[0]); err != nil {
		t.Fatalf("normalizeAndValidateValue() in range error = %v", err)
	}
	if _, err := normalizeAndValidateValue("2021-01-01", schema.Fields[0]); err == nil || !strings.Contains(err.Error(), "date outside range 2020-01-01 to 2020-12-31") {
		t.Fatalf("normalizeAndValidateValue() error = %v, want custom range error", err)
	}
}

func TestValidateSchemaRejectsInvalidDateRange(t *testing.T) {
	schema := SchemaConfig{
		Fields: []FieldRule{
			{
				Name:    "event_date",
				Type:    "date",
				MinDate: "2021-01-01",
				MaxDate: "2020-12-31",
			},
		},
	}

	if err := ValidateSchema(&schema); err == nil || !strings.Contains(err.Error(), "min_date after max_date") {
		t.Fatalf("ValidateSchema() error = %v, want date range order error", err)
	}
}

func TestBuildParquetSchemaMetadataDatetime(t *testing.T) {
	schema := SchemaConfig{
		Fields: []FieldRule{
			{
				Name:        "created_at",
				ParquetName: "created_at",
				Type:        "datetime",
				Required:    true,
			},
		},
	}

	got, err := buildParquetSchemaMetadata(schema)
	if err != nil {
		t.Fatalf("buildParquetSchemaMetadata() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("buildParquetSchemaMetadata() len = %d, want 1", len(got))
	}
	want := "name=created_at, repetitiontype=REQUIRED, type=INT64, convertedtype=TIMESTAMP_MICROS, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MICROS"
	if got[0] != want {
		t.Fatalf("buildParquetSchemaMetadata() = %q, want %q", got[0], want)
	}
}

func TestProcessDirectoryReturnsContextCanceled(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.csv")
	successDir := filepath.Join(dir, "success")
	errorDir := filepath.Join(dir, "errors")

	if err := os.WriteFile(input, []byte("Record ID,Amount\n1,10\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(input) error = %v", err)
	}
	if err := os.MkdirAll(successDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(success) error = %v", err)
	}
	if err := os.MkdirAll(errorDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(errors) error = %v", err)
	}

	schema := SchemaConfig{
		Fields: []FieldRule{
			{Name: "Record ID", Type: "string", Required: true},
			{Name: "Amount", Type: "int", Required: true},
		},
	}
	if err := ValidateSchema(&schema); err != nil {
		t.Fatalf("ValidateSchema() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := ProcessDirectory(ctx, []string{input}, 1, successDir, errorDir, schema, false, progress.Emitter{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ProcessDirectory() error = %v, want context canceled", err)
	}
}

func TestRunValidationWritesErrorParquet(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.csv")
	successDir := filepath.Join(dir, "success")
	errorDir := filepath.Join(dir, "errors")
	successPath := filepath.Join(successDir, "input.parquet")
	errorPath := filepath.Join(errorDir, "input_error.parquet")

	if err := os.MkdirAll(successDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(success) error = %v", err)
	}
	if err := os.MkdirAll(errorDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(errors) error = %v", err)
	}
	if err := os.WriteFile(input, []byte("Record ID,Amount\n1,10\n2,not-number\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(input) error = %v", err)
	}

	schema := SchemaConfig{
		Fields: []FieldRule{
			{Name: "Record ID", Type: "string", Required: true},
			{Name: "Amount", Type: "int", Required: true},
		},
	}
	if err := ValidateSchema(&schema); err != nil {
		t.Fatalf("ValidateSchema() error = %v", err)
	}

	stats, err := RunValidationAndWriteParquet(context.Background(), input, successPath, errorPath, schema, false)
	if err != nil {
		t.Fatalf("RunValidationAndWriteParquet() error = %v", err)
	}
	if stats.InvalidRows != 1 {
		t.Fatalf("InvalidRows = %d, want 1", stats.InvalidRows)
	}

	var rows []errorstore.StoredErrorRow
	if err := errorstore.Scan(errorPath, func(row errorstore.StoredErrorRow) error {
		rows = append(rows, row)
		return nil
	}); err != nil {
		t.Fatalf("Scan(error parquet) error = %v", err)
	}
	if len(rows) != 1 || rows[0].RowNumber != 3 {
		t.Fatalf("unexpected stored rows: %+v", rows)
	}
	columns, err := errorstore.DecodeColumns(rows[0])
	if err != nil {
		t.Fatalf("DecodeColumns() error = %v", err)
	}
	if len(columns) != 2 || columns[1].Name != "Amount" || columns[1].Value != "not-number" {
		t.Fatalf("unexpected stored columns: %+v", columns)
	}
}

func TestRunValidationReadsParquetInputUsingParquetNames(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.parquet")
	successDir := filepath.Join(dir, "success")
	errorDir := filepath.Join(dir, "errors")
	successPath := filepath.Join(successDir, "input.parquet")
	errorPath := filepath.Join(errorDir, "input_error.parquet")

	if err := os.MkdirAll(successDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(success) error = %v", err)
	}
	if err := os.MkdirAll(errorDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(errors) error = %v", err)
	}
	writeStringParquet(t, input, []string{"record_id", "amount"}, [][]string{
		{"1", "10"},
		{"2", "not-number"},
	})

	schema := SchemaConfig{
		Fields: []FieldRule{
			{Name: "Record ID", ParquetName: "record_id", Type: "string", Required: true},
			{Name: "Amount", ParquetName: "amount", Type: "int", Required: true},
		},
	}
	if err := ValidateSchema(&schema); err != nil {
		t.Fatalf("ValidateSchema() error = %v", err)
	}

	stats, err := RunValidationAndWriteParquet(context.Background(), input, successPath, errorPath, schema, false)
	if err != nil {
		t.Fatalf("RunValidationAndWriteParquet() error = %v", err)
	}
	if stats.TotalRows != 2 || stats.ValidRows != 1 || stats.InvalidRows != 1 {
		t.Fatalf("Stats = %+v, want total=2 valid=1 invalid=1", stats)
	}

	var rows []errorstore.StoredErrorRow
	if err := errorstore.Scan(errorPath, func(row errorstore.StoredErrorRow) error {
		rows = append(rows, row)
		return nil
	}); err != nil {
		t.Fatalf("Scan(error parquet) error = %v", err)
	}
	if len(rows) != 1 || rows[0].RowNumber != 2 {
		t.Fatalf("unexpected stored rows: %+v", rows)
	}
	columns, err := errorstore.DecodeColumns(rows[0])
	if err != nil {
		t.Fatalf("DecodeColumns() error = %v", err)
	}
	if len(columns) != 2 || columns[0].Name != "record_id" || columns[1].Value != "not-number" {
		t.Fatalf("unexpected stored columns: %+v", columns)
	}
}

func TestRunValidationReadsTypedParquetOutput(t *testing.T) {
	dir := t.TempDir()
	csvInput := filepath.Join(dir, "input.csv")
	firstSuccessDir := filepath.Join(dir, "first-success")
	firstErrorDir := filepath.Join(dir, "first-errors")
	secondSuccessDir := filepath.Join(dir, "second-success")
	secondErrorDir := filepath.Join(dir, "second-errors")
	firstParquet := filepath.Join(firstSuccessDir, "input.parquet")
	firstError := filepath.Join(firstErrorDir, "input_error.parquet")
	secondParquet := filepath.Join(secondSuccessDir, "input.parquet")
	secondError := filepath.Join(secondErrorDir, "input_error.parquet")

	for _, path := range []string{firstSuccessDir, firstErrorDir, secondSuccessDir, secondErrorDir} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s) error = %v", path, err)
		}
	}
	if err := os.WriteFile(csvInput, []byte("Record ID,Event Date,Created At\n1,2026-03-26,2026-03-26 14:30:00\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(input) error = %v", err)
	}

	schema := SchemaConfig{
		Fields: []FieldRule{
			{Name: "Record ID", ParquetName: "record_id", Type: "string", Required: true},
			{Name: "Event Date", ParquetName: "event_date", Type: "date", Required: true},
			{Name: "Created At", ParquetName: "created_at", Type: "datetime", Required: true},
		},
	}
	if err := ValidateSchema(&schema); err != nil {
		t.Fatalf("ValidateSchema() error = %v", err)
	}

	if _, err := RunValidationAndWriteParquet(context.Background(), csvInput, firstParquet, firstError, schema, false); err != nil {
		t.Fatalf("first RunValidationAndWriteParquet() error = %v", err)
	}
	stats, err := RunValidationAndWriteParquet(context.Background(), firstParquet, secondParquet, secondError, schema, false)
	if err != nil {
		t.Fatalf("second RunValidationAndWriteParquet() error = %v", err)
	}
	if stats.TotalRows != 1 || stats.ValidRows != 1 || stats.InvalidRows != 0 {
		t.Fatalf("Stats = %+v, want total=1 valid=1 invalid=0", stats)
	}
}

func TestListCSVFilesIncludesParquetInputs(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"b.parquet", "a.csv", "ignore.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", name, err)
		}
	}

	files, err := ListCSVFiles(dir)
	if err != nil {
		t.Fatalf("ListCSVFiles() error = %v", err)
	}
	if len(files) != 2 || filepath.Base(files[0]) != "a.csv" || filepath.Base(files[1]) != "b.parquet" {
		t.Fatalf("ListCSVFiles() = %+v", files)
	}
}

func writeStringParquet(t *testing.T, path string, columns []string, rows [][]string) {
	t.Helper()

	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		t.Fatalf("NewLocalFileWriter() error = %v", err)
	}
	defer fw.Close()

	metadata := make([]string, 0, len(columns))
	for _, column := range columns {
		metadata = append(metadata, fmt.Sprintf("name=%s, repetitiontype=OPTIONAL, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY", column))
	}
	pw, err := writer.NewCSVWriter(metadata, fw, 2)
	if err != nil {
		t.Fatalf("NewCSVWriter() error = %v", err)
	}
	for _, row := range rows {
		values := make([]*string, 0, len(row))
		for i := range row {
			value := row[i]
			values = append(values, &value)
		}
		if err := pw.WriteString(values); err != nil {
			t.Fatalf("WriteString() error = %v", err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		t.Fatalf("WriteStop() error = %v", err)
	}
}
