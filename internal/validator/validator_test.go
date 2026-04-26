package validator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"go_validate_yourself/internal/progress"
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
