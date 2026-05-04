package schemainfer

import (
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestInferByteSpreadInfersCommonGVYTypes(t *testing.T) {
	path := writeInferenceFixture(t, 240)

	result, err := Infer(context.Background(), path, Options{
		SampleSize:  40,
		Strategy:    StrategyByteSpread,
		KeepSamples: true,
	})
	if err != nil {
		t.Fatalf("Infer() error = %v", err)
	}
	if result.SampledRows == 0 {
		t.Fatalf("expected sampled rows")
	}
	if len(result.Samples) != result.SampledRows {
		t.Fatalf("expected retained samples, got %d samples for %d rows", len(result.Samples), result.SampledRows)
	}

	assertFieldType(t, result, "id", "int")
	assertFieldType(t, result, "amount", "float")
	assertFieldType(t, result, "event_date", "date")
	assertFieldType(t, result, "created_at", "datetime")
	assertFieldType(t, result, "status", "string")

	optional := findField(t, result, "optional_note")
	if optional.Required {
		t.Fatalf("optional_note should not be inferred as required")
	}
	if len(result.Schema.Fields) != 6 {
		t.Fatalf("schema fields = %d, want 6", len(result.Schema.Fields))
	}
}

func TestInferHeadHandlesQuotedNewlines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "quoted.csv")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := csv.NewWriter(f)
	rows := [][]string{
		{"id", "description"},
		{"1", "hello\nworld"},
		{"2", "plain text"},
	}
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			t.Fatal(err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	result, err := Infer(context.Background(), path, Options{
		SampleSize: 2,
		Strategy:   StrategyHead,
	})
	if err != nil {
		t.Fatalf("Infer() error = %v", err)
	}
	assertFieldType(t, result, "id", "int")
	assertFieldType(t, result, "description", "string")
}

func TestWriteSamplesParquet(t *testing.T) {
	path := writeInferenceFixture(t, 20)
	result, err := Infer(context.Background(), path, Options{
		SampleSize:  8,
		Strategy:    StrategyHead,
		KeepSamples: true,
	})
	if err != nil {
		t.Fatalf("Infer() error = %v", err)
	}

	outputPath := filepath.Join(t.TempDir(), "samples.parquet")
	if err := WriteSamplesParquet(outputPath, result); err != nil {
		t.Fatalf("WriteSamplesParquet() error = %v", err)
	}
	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("stat output parquet: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("expected non-empty parquet output")
	}
}

func writeInferenceFixture(t *testing.T, rows int) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "input.csv")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := csv.NewWriter(f)
	if err := writer.Write([]string{"id", "amount", "event_date", "created_at", "status", "optional_note"}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= rows; i++ {
		optional := "present"
		if i%5 == 0 {
			optional = ""
		}
		row := []string{
			intString(i),
			floatString(i),
			"2026-05-04",
			"2026-05-04T12:34:56Z",
			statusValue(i),
			optional,
		}
		if err := writer.Write(row); err != nil {
			t.Fatal(err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertFieldType(t *testing.T, result Result, name, want string) {
	t.Helper()
	field := findField(t, result, name)
	if field.Type != want {
		t.Fatalf("%s type = %s, want %s; counts=%v samples=%v", name, field.Type, want, field.TypeCounts, field.SampleValues)
	}
}

func findField(t *testing.T, result Result, name string) FieldInference {
	t.Helper()
	for _, field := range result.Fields {
		if field.Name == name {
			return field
		}
	}
	t.Fatalf("field %q not found", name)
	return FieldInference{}
}

func intString(i int) string {
	return strconv.Itoa(i)
}

func floatString(i int) string {
	return strconv.FormatFloat(float64(i)+0.25, 'f', 2, 64)
}

func statusValue(i int) string {
	if i%2 == 0 {
		return "active"
	}
	return "pending"
}
