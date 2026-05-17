package splitcsv

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"go_validate_yourself/internal/inputrows"
	"go_validate_yourself/internal/progress"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

func TestSplitByPrimaryKeyWritesParquetOutputForCSVInput(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.csv")
	outputDir := filepath.Join(dir, "split")
	writeTestFile(t, input, "Record ID,Amount\n1,10\n2,20\n1,30\n")

	summary, err := SplitByPrimaryKey(context.Background(), Config{
		InputPath:       input,
		OutputDir:       outputDir,
		PrimaryKey:      "Record ID",
		MaxOpenWriters:  1,
		MissingKeysFile: "missing_keys.csv",
		Progress:        progress.Emitter{},
	})
	if err != nil {
		t.Fatalf("SplitByPrimaryKey() error = %v", err)
	}
	if summary.TotalRows != 3 || summary.SplitRows != 3 || summary.MissingKeyRows != 0 || summary.OutputFiles != 2 {
		t.Fatalf("Summary = %+v", summary)
	}

	rows := readParquetRows(t, filepath.Join(outputDir, "1.parquet"))
	if len(rows) != 3 || rows[0][0] != "Record ID" || rows[1][1] != "10" || rows[2][1] != "30" {
		t.Fatalf("unexpected split rows: %+v", rows)
	}
	if _, err := os.Stat(filepath.Join(outputDir, "1.csv")); !os.IsNotExist(err) {
		t.Fatalf("split CSV output exists after parquet split: %v", err)
	}
}

func TestSplitByPrimaryKeyReadsParquetInput(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.parquet")
	outputDir := filepath.Join(dir, "split")
	writeStringParquet(t, input, []string{"policy_number", "amount"}, [][]string{
		{"A123", "10"},
		{"", "20"},
		{"A123", "30"},
	})

	summary, err := SplitByPrimaryKey(context.Background(), Config{
		InputPath:       input,
		OutputDir:       outputDir,
		PrimaryKey:      "policy_number",
		MaxOpenWriters:  2,
		MissingKeysFile: "missing_keys.csv",
		Progress:        progress.Emitter{},
	})
	if err != nil {
		t.Fatalf("SplitByPrimaryKey() error = %v", err)
	}
	if summary.TotalRows != 3 || summary.SplitRows != 2 || summary.MissingKeyRows != 1 || summary.OutputFiles != 1 {
		t.Fatalf("Summary = %+v", summary)
	}

	rows := readParquetRows(t, filepath.Join(outputDir, "A123.parquet"))
	if len(rows) != 3 || rows[0][0] != "policy_number" || rows[1][1] != "10" || rows[2][1] != "30" {
		t.Fatalf("unexpected split rows: %+v", rows)
	}
	missingRows := readParquetRows(t, filepath.Join(outputDir, "missing_keys.parquet"))
	if len(missingRows) != 2 || missingRows[1][1] != "20" {
		t.Fatalf("unexpected missing rows: %+v", missingRows)
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

func readParquetRows(t *testing.T, path string) [][]string {
	t.Helper()

	source, err := inputrows.Open(path)
	if err != nil {
		t.Fatalf("inputrows.Open(%s) error = %v", path, err)
	}
	defer source.Close()

	rows := [][]string{source.Header()}
	for {
		record, _, err := source.Next()
		if err != nil {
			if err == io.EOF {
				return rows
			}
			t.Fatalf("Next(%s) error = %v", path, err)
		}
		rows = append(rows, record)
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", path, err)
	}
}
