package splitcsv

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go_validate_yourself/internal/progress"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

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

	rows := readCSVRows(t, filepath.Join(outputDir, "A123.csv"))
	if len(rows) != 3 || rows[0][0] != "policy_number" || rows[1][1] != "10" || rows[2][1] != "30" {
		t.Fatalf("unexpected split rows: %+v", rows)
	}
	missingRows := readCSVRows(t, filepath.Join(outputDir, "missing_keys.csv"))
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

func readCSVRows(t *testing.T, path string) [][]string {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open(%s) error = %v", path, err)
	}
	defer file.Close()
	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatalf("ReadAll(%s) error = %v", path, err)
	}
	return rows
}
