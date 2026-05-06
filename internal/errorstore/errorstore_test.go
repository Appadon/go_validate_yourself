package errorstore

import (
	"path/filepath"
	"testing"
)

func TestWriteAndScanErrorRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input_error.parquet")
	if err := Write(path, []string{"Policy Number", "Name"}, []InvalidRow{
		{
			RowNumber: 2,
			Record:    []string{"P-1", "A"},
			Errors: []FieldError{
				{Field: "Name", Message: "min length 3"},
			},
		},
	}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	var rows []StoredErrorRow
	if err := Scan(path, func(row StoredErrorRow) error {
		rows = append(rows, row)
		return nil
	}); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if len(rows) != 1 || rows[0].RowNumber != 2 {
		t.Fatalf("unexpected rows: %+v", rows)
	}
	columns, err := DecodeColumns(rows[0])
	if err != nil {
		t.Fatalf("DecodeColumns() error = %v", err)
	}
	if len(columns) != 2 || columns[1].Name != "Name" || columns[1].Value != "A" {
		t.Fatalf("unexpected columns: %+v", columns)
	}
}

func TestWriteEmptyErrorFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input_error.parquet")
	if err := Write(path, []string{"Policy Number"}, nil); err != nil {
		t.Fatalf("Write(empty) error = %v", err)
	}

	rows := 0
	if err := Scan(path, func(row StoredErrorRow) error {
		rows++
		return nil
	}); err != nil {
		t.Fatalf("Scan(empty) error = %v", err)
	}
	if rows != 0 {
		t.Fatalf("rows = %d, want 0", rows)
	}
}
