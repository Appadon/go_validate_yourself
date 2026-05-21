package schemaeditor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go_validate_yourself/internal/validator"
)

func TestSaveFormatsAndValidatesSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "schema.json")

	err := Save(path, Document{
		Fields: []validator.FieldRule{
			{Name: "Record ID", Type: "string", Required: true},
			{Name: "Amount", Type: "float"},
		},
	})
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !json.Valid(data) {
		t.Fatalf("saved schema is not valid JSON: %s", string(data))
	}
	if !strings.Contains(string(data), `"parquet_name": "record_id"`) {
		t.Fatalf("saved schema did not normalize parquet name: %s", string(data))
	}
}

func TestMarshalIncludesDateRange(t *testing.T) {
	data, err := Marshal(Document{
		Fields: []validator.FieldRule{
			{Name: "Event Date", Type: "date", MinDate: "2020-01-01", MaxDate: "2020-12-31"},
		},
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	text := string(data)
	if !strings.Contains(text, `"min_date": "2020-01-01"`) || !strings.Contains(text, `"max_date": "2020-12-31"`) {
		t.Fatalf("saved schema did not include date range: %s", text)
	}
}

func TestMarshalRejectsInvalidSchema(t *testing.T) {
	_, err := Marshal(Document{})
	if err == nil {
		t.Fatal("Marshal() expected validation error")
	}
	if !strings.Contains(err.Error(), "schema.fields cannot be empty") {
		t.Fatalf("Marshal() error = %v", err)
	}
}
