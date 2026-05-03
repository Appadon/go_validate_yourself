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

func TestMarshalRejectsInvalidSchema(t *testing.T) {
	_, err := Marshal(Document{})
	if err == nil {
		t.Fatal("Marshal() expected validation error")
	}
	if !strings.Contains(err.Error(), "schema.fields cannot be empty") {
		t.Fatalf("Marshal() error = %v", err)
	}
}
