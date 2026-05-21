package schemaeditor

import (
	"encoding/json"
	"fmt"
	"os"

	"go_validate_yourself/internal/validator"
)

/* Document is the editable validation schema document used by the schema editor. */
type Document = validator.SchemaConfig

/* Load reads and validates a schema document from disk. */
func Load(path string) (Document, error) {
	schema, err := validator.LoadSchema(path)
	if err != nil {
		return Document{}, fmt.Errorf("load schema: %w", err)
	}
	if err := validator.ValidateSchema(&schema); err != nil {
		return Document{}, fmt.Errorf("validate schema: %w", err)
	}
	return schema, nil
}

/* Marshal validates and formats a schema document as stable JSON. */
func Marshal(schema Document) ([]byte, error) {
	if err := validator.ValidateSchema(&schema); err != nil {
		return nil, fmt.Errorf("validate schema: %w", err)
	}
	data, err := json.MarshalIndent(toSchemaFile(schema), "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal schema: %w", err)
	}
	return append(data, '\n'), nil
}

/* Save validates and writes a schema document to disk. */
func Save(path string, schema Document) error {
	data, err := Marshal(schema)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write schema: %w", err)
	}
	return nil
}

type schemaFile struct {
	Fields []schemaField `json:"fields"`
}

type schemaField struct {
	Name             string            `json:"name"`
	ParquetName      string            `json:"parquet_name,omitempty"`
	Type             string            `json:"type"`
	Required         bool              `json:"required,omitempty"`
	ExcludeIfMissing bool              `json:"exclude_if_missing,omitempty"`
	MinLength        int               `json:"min_length,omitempty"`
	Lower            bool              `json:"lower,omitempty"`
	AllowedValues    []string          `json:"allowed_values,omitempty"`
	InlineReplace    map[string]string `json:"inline_replace,omitempty"`
	Default          interface{}       `json:"default,omitempty"`
	Override         interface{}       `json:"override,omitempty"`
	NonZero          bool              `json:"non_zero,omitempty"`
	DateFormats      []string          `json:"date_formats,omitempty"`
	DatetimeFormats  []string          `json:"datetime_formats,omitempty"`
	MinDate          string            `json:"min_date,omitempty"`
	MaxDate          string            `json:"max_date,omitempty"`
}

func toSchemaFile(schema Document) schemaFile {
	out := schemaFile{Fields: make([]schemaField, 0, len(schema.Fields))}
	for _, field := range schema.Fields {
		out.Fields = append(out.Fields, schemaField{
			Name:             field.Name,
			ParquetName:      field.ParquetName,
			Type:             field.Type,
			Required:         field.Required,
			ExcludeIfMissing: field.ExcludeIfMissing,
			MinLength:        field.MinLength,
			Lower:            field.Lower,
			AllowedValues:    field.AllowedValues,
			InlineReplace:    field.InlineReplace,
			Default:          field.Default,
			Override:         field.Override,
			NonZero:          field.NonZero,
			DateFormats:      field.DateFormats,
			DatetimeFormats:  field.DatetimeFormats,
			MinDate:          field.MinDate,
			MaxDate:          field.MaxDate,
		})
	}
	return out
}
