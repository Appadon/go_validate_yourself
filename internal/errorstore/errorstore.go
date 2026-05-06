package errorstore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

const (
	RowNumberColumn   = "__row_number"
	ErrorsColumn      = "__errors"
	ErrorFieldsColumn = "__error_fields"
	RowValuesColumn   = "__row_values"
	SearchTextColumn  = "__search_text"
)

const readChunkSize = 2048

/* FieldError stores one field-level validation failure. */
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

/* RowColumn preserves original source column order for row samples. */
type RowColumn struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

/* InvalidRow is the storage-layer representation of one rejected source row. */
type InvalidRow struct {
	RowNumber int
	Record    []string
	Errors    []FieldError
}

/* StoredErrorRow is the fixed Parquet schema used for invalid-row files. */
type StoredErrorRow struct {
	RowNumber       int64  `parquet:"name=__row_number, type=INT64"`
	Errors          string `parquet:"name=__errors, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ErrorFieldsJSON string `parquet:"name=__error_fields, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	RowValuesJSON   string `parquet:"name=__row_values, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	SearchText      string `parquet:"name=__search_text, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
}

/* Write persists invalid rows to a fixed-schema Parquet file. */
func Write(path string, header []string, rows []InvalidRow) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("error parquet output path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create error parquet directory: %w", err)
	}

	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		return fmt.Errorf("create error parquet: %w", err)
	}
	writeComplete := false
	defer func() {
		_ = fw.Close()
		if !writeComplete {
			_ = os.Remove(path)
		}
	}()

	pw, err := writer.NewParquetWriter(fw, new(StoredErrorRow), 4)
	if err != nil {
		return fmt.Errorf("new error parquet writer: %w", err)
	}
	pw.RowGroupSize = 32 * 1024 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, row := range rows {
		stored, err := encodeRow(header, row)
		if err != nil {
			return err
		}
		if err := pw.Write(stored); err != nil {
			return fmt.Errorf("write error parquet row %d: %w", row.RowNumber, err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("close error parquet writer: %w", err)
	}
	writeComplete = true
	return nil
}

/* Scan streams one Parquet error file in chunks. */
func Scan(path string, visit func(StoredErrorRow) error) error {
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		return fmt.Errorf("open error parquet: %w", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(StoredErrorRow), 2)
	if err != nil {
		return fmt.Errorf("new error parquet reader: %w", err)
	}
	defer pr.ReadStop()

	remaining := int(pr.GetNumRows())
	for remaining > 0 {
		chunk := readChunkSize
		if remaining < chunk {
			chunk = remaining
		}
		rows, err := pr.ReadByNumber(chunk)
		if err != nil {
			return fmt.Errorf("read error parquet rows: %w", err)
		}
		for _, item := range rows {
			row, ok := item.(StoredErrorRow)
			if !ok {
				return fmt.Errorf("unexpected error parquet row type %T", item)
			}
			if err := visit(row); err != nil {
				return err
			}
		}
		remaining -= len(rows)
		if len(rows) == 0 {
			break
		}
	}
	return nil
}

/* DecodeColumns returns source row columns in original order. */
func DecodeColumns(row StoredErrorRow) ([]RowColumn, error) {
	if strings.TrimSpace(row.RowValuesJSON) == "" {
		return nil, nil
	}
	var columns []RowColumn
	if err := json.Unmarshal([]byte(row.RowValuesJSON), &columns); err != nil {
		return nil, err
	}
	return columns, nil
}

/* DecodeErrorFields returns the de-duplicated field names written for fast filtering. */
func DecodeErrorFields(row StoredErrorRow) ([]string, error) {
	if strings.TrimSpace(row.ErrorFieldsJSON) == "" {
		return nil, nil
	}
	var fields []string
	if err := json.Unmarshal([]byte(row.ErrorFieldsJSON), &fields); err != nil {
		return nil, err
	}
	return fields, nil
}

/* FormatErrors serializes field errors in the legacy human-readable form. */
func FormatErrors(errs []FieldError) string {
	parts := make([]string, 0, len(errs))
	for _, e := range errs {
		parts = append(parts, fmt.Sprintf("%s: %s", e.Field, e.Message))
	}
	return strings.Join(parts, " | ")
}

func encodeRow(header []string, row InvalidRow) (StoredErrorRow, error) {
	columns := make([]RowColumn, 0, len(header))
	searchParts := make([]string, 0, len(header)+len(row.Errors)+2)
	for index, name := range header {
		value := ""
		if index < len(row.Record) {
			value = row.Record[index]
		}
		columns = append(columns, RowColumn{Name: name, Value: value})
		searchParts = append(searchParts, name, value)
	}

	fields := uniqueErrorFields(row.Errors)
	rowValuesJSON, err := json.Marshal(columns)
	if err != nil {
		return StoredErrorRow{}, fmt.Errorf("encode row values for row %d: %w", row.RowNumber, err)
	}
	errorFieldsJSON, err := json.Marshal(fields)
	if err != nil {
		return StoredErrorRow{}, fmt.Errorf("encode error fields for row %d: %w", row.RowNumber, err)
	}
	errorsText := FormatErrors(row.Errors)
	searchParts = append(searchParts, strconv.Itoa(row.RowNumber), errorsText)
	for _, field := range fields {
		searchParts = append(searchParts, field)
	}

	return StoredErrorRow{
		RowNumber:       int64(row.RowNumber),
		Errors:          errorsText,
		ErrorFieldsJSON: string(errorFieldsJSON),
		RowValuesJSON:   string(rowValuesJSON),
		SearchText:      strings.Join(searchParts, "\n"),
	}, nil
}

func uniqueErrorFields(errs []FieldError) []string {
	seen := make(map[string]struct{}, len(errs))
	fields := make([]string, 0, len(errs))
	for _, err := range errs {
		field := strings.TrimSpace(err.Field)
		if field == "" {
			continue
		}
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		fields = append(fields, field)
	}
	return fields
}
