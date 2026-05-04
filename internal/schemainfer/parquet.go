package schemainfer

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

/* WriteSamplesParquet writes retained sample rows as an all-string parquet preview file. */
func WriteSamplesParquet(outputPath string, result Result) error {
	if strings.TrimSpace(outputPath) == "" {
		return fmt.Errorf("sample parquet output path is required")
	}
	if len(result.Samples) == 0 {
		return fmt.Errorf("no retained samples to write")
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return fmt.Errorf("create sample parquet directory: %w", err)
	}

	pw, err := local.NewLocalFileWriter(outputPath)
	if err != nil {
		return fmt.Errorf("create sample parquet: %w", err)
	}
	writeComplete := false
	defer func() {
		_ = pw.Close()
		if !writeComplete {
			_ = os.Remove(outputPath)
		}
	}()

	metadata := sampleParquetMetadata(result.Fields)
	parquetWriter, err := writer.NewCSVWriter(metadata, pw, 2)
	if err != nil {
		return fmt.Errorf("new sample parquet writer: %w", err)
	}
	parquetWriter.RowGroupSize = 16 * 1024 * 1024
	parquetWriter.CompressionType = 1

	for _, sample := range result.Samples {
		row := make([]*string, 0, len(result.Fields))
		for _, field := range result.Fields {
			value := sample.Values[field.Name]
			row = append(row, &value)
		}
		if err := parquetWriter.WriteString(row); err != nil {
			return fmt.Errorf("write sample parquet row %d: %w", sample.SampleIndex, err)
		}
	}
	if err := parquetWriter.WriteStop(); err != nil {
		return fmt.Errorf("close sample parquet writer: %w", err)
	}
	writeComplete = true
	return nil
}

func sampleParquetMetadata(fields []FieldInference) []string {
	metadata := make([]string, 0, len(fields))
	seen := map[string]int{}
	for i, field := range fields {
		name := field.ParquetName
		if name == "" {
			name = fmt.Sprintf("column_%d", i+1)
		}
		seen[name]++
		if seen[name] > 1 {
			name = fmt.Sprintf("%s_%d", name, seen[name])
		}
		metadata = append(metadata, "name="+name+", repetitiontype=OPTIONAL, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY")
	}
	return metadata
}
