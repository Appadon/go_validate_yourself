package inputrows

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
)

const parquetReadChunkSize = 2048

/* Source streams CSV or Parquet rows as header-aligned string records. */
type Source interface {
	Header() []string
	Next() ([]string, int, error)
	Close() error
	BytesRead() int64
	InputSize() int64
}

/* Open opens a supported flat CSV or Parquet source. */
func Open(path string) (Source, error) {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".csv":
		return openCSVRows(path)
	case ".parquet":
		return openParquetRows(path)
	default:
		return nil, fmt.Errorf("input must use .csv or .parquet extension")
	}
}

/* HeaderIndex builds a trimmed-name index for a source header. */
func HeaderIndex(header []string) map[string]int {
	headerIdx := make(map[string]int, len(header))
	for i, h := range header {
		headerIdx[strings.TrimSpace(h)] = i
	}
	return headerIdx
}

/* IsSupportedExtension reports whether ext is a validation/split source extension. */
func IsSupportedExtension(ext string) bool {
	return strings.EqualFold(ext, ".csv") || strings.EqualFold(ext, ".parquet")
}

type csvRowSource struct {
	file      *os.File
	reader    *csv.Reader
	header    []string
	rowNum    int
	counter   *countingReader
	inputSize int64
}

func openCSVRows(path string) (*csvRowSource, error) {
	in, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open input: %w", err)
	}
	stat, err := in.Stat()
	if err != nil {
		_ = in.Close()
		return nil, fmt.Errorf("stat input: %w", err)
	}

	counter := &countingReader{r: in}
	csvReader := csv.NewReader(counter)
	csvReader.FieldsPerRecord = -1
	header, err := csvReader.Read()
	if err != nil {
		_ = in.Close()
		return nil, fmt.Errorf("read header: %w", err)
	}
	return &csvRowSource{
		file:      in,
		reader:    csvReader,
		header:    header,
		rowNum:    1,
		counter:   counter,
		inputSize: stat.Size(),
	}, nil
}

func (s *csvRowSource) Header() []string {
	return s.header
}

func (s *csvRowSource) Next() ([]string, int, error) {
	s.rowNum++
	record, err := s.reader.Read()
	if err != nil {
		return nil, s.rowNum, err
	}
	return record, s.rowNum, nil
}

func (s *csvRowSource) Close() error {
	return s.file.Close()
}

func (s *csvRowSource) BytesRead() int64 {
	return s.counter.bytesRead.Load()
}

func (s *csvRowSource) InputSize() int64 {
	return s.inputSize
}

type parquetColumn struct {
	exName  string
	inName  string
	element *parquet.SchemaElement
}

type parquetRowSource struct {
	file      interface{ Close() error }
	reader    *reader.ParquetReader
	header    []string
	columns   []parquetColumn
	remaining int64
	totalRows int64
	inputSize int64
	buffer    []interface{}
	bufferPos int
	rowNum    int
}

func openParquetRows(path string) (*parquetRowSource, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat input: %w", err)
	}
	file, err := local.NewLocalFileReader(path)
	if err != nil {
		return nil, fmt.Errorf("open input parquet: %w", err)
	}

	parquetReader, err := reader.NewParquetReader(file, nil, 2)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("new parquet reader: %w", err)
	}
	columns, err := parquetFlatColumns(parquetReader)
	if err != nil {
		parquetReader.ReadStop()
		_ = file.Close()
		return nil, err
	}
	header := make([]string, 0, len(columns))
	for _, column := range columns {
		header = append(header, column.exName)
	}
	totalRows := parquetReader.GetNumRows()
	return &parquetRowSource{
		file:      file,
		reader:    parquetReader,
		header:    header,
		columns:   columns,
		remaining: totalRows,
		totalRows: totalRows,
		inputSize: stat.Size(),
	}, nil
}

func parquetFlatColumns(parquetReader *reader.ParquetReader) ([]parquetColumn, error) {
	columns := make([]parquetColumn, 0, len(parquetReader.SchemaHandler.ValueColumns))
	for i, element := range parquetReader.SchemaHandler.SchemaElements {
		if element.GetNumChildren() != 0 {
			continue
		}
		path := parquetReader.SchemaHandler.IndexMap[int32(i)]
		if len(common.StrToPath(path)) != 2 {
			return nil, fmt.Errorf("nested parquet columns are not supported for input: %s", strings.Join(common.StrToPath(path)[1:], "."))
		}
		info := parquetReader.SchemaHandler.Infos[i]
		exName := strings.TrimSpace(info.ExName)
		if exName == "" {
			exName = strings.TrimSpace(info.InName)
		}
		if exName == "" {
			return nil, fmt.Errorf("parquet column at index %d has no name", i)
		}
		columns = append(columns, parquetColumn{
			exName:  exName,
			inName:  info.InName,
			element: element,
		})
	}
	if len(columns) == 0 && parquetReader.GetNumRows() > 0 {
		return nil, fmt.Errorf("parquet input has no readable columns")
	}
	return columns, nil
}

func (s *parquetRowSource) Header() []string {
	return s.header
}

func (s *parquetRowSource) Next() ([]string, int, error) {
	if s.bufferPos >= len(s.buffer) {
		if s.remaining <= 0 {
			return nil, s.rowNum + 1, io.EOF
		}
		toRead := parquetReadChunkSize
		if int64(toRead) > s.remaining {
			toRead = int(s.remaining)
		}
		rows, err := s.reader.ReadByNumber(toRead)
		if err != nil {
			return nil, s.rowNum + 1, err
		}
		if len(rows) == 0 {
			s.remaining = 0
			return nil, s.rowNum + 1, io.EOF
		}
		s.buffer = rows
		s.bufferPos = 0
		s.remaining -= int64(len(rows))
	}

	row := s.buffer[s.bufferPos]
	s.bufferPos++
	s.rowNum++
	record, err := parquetRecord(row, s.columns)
	if err != nil {
		return nil, s.rowNum, err
	}
	return record, s.rowNum, nil
}

func (s *parquetRowSource) Close() error {
	s.reader.ReadStop()
	return s.file.Close()
}

func (s *parquetRowSource) BytesRead() int64 {
	if s.totalRows <= 0 {
		return s.inputSize
	}
	read := s.inputSize * int64(s.rowNum) / s.totalRows
	if read > s.inputSize {
		return s.inputSize
	}
	return read
}

func (s *parquetRowSource) InputSize() int64 {
	return s.inputSize
}

func parquetRecord(row interface{}, columns []parquetColumn) ([]string, error) {
	record := make([]string, 0, len(columns))
	for _, column := range columns {
		value, ok := reflectFieldValue(row, column.inName)
		if !ok {
			return nil, fmt.Errorf("read parquet column %q", column.exName)
		}
		record = append(record, parquetValueString(value, column.element))
	}
	return record, nil
}

func reflectFieldValue(row interface{}, name string) (interface{}, bool) {
	value := reflect.ValueOf(row)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, true
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return nil, false
	}
	field := value.FieldByName(name)
	if !field.IsValid() {
		return nil, false
	}
	for field.Kind() == reflect.Pointer {
		if field.IsNil() {
			return nil, true
		}
		field = field.Elem()
	}
	return field.Interface(), true
}

func parquetValueString(value interface{}, element *parquet.SchemaElement) string {
	if value == nil {
		return ""
	}
	if isParquetDate(element) {
		if days, ok := int64Value(value); ok {
			return time.Unix(days*86400, 0).UTC().Format("2006-01-02")
		}
	}
	if scale, ok := parquetTimestampScale(element); ok {
		if timestamp, ok := int64Value(value); ok {
			return time.Unix(0, timestamp*scale).UTC().Format(time.RFC3339Nano)
		}
	}
	return fmt.Sprint(value)
}

func isParquetDate(element *parquet.SchemaElement) bool {
	if element == nil {
		return false
	}
	if element.LogicalType != nil && element.LogicalType.DATE != nil {
		return true
	}
	return element.ConvertedType != nil && *element.ConvertedType == parquet.ConvertedType_DATE
}

func parquetTimestampScale(element *parquet.SchemaElement) (int64, bool) {
	if element == nil {
		return 0, false
	}
	if element.LogicalType != nil && element.LogicalType.TIMESTAMP != nil {
		unit := element.LogicalType.TIMESTAMP.Unit
		switch {
		case unit != nil && unit.MILLIS != nil:
			return int64(time.Millisecond), true
		case unit != nil && unit.MICROS != nil:
			return int64(time.Microsecond), true
		case unit != nil && unit.NANOS != nil:
			return int64(time.Nanosecond), true
		}
	}
	if element.ConvertedType != nil {
		switch *element.ConvertedType {
		case parquet.ConvertedType_TIMESTAMP_MILLIS:
			return int64(time.Millisecond), true
		case parquet.ConvertedType_TIMESTAMP_MICROS:
			return int64(time.Microsecond), true
		}
	}
	return 0, false
}

func int64Value(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		if v > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(v), true
	default:
		return 0, false
	}
}

type countingReader struct {
	r         io.Reader
	bytesRead atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.bytesRead.Add(int64(n))
	}
	return n, err
}
