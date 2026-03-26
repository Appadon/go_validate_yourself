package batchparquet

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go_validate_yourself/internal/console"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

const readChunkSize = 2048

/* Summary captures the high-level result of a batch parquet run. */
type Summary struct {
	InputFiles int
	Batches    int
	BatchSize  int
	Workers    int
	TotalRows  int64
	OutputDir  string
}

/* ListParquetFiles returns sorted parquet files from one directory. */
func ListParquetFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.EqualFold(filepath.Ext(name), ".parquet") {
			files = append(files, filepath.Join(dir, name))
		}
	}
	sort.Strings(files)
	return files, nil
}

/* BatchDirectory combines parquet files into fixed-size file batches. */
func BatchDirectory(inputDir, outputDir string, batchSize int, workers int) (Summary, error) {
	if strings.TrimSpace(inputDir) == "" {
		return Summary{}, fmt.Errorf("missing batch input directory")
	}
	if strings.TrimSpace(outputDir) == "" {
		return Summary{}, fmt.Errorf("missing batch export directory")
	}
	if batchSize < 1 {
		batchSize = 1
	}
	if workers < 1 {
		workers = 1
	}

	inputAbs, err := filepath.Abs(inputDir)
	if err != nil {
		return Summary{}, fmt.Errorf("resolve batch input dir: %w", err)
	}
	outputAbs, err := filepath.Abs(outputDir)
	if err != nil {
		return Summary{}, fmt.Errorf("resolve batch export dir: %w", err)
	}
	if inputAbs == outputAbs {
		return Summary{}, fmt.Errorf("batch export directory must differ from batch input directory")
	}

	files, err := ListParquetFiles(inputDir)
	if err != nil {
		return Summary{}, fmt.Errorf("list parquet files: %w", err)
	}
	if len(files) == 0 {
		return Summary{}, fmt.Errorf("no parquet files found in directory: %s", inputDir)
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return Summary{}, fmt.Errorf("create batch export dir: %w", err)
	}

	totalBatches := (len(files) + batchSize - 1) / batchSize
	console.Infof(
		"starting batch phase [files %s] [batch_size %s] [workers %s] [expected_batches %s] [input_dir %s] [output_dir %s]",
		console.GreenValue(strconv.Itoa(len(files))),
		console.GreenValue(strconv.Itoa(batchSize)),
		console.GreenValue(strconv.Itoa(workers)),
		console.GreenValue(strconv.Itoa(totalBatches)),
		console.GreenValue(inputDir),
		console.GreenValue(outputDir),
	)

	var completedFiles atomic.Int64
	startedAt := time.Now()
	doneProgress := startBatchProgressReporter(&completedFiles, len(files), startedAt)
	defer close(doneProgress)

	summary := Summary{
		InputFiles: len(files),
		BatchSize:  batchSize,
		Workers:    workers,
		OutputDir:  outputDir,
	}

	jobs := make(chan batchJob)
	results := make(chan batchResult, workers*2)
	for i := 0; i < workers; i++ {
		go batchWorker(jobs, results, func() {
			completedFiles.Add(1)
		})
	}

	batchJobs := make([]batchJob, 0, totalBatches)
	for start, batchNumber := 0, 1; start < len(files); start, batchNumber = start+batchSize, batchNumber+1 {
		end := start + batchSize
		if end > len(files) {
			end = len(files)
		}
		batchFiles := append([]string(nil), files[start:end]...)
		outputPath := filepath.Join(outputDir, fmt.Sprintf("validation_batch_%d.parquet", batchNumber))
		batchJobs = append(batchJobs, batchJob{
			number:     batchNumber,
			files:      batchFiles,
			outputPath: outputPath,
		})
	}
	expected := len(batchJobs)
	go func() {
		for _, job := range batchJobs {
			jobs <- job
		}
		close(jobs)
	}()

	var firstErr error
	for i := 0; i < expected; i++ {
		r := <-results
		if r.err != nil && firstErr == nil {
			firstErr = fmt.Errorf("write batch %d: %w", r.number, r.err)
		}
		if r.err != nil {
			continue
		}
		summary.TotalRows += r.rowsWritten
		summary.Batches++
	}

	printBatchFinalProgress(completedFiles.Load(), len(files))
	if firstErr != nil {
		return summary, firstErr
	}
	return summary, nil
}

type batchJob struct {
	number     int
	files      []string
	outputPath string
}

/* batchResult stores one worker result for aggregation in the coordinator. */
type batchResult struct {
	number      int
	rowsWritten int64
	err         error
}

/* batchWorker processes one batch job at a time and returns write results. */
func batchWorker(jobs <-chan batchJob, results chan<- batchResult, onFileDone func()) {
	for job := range jobs {
		rowsWritten, err := writeBatchParquet(job.files, job.outputPath, onFileDone)
		results <- batchResult{
			number:      job.number,
			rowsWritten: rowsWritten,
			err:         err,
		}
	}
}

/* writeBatchParquet merges a set of parquet files into one output parquet file. */
func writeBatchParquet(batchFiles []string, outputPath string, onFileDone func()) (int64, error) {
	var totalRows int64
	var outWriter *writer.ParquetWriter
	var outFile source.ParquetFile
	var schemaSignature string
	writeComplete := false

	defer func() {
		if outFile != nil {
			_ = outFile.Close()
		}
		if !writeComplete {
			_ = os.Remove(outputPath)
		}
	}()

	for _, filePath := range batchFiles {
		if err := validateBatchInputFile(filePath); err != nil {
			return totalRows, err
		}

		inFile, err := local.NewLocalFileReader(filePath)
		if err != nil {
			return totalRows, fmt.Errorf("open input parquet %q: %w", filePath, err)
		}

		inReader, err := reader.NewParquetReader(inFile, nil, 2)
		if err != nil {
			_ = inFile.Close()
			return totalRows, fmt.Errorf("create reader for %q: %w", filePath, err)
		}

		normalizedSchema := schemaWithExternalNames(inReader.Footer.Schema, inReader.SchemaHandler.Infos)
		currentSignature, err := schemaFingerprint(normalizedSchema)
		if err != nil {
			inReader.ReadStop()
			_ = inFile.Close()
			return totalRows, fmt.Errorf("serialize schema %q: %w", filePath, err)
		}

		if outWriter == nil {
			outFile, err = local.NewLocalFileWriter(outputPath)
			if err != nil {
				inReader.ReadStop()
				_ = inFile.Close()
				return totalRows, fmt.Errorf("create batch output %q: %w", outputPath, err)
			}

			outWriter, err = writer.NewParquetWriter(outFile, normalizedSchema, 2)
			if err != nil {
				inReader.ReadStop()
				_ = inFile.Close()
				return totalRows, fmt.Errorf("create output writer %q: %w", outputPath, err)
			}
			schemaSignature = currentSignature
		} else if schemaSignature != currentSignature {
			inReader.ReadStop()
			_ = inFile.Close()
			return totalRows, fmt.Errorf("schema mismatch for %q", filePath)
		}

		remaining := inReader.GetNumRows()
		for remaining > 0 {
			toRead := readChunkSize
			if int64(toRead) > remaining {
				toRead = int(remaining)
			}
			rows, err := inReader.ReadByNumber(toRead)
			if err != nil {
				inReader.ReadStop()
				_ = inFile.Close()
				return totalRows, fmt.Errorf("read rows from %q: %w", filePath, err)
			}
			if len(rows) == 0 {
				break
			}
			for _, row := range rows {
				if err := outWriter.Write(row); err != nil {
					inReader.ReadStop()
					_ = inFile.Close()
					return totalRows, fmt.Errorf("write row to batch output: %w", err)
				}
			}
			remaining -= int64(len(rows))
			totalRows += int64(len(rows))
		}

		inReader.ReadStop()
		_ = inFile.Close()
		if onFileDone != nil {
			onFileDone()
		}
	}

	if outWriter == nil {
		return 0, fmt.Errorf("no parquet data found in batch")
	}
	if err := outWriter.WriteStop(); err != nil {
		return totalRows, fmt.Errorf("finalize batch parquet: %w", err)
	}
	writeComplete = true
	return totalRows, nil
}

/* validateBatchInputFile ensures batch inputs are normal seekable parquet files. */
func validateBatchInputFile(filePath string) error {
	info, err := os.Lstat(filePath)
	if err != nil {
		return fmt.Errorf("stat input parquet %q: %w", filePath, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("input parquet %q is a symlink, not a regular file", filePath)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("input parquet %q is not a regular file (mode=%s)", filePath, info.Mode().String())
	}
	if info.Size() < 8 {
		return fmt.Errorf("input parquet %q is too small to contain a parquet footer (size=%d)", filePath, info.Size())
	}
	return nil
}

/* schemaFingerprint creates a stable schema identity for schema-compat checks. */
func schemaFingerprint(schemaList []*parquet.SchemaElement) (string, error) {
	payload, err := json.Marshal(schemaList)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

/* schemaWithExternalNames rewrites schema element names to parquet external names. */
func schemaWithExternalNames(schemaList []*parquet.SchemaElement, infos []*common.Tag) []*parquet.SchemaElement {
	out := make([]*parquet.SchemaElement, 0, len(schemaList))
	for i, se := range schemaList {
		if se == nil {
			out = append(out, nil)
			continue
		}
		clone := *se
		if i < len(infos) && infos[i] != nil && strings.TrimSpace(infos[i].ExName) != "" {
			clone.Name = infos[i].ExName
		}
		out = append(out, &clone)
	}
	return out
}

/* startBatchProgressReporter launches periodic batch progress logging. */
func startBatchProgressReporter(completed *atomic.Int64, total int, startedAt time.Time) chan struct{} {
	done := make(chan struct{})
	go reportBatchProgress(done, completed, total, startedAt)
	return done
}

/* reportBatchProgress prints standardized progress snapshots until done is closed. */
func reportBatchProgress(done <-chan struct{}, completed *atomic.Int64, total int, startedAt time.Time) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			finished := completed.Load()
			pct := batchPercent(finished, total)
			elapsed := time.Since(startedAt)
			rate := batchRate(finished, elapsed)
			eta := batchETA(total-int(finished), rate)
			console.Progressf(console.ProgressSnapshot{
				Segments: []string{
					fmt.Sprintf("%d/%d files", finished, total),
					fmt.Sprintf("%.2f%%", pct),
					fmt.Sprintf("%.2f files/s", rate),
					fmt.Sprintf("eta %s", eta),
					fmt.Sprintf("elapsed %s", console.FormatDuration(elapsed)),
				},
			})
		case <-done:
			return
		}
	}
}

/* batchPercent returns completion percentage for parquet batching. */
func batchPercent(completed int64, total int) float64 {
	if total == 0 {
		return 100
	}
	return float64(completed) * 100.0 / float64(total)
}

/* batchRate returns processed parquet files per second. */
func batchRate(completed int64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(completed) / elapsed.Seconds()
}

/* batchETA estimates remaining batch duration from progress rate. */
func batchETA(remaining int, rate float64) string {
	if rate <= 0 || remaining < 0 {
		return "unknown"
	}
	return console.FormatDuration(time.Duration(float64(remaining)/rate) * time.Second)
}

/* printBatchFinalProgress prints a terminal 100% progress snapshot. */
func printBatchFinalProgress(completed int64, total int) {
	pct := batchPercent(completed, total)
	console.Progressf(console.ProgressSnapshot{
		Segments: []string{
			fmt.Sprintf("%d/%d files", completed, total),
			fmt.Sprintf("%.2f%%", pct),
			"0.00 files/s",
			"eta 0s",
			"elapsed done",
		},
	})
}
