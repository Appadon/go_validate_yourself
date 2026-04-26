package batchparquet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"go_validate_yourself/internal/progress"

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
func BatchDirectory(ctx context.Context, inputDir, outputDir string, batchSize int, workers int, emitter progress.Emitter) (Summary, error) {
	if ctx == nil {
		ctx = context.Background()
	}
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
	emitter.Started(progress.PhaseBatch, fmt.Sprintf("starting batch phase [files %d] [batch_size %d] [workers %d] [expected_batches %d] [input_dir %s] [output_dir %s]", len(files), batchSize, workers, totalBatches, inputDir, outputDir), map[string]any{
		"files_total":      len(files),
		"batch_size":       batchSize,
		"workers":          workers,
		"expected_batches": totalBatches,
		"input_dir":        inputDir,
		"output_dir":       outputDir,
	})
	var completedFiles atomic.Int64
	var completedBatches atomic.Int64
	var totalRowsWritten atomic.Int64
	startedAt := time.Now()
	doneProgress := startBatchProgressReporter(ctx, &completedFiles, &completedBatches, &totalRowsWritten, len(files), totalBatches, startedAt, emitter)
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
		go batchWorker(ctx, jobs, results, func() {
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
		defer close(jobs)
		for _, job := range batchJobs {
			select {
			case <-ctx.Done():
				return
			case jobs <- job:
			}
		}
	}()

	var firstErr error
	for i := 0; i < expected; i++ {
		select {
		case <-ctx.Done():
			return summary, ctx.Err()
		case r := <-results:
			if r.err != nil && firstErr == nil {
				firstErr = fmt.Errorf("write batch %d: %w", r.number, r.err)
			}
			if r.err != nil {
				continue
			}
			summary.TotalRows += r.rowsWritten
			totalRowsWritten.Add(r.rowsWritten)
			summary.Batches++
			completedBatches.Add(1)
		}
	}

	printBatchFinalProgress(completedFiles.Load(), len(files), completedBatches.Load(), totalBatches, totalRowsWritten.Load(), startedAt, emitter)
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
func batchWorker(ctx context.Context, jobs <-chan batchJob, results chan<- batchResult, onFileDone func()) {
	for job := range jobs {
		rowsWritten, err := writeBatchParquet(ctx, job.files, job.outputPath, onFileDone)
		results <- batchResult{
			number:      job.number,
			rowsWritten: rowsWritten,
			err:         err,
		}
	}
}

/* writeBatchParquet merges a set of parquet files into one output parquet file. */
func writeBatchParquet(ctx context.Context, batchFiles []string, outputPath string, onFileDone func()) (int64, error) {
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
		if err := ctx.Err(); err != nil {
			return totalRows, err
		}
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
			if err := ctx.Err(); err != nil {
				inReader.ReadStop()
				_ = inFile.Close()
				return totalRows, err
			}
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
				if err := ctx.Err(); err != nil {
					inReader.ReadStop()
					_ = inFile.Close()
					return totalRows, err
				}
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
	if err := ctx.Err(); err != nil {
		return totalRows, err
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
func startBatchProgressReporter(ctx context.Context, completedFiles, completedBatches, rowsWritten *atomic.Int64, totalFiles, totalBatches int, startedAt time.Time, emitter progress.Emitter) chan struct{} {
	done := make(chan struct{})
	go reportBatchProgress(ctx, done, completedFiles, completedBatches, rowsWritten, totalFiles, totalBatches, startedAt, emitter)
	return done
}

/* reportBatchProgress prints standardized progress snapshots until done is closed. */
func reportBatchProgress(ctx context.Context, done <-chan struct{}, completedFiles, completedBatches, rowsWritten *atomic.Int64, totalFiles, totalBatches int, startedAt time.Time, emitter progress.Emitter) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			finished := completedFiles.Load()
			pct := batchPercent(finished, totalFiles)
			elapsed := time.Since(startedAt)
			rate := batchRate(finished, elapsed)
			emitter.Progress(progress.PhaseBatch, pct, map[string]any{
				"files_completed":   finished,
				"files_total":       totalFiles,
				"files_per_sec":     rate,
				"batches_completed": completedBatches.Load(),
				"batches_total":     totalBatches,
				"rows_written":      rowsWritten.Load(),
				"eta_seconds":       batchETASeconds(totalFiles-int(finished), rate),
				"elapsed_seconds":   elapsed.Seconds(),
			})
		case <-done:
			return
		case <-ctx.Done():
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
func batchETASeconds(remaining int, rate float64) float64 {
	if rate <= 0 || remaining < 0 {
		return -1
	}
	return float64(remaining) / rate
}

/* printBatchFinalProgress prints a terminal 100% progress snapshot. */
func printBatchFinalProgress(completedFiles int64, totalFiles int, completedBatches int64, totalBatches int, rowsWritten int64, startedAt time.Time, emitter progress.Emitter) {
	emitter.Progress(progress.PhaseBatch, batchPercent(completedFiles, totalFiles), map[string]any{
		"files_completed":   completedFiles,
		"files_total":       totalFiles,
		"files_per_sec":     0.0,
		"batches_completed": completedBatches,
		"batches_total":     totalBatches,
		"rows_written":      rowsWritten,
		"eta_seconds":       0.0,
		"elapsed_seconds":   time.Since(startedAt).Seconds(),
	})
}
