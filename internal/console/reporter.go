package console

import (
	"fmt"
	"time"

	"go_validate_yourself/internal/progress"
)

/* ProgressReporter formats structured events back into terminal-friendly output. */
type ProgressReporter struct{}

/* NewProgressReporter returns a console-backed structured event reporter. */
func NewProgressReporter() ProgressReporter {
	return ProgressReporter{}
}

/* Report renders one structured progress event to the console. */
func (ProgressReporter) Report(event progress.Event) {
	switch event.Type {
	case progress.TypeProgress:
		reportProgressEvent(event)
	case progress.TypeStarted:
		if event.Phase == progress.PhaseRun {
			return
		}
		Infof("%s", event.Message)
	case progress.TypeCompleted:
		if event.Phase == progress.PhaseRun {
			return
		}
		Successf("%s", event.Message)
	case progress.TypeFailed:
		return
	case progress.TypeLog:
		Infof("%s", logMessage(event))
	}
}

func reportProgressEvent(event progress.Event) {
	switch event.Phase {
	case progress.PhaseSplit:
		Progressf(ProgressSnapshot{Segments: splitProgressSegments(event)})
	case progress.PhaseValidate:
		Progressf(ProgressSnapshot{Segments: validateProgressSegments(event)})
	case progress.PhaseBatch:
		Progressf(ProgressSnapshot{Segments: batchProgressSegments(event)})
	}
}

func splitProgressSegments(event progress.Event) []string {
	readMiB, _ := metricFloat64(event.Metrics, "read_mib")
	inputMiB, _ := metricFloat64(event.Metrics, "input_mib")
	rowsTotal, _ := metricInt64(event.Metrics, "rows_total")
	rowsEstimated, _ := metricString(event.Metrics, "rows_estimated")
	rowRate, _ := metricFloat64(event.Metrics, "rows_per_sec")
	readBytes, _ := metricInt64(event.Metrics, "read_bytes")
	ioMiBPerSec, _ := metricFloat64(event.Metrics, "io_mib_per_sec")
	missingRows, _ := metricInt64(event.Metrics, "missing_rows")
	etaSeconds, _ := metricFloat64(event.Metrics, "eta_seconds")
	elapsedSeconds, _ := metricFloat64(event.Metrics, "elapsed_seconds")
	return []string{
		fmt.Sprintf("%.2f/%.2f mb", readMiB, inputMiB),
		fmt.Sprintf("%d/%s rows", rowsTotal, rowsEstimated),
		percentString(event.Percent),
		fmt.Sprintf("%.2f rows/s", rowRate),
		fmt.Sprintf("read=%s io=%.2f MiB", FormatBytes(readBytes), ioMiBPerSec),
		fmt.Sprintf("%d missing", missingRows),
		fmt.Sprintf("eta %s", durationLabel(etaSeconds)),
		fmt.Sprintf("elapsed %s", durationLabel(elapsedSeconds)),
	}
}

func validateProgressSegments(event progress.Event) []string {
	filesCompleted, _ := metricInt64(event.Metrics, "files_completed")
	filesTotal, _ := metricInt64(event.Metrics, "files_total")
	filesPerSec, _ := metricFloat64(event.Metrics, "files_per_sec")
	etaSeconds, _ := metricFloat64(event.Metrics, "eta_seconds")
	elapsedSeconds, _ := metricFloat64(event.Metrics, "elapsed_seconds")
	return []string{
		fmt.Sprintf("%d/%d files", filesCompleted, filesTotal),
		percentString(event.Percent),
		fmt.Sprintf("%.2f files/s", filesPerSec),
		fmt.Sprintf("eta %s", durationLabel(etaSeconds)),
		fmt.Sprintf("elapsed %s", validateElapsedLabel(event, elapsedSeconds)),
	}
}

func batchProgressSegments(event progress.Event) []string {
	filesCompleted, _ := metricInt64(event.Metrics, "files_completed")
	filesTotal, _ := metricInt64(event.Metrics, "files_total")
	filesPerSec, _ := metricFloat64(event.Metrics, "files_per_sec")
	etaSeconds, _ := metricFloat64(event.Metrics, "eta_seconds")
	elapsedSeconds, _ := metricFloat64(event.Metrics, "elapsed_seconds")
	batchesCompleted, hasBatchesCompleted := metricInt64(event.Metrics, "batches_completed")
	batchesTotal, hasBatchesTotal := metricInt64(event.Metrics, "batches_total")
	rowsWritten, hasRowsWritten := metricInt64(event.Metrics, "rows_written")

	segments := []string{
		fmt.Sprintf("%d/%d files", filesCompleted, filesTotal),
		percentString(event.Percent),
		fmt.Sprintf("%.2f files/s", filesPerSec),
	}
	if hasBatchesCompleted && hasBatchesTotal {
		segments = append(segments, fmt.Sprintf("%d/%d batches", batchesCompleted, batchesTotal))
	}
	if hasRowsWritten {
		segments = append(segments, fmt.Sprintf("%d rows", rowsWritten))
	}
	segments = append(segments,
		fmt.Sprintf("eta %s", durationLabel(etaSeconds)),
		fmt.Sprintf("elapsed %s", batchElapsedLabel(event, elapsedSeconds)),
	)
	return segments
}

func logMessage(event progress.Event) string {
	if event.Message != "" {
		return event.Message
	}
	return ""
}

func percentString(percent *float64) string {
	if percent == nil {
		return "0.00%"
	}
	return fmt.Sprintf("%.2f%%", *percent)
}

func metricFloat64(metrics map[string]any, key string) (float64, bool) {
	value, ok := metrics[key]
	if !ok {
		return 0, false
	}
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	}
	return 0, false
}

func metricInt64(metrics map[string]any, key string) (int64, bool) {
	value, ok := metrics[key]
	if !ok {
		return 0, false
	}
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int32:
		return int64(typed), true
	case int64:
		return typed, true
	case float64:
		return int64(typed), true
	}
	return 0, false
}

func metricString(metrics map[string]any, key string) (string, bool) {
	value, ok := metrics[key]
	if !ok {
		return "", false
	}
	text, ok := value.(string)
	return text, ok
}

func durationLabel(seconds float64) string {
	if seconds < 0 {
		return "unknown"
	}
	return FormatDuration(time.Duration(seconds * float64(time.Second)))
}

func validateElapsedLabel(event progress.Event, elapsedSeconds float64) string {
	if isCompletedProgress(event) {
		return "done"
	}
	return durationLabel(elapsedSeconds)
}

func batchElapsedLabel(event progress.Event, elapsedSeconds float64) string {
	if isCompletedProgress(event) {
		return "done"
	}
	return durationLabel(elapsedSeconds)
}

func isCompletedProgress(event progress.Event) bool {
	return event.Percent != nil && *event.Percent >= 100
}
