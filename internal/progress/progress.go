package progress

import (
	"fmt"
	"time"
)

const (
	PhaseRun      = "run"
	PhaseSplit    = "split"
	PhaseValidate = "validate"
	PhaseBatch    = "batch"
)

const (
	TypeStarted   = "started"
	TypeProgress  = "progress"
	TypeCompleted = "completed"
	TypeFailed    = "failed"
	TypeLog       = "log"
	TypeTelemetry = "telemetry"
)

/* Event captures one structured progress update for a validation run. */
type Event struct {
	RunID   string         `json:"run_id"`
	Time    time.Time      `json:"time"`
	Phase   string         `json:"phase"`
	Type    string         `json:"type"`
	Message string         `json:"message,omitempty"`
	Percent *float64       `json:"percent,omitempty"`
	Metrics map[string]any `json:"metrics,omitempty"`
}

/* Reporter receives structured progress events. */
type Reporter interface {
	Report(Event)
}

/* FuncReporter adapts a function into a Reporter. */
type FuncReporter func(Event)

/* Report emits one event through the wrapped function. */
func (f FuncReporter) Report(event Event) {
	f(event)
}

/* Emitter binds a reporter to one run id and clock source. */
type Emitter struct {
	runID    string
	reporter Reporter
	now      func() time.Time
}

/* NewEmitter creates a run-scoped event emitter. */
func NewEmitter(runID string, reporter Reporter) Emitter {
	if runID == "" {
		runID = NewRunID()
	}
	if reporter == nil {
		reporter = FuncReporter(func(Event) {})
	}
	return Emitter{
		runID:    runID,
		reporter: reporter,
		now:      time.Now().UTC,
	}
}

/* NewRunID returns a simple unique identifier for one execution. */
func NewRunID() string {
	return fmt.Sprintf("run-%d", time.Now().UTC().UnixNano())
}

/* RunID returns the emitter's bound run identifier. */
func (e Emitter) RunID() string {
	return e.runID
}

/* Emit reports one structured event after filling common fields. */
func (e Emitter) Emit(phase, eventType, message string, percent *float64, metrics map[string]any) {
	if e.reporter == nil {
		return
	}
	e.reporter.Report(Event{
		RunID:   e.runID,
		Time:    e.now(),
		Phase:   phase,
		Type:    eventType,
		Message: message,
		Percent: percent,
		Metrics: cloneMetrics(metrics),
	})
}

/* Started emits a started event. */
func (e Emitter) Started(phase, message string, metrics map[string]any) {
	e.Emit(phase, TypeStarted, message, nil, metrics)
}

/* Progress emits an in-flight progress event. */
func (e Emitter) Progress(phase string, percent float64, metrics map[string]any) {
	pct := percent
	e.Emit(phase, TypeProgress, "", &pct, metrics)
}

/* Completed emits a completed event. */
func (e Emitter) Completed(phase, message string, metrics map[string]any) {
	e.Emit(phase, TypeCompleted, message, nil, metrics)
}

/* Failed emits a failed event. */
func (e Emitter) Failed(phase, message string, metrics map[string]any) {
	e.Emit(phase, TypeFailed, message, nil, metrics)
}

/* Log emits a non-terminal factual log event. */
func (e Emitter) Log(phase, message string, metrics map[string]any) {
	e.Emit(phase, TypeLog, message, nil, metrics)
}

func cloneMetrics(metrics map[string]any) map[string]any {
	if len(metrics) == 0 {
		return nil
	}
	out := make(map[string]any, len(metrics))
	for key, value := range metrics {
		out[key] = value
	}
	return out
}
