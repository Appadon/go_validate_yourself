package progress

import (
	"testing"
	"time"
)

func TestNewEmitterFillsCommonEventFields(t *testing.T) {
	var events []Event
	emitter := NewEmitter("run-123", FuncReporter(func(event Event) {
		events = append(events, event)
	}))

	emitter.Progress(PhaseSplit, 42.5, map[string]any{"rows": 10})

	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1", len(events))
	}
	got := events[0]
	if got.RunID != "run-123" {
		t.Fatalf("RunID = %q, want run-123", got.RunID)
	}
	if got.Phase != PhaseSplit {
		t.Fatalf("Phase = %q, want %q", got.Phase, PhaseSplit)
	}
	if got.Type != TypeProgress {
		t.Fatalf("Type = %q, want %q", got.Type, TypeProgress)
	}
	if got.Percent == nil || *got.Percent != 42.5 {
		t.Fatalf("Percent = %#v, want 42.5", got.Percent)
	}
	if got.Time.IsZero() {
		t.Fatal("Time was not populated")
	}
	if _, ok := got.Metrics["rows"]; !ok {
		t.Fatalf("Metrics missing rows: %#v", got.Metrics)
	}
}

func TestNewEmitterGeneratesRunIDWhenMissing(t *testing.T) {
	var got Event
	emitter := NewEmitter("", FuncReporter(func(event Event) {
		got = event
	}))

	emitter.Started(PhaseRun, "starting", nil)

	if got.RunID == "" {
		t.Fatal("RunID was empty")
	}
	if got.Time.IsZero() || got.Time.After(time.Now().UTC().Add(time.Second)) {
		t.Fatalf("unexpected Time value: %v", got.Time)
	}
}
