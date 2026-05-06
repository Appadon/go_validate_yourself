package runs

import (
	"errors"
	"testing"
	"time"

	"go_validate_yourself/internal/monitor"
	"go_validate_yourself/internal/progress"
	"go_validate_yourself/internal/workspace"
)

func TestManagerTransitionsAndRetainsFinalResult(t *testing.T) {
	manager := NewManager()
	runWorkspace := mustWorkspace(t, "run-123")

	queued, err := manager.Create("run-123", &runWorkspace)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if queued.State != StateQueued {
		t.Fatalf("queued state = %q", queued.State)
	}
	if queued.Workspace == nil || queued.Workspace.RootDir == "" {
		t.Fatalf("queued workspace missing: %+v", queued.Workspace)
	}

	running, err := manager.Start("run-123")
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if running.State != StateRunning {
		t.Fatalf("running state = %q", running.State)
	}
	if running.StartedAt == nil {
		t.Fatal("running.StartedAt was nil")
	}

	result := map[string]any{"ok": true}
	completed, err := manager.Complete("run-123", result)
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if completed.State != StateCompleted {
		t.Fatalf("completed state = %q", completed.State)
	}
	if completed.FinishedAt == nil {
		t.Fatal("completed.FinishedAt was nil")
	}
	gotResult, ok := completed.FinalResult.(map[string]any)
	if !ok || !gotResult["ok"].(bool) {
		t.Fatalf("FinalResult = %#v", completed.FinalResult)
	}
	if manager.HasActive() {
		t.Fatal("manager should not be active after completion")
	}
}

func TestManagerRejectsSecondActiveRun(t *testing.T) {
	manager := NewManager()
	if _, err := manager.Create("run-1", nil); err != nil {
		t.Fatalf("Create(run-1) error = %v", err)
	}

	_, err := manager.Create("run-2", nil)
	if !errors.Is(err, ErrActiveRunExists) {
		t.Fatalf("Create(run-2) error = %v, want %v", err, ErrActiveRunExists)
	}
}

func TestManagerRetainsFailureEventsAndFinalError(t *testing.T) {
	manager := NewManager()
	if _, err := manager.Create("run-fail", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := manager.Start("run-fail"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	reporter := manager.Reporter("run-fail")
	reporter.Report(progress.Event{
		RunID:   "run-fail",
		Time:    time.Now().UTC(),
		Phase:   progress.PhaseValidate,
		Type:    progress.TypeFailed,
		Message: "directory validation failed: bad row",
		Metrics: map[string]any{"failed_files": 1},
	})

	failed, err := manager.Fail("run-fail", errors.New("directory validation completed with 1 failed file(s)"))
	if err != nil {
		t.Fatalf("Fail() error = %v", err)
	}
	if failed.State != StateFailed {
		t.Fatalf("failed state = %q", failed.State)
	}
	if failed.FinalError == "" {
		t.Fatal("FinalError was empty")
	}
	if failed.LatestEvent == nil {
		t.Fatal("LatestEvent was nil")
	}
	if failed.LatestEvent.Type != progress.TypeFailed {
		t.Fatalf("LatestEvent.Type = %q", failed.LatestEvent.Type)
	}
	if len(failed.Events) != 1 {
		t.Fatalf("Events length = %d, want 1", len(failed.Events))
	}
	if failed.Events[0].Message != "directory validation failed: bad row" {
		t.Fatalf("failure event message = %q", failed.Events[0].Message)
	}
}

func TestManagerStoresTelemetryWithoutReplacingLatestEvent(t *testing.T) {
	manager := NewManager()
	if _, err := manager.Create("run-telemetry", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := manager.Start("run-telemetry"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	reporter := manager.Reporter("run-telemetry")
	reporter.Report(progress.Event{
		RunID:   "run-telemetry",
		Time:    time.Now().UTC(),
		Phase:   progress.PhaseValidate,
		Type:    progress.TypeProgress,
		Message: "validating",
	})
	cpu := 12.5
	reporter.Report(progress.Event{
		RunID: "run-telemetry",
		Time:  time.Now().UTC(),
		Phase: progress.PhaseRun,
		Type:  progress.TypeTelemetry,
		Metrics: map[string]any{
			"performance": monitor.ResourceSnapshot{
				Time:       time.Now().UTC(),
				CPUPercent: &cpu,
				Memory:     monitor.MemorySnapshot{AllocBytes: 1024},
				Disk:       monitor.DiskSnapshot{AvailableBytes: 2048},
			},
		},
	})

	snapshot, ok := manager.Snapshot("run-telemetry")
	if !ok {
		t.Fatal("Snapshot() returned ok=false")
	}
	if snapshot.Performance == nil {
		t.Fatal("Performance was nil")
	}
	if snapshot.Performance.CPUPercent == nil || *snapshot.Performance.CPUPercent != cpu {
		t.Fatalf("CPUPercent = %#v, want %v", snapshot.Performance.CPUPercent, cpu)
	}
	if snapshot.LatestEvent == nil || snapshot.LatestEvent.Type != progress.TypeProgress {
		t.Fatalf("LatestEvent = %+v, want progress event", snapshot.LatestEvent)
	}
	if len(snapshot.Events) != 1 {
		t.Fatalf("Events length = %d, want telemetry excluded from retained event log", len(snapshot.Events))
	}
}

func mustWorkspace(t *testing.T, runID string) workspace.RunWorkspace {
	t.Helper()
	ws, err := workspace.NewUnder(t.TempDir(), runID)
	if err != nil {
		t.Fatalf("workspace.NewUnder() error = %v", err)
	}
	return ws
}
