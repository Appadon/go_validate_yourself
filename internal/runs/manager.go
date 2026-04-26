package runs

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go_validate_yourself/internal/progress"
	"go_validate_yourself/internal/workspace"
)

const defaultEventLimit = 256

var (
	ErrActiveRunExists   = errors.New("another run is already active")
	ErrRunNotFound       = errors.New("run not found")
	ErrInvalidTransition = errors.New("invalid run state transition")
)

type State string

const (
	StateQueued    State = "queued"
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
)

/* Snapshot captures the observable state for one run. */
type Snapshot struct {
	RunID       string                  `json:"run_id"`
	State       State                   `json:"state"`
	CreatedAt   time.Time               `json:"created_at"`
	StartedAt   *time.Time              `json:"started_at,omitempty"`
	FinishedAt  *time.Time              `json:"finished_at,omitempty"`
	Workspace   *workspace.RunWorkspace `json:"workspace,omitempty"`
	LatestEvent *progress.Event         `json:"latest_event,omitempty"`
	Events      []progress.Event        `json:"events,omitempty"`
	FinalResult any                     `json:"final_result,omitempty"`
	FinalError  string                  `json:"final_error,omitempty"`
}

type runRecord struct {
	snapshot    Snapshot
	subscribers map[int]chan progress.Event
}

/* Manager tracks one active run plus recent in-memory run state. */
type Manager struct {
	mu         sync.Mutex
	now        func() time.Time
	eventLimit int
	activeRun  string
	latestRun  string
	runs       map[string]*runRecord
	nextSubID  int
}

/* NewManager returns the smallest in-memory manager needed for Stage 3. */
func NewManager() *Manager {
	return &Manager{
		now:        func() time.Time { return time.Now().UTC() },
		eventLimit: defaultEventLimit,
		runs:       make(map[string]*runRecord),
	}
}

/* Create registers a queued run when no other run is active. */
func (m *Manager) Create(runID string, ws *workspace.RunWorkspace) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeRun != "" {
		return Snapshot{}, ErrActiveRunExists
	}
	if runID == "" {
		runID = progress.NewRunID()
	}
	if _, exists := m.runs[runID]; exists {
		return Snapshot{}, fmt.Errorf("run %q already exists", runID)
	}

	record := &runRecord{
		snapshot: Snapshot{
			RunID:     runID,
			State:     StateQueued,
			CreatedAt: m.now(),
			Workspace: cloneWorkspace(ws),
		},
		subscribers: make(map[int]chan progress.Event),
	}
	m.runs[runID] = record
	m.activeRun = runID
	m.latestRun = runID
	if err := persistSnapshot(record.snapshot); err != nil {
		delete(m.runs, runID)
		m.activeRun = ""
		if m.latestRun == runID {
			m.latestRun = ""
		}
		return Snapshot{}, err
	}
	return cloneSnapshot(record.snapshot), nil
}

/* Start marks a queued run as running. */
func (m *Manager) Start(runID string) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	record, err := m.lookup(runID)
	if err != nil {
		return Snapshot{}, err
	}
	if record.snapshot.State != StateQueued {
		return Snapshot{}, fmt.Errorf("%w: cannot start from %s", ErrInvalidTransition, record.snapshot.State)
	}

	startedAt := m.now()
	record.snapshot.State = StateRunning
	record.snapshot.StartedAt = &startedAt
	m.latestRun = runID
	if err := persistSnapshot(record.snapshot); err != nil {
		m.latestRun = runID
		record.snapshot.State = StateQueued
		record.snapshot.StartedAt = nil
		return Snapshot{}, err
	}
	return cloneSnapshot(record.snapshot), nil
}

/* Complete stores the final result and releases the active slot. */
func (m *Manager) Complete(runID string, result any) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	record, err := m.lookup(runID)
	if err != nil {
		return Snapshot{}, err
	}
	if record.snapshot.State != StateQueued && record.snapshot.State != StateRunning {
		return Snapshot{}, fmt.Errorf("%w: cannot complete from %s", ErrInvalidTransition, record.snapshot.State)
	}

	previousState := record.snapshot.State
	finishedAt := m.now()
	record.snapshot.State = StateCompleted
	record.snapshot.FinishedAt = &finishedAt
	record.snapshot.FinalResult = result
	record.snapshot.FinalError = ""
	if m.activeRun == runID {
		m.activeRun = ""
	}
	m.latestRun = runID
	if err := persistSnapshot(record.snapshot); err != nil {
		record.snapshot.State = previousState
		record.snapshot.FinishedAt = nil
		record.snapshot.FinalResult = nil
		record.snapshot.FinalError = ""
		m.activeRun = runID
		return Snapshot{}, err
	}
	closeSubscribers(record.subscribers)
	return cloneSnapshot(record.snapshot), nil
}

/* Fail stores the final error and releases the active slot. */
func (m *Manager) Fail(runID string, runErr error) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	record, err := m.lookup(runID)
	if err != nil {
		return Snapshot{}, err
	}
	if record.snapshot.State != StateQueued && record.snapshot.State != StateRunning {
		return Snapshot{}, fmt.Errorf("%w: cannot fail from %s", ErrInvalidTransition, record.snapshot.State)
	}

	previousState := record.snapshot.State
	finishedAt := m.now()
	record.snapshot.State = StateFailed
	record.snapshot.FinishedAt = &finishedAt
	if runErr != nil {
		record.snapshot.FinalError = runErr.Error()
	} else {
		record.snapshot.FinalError = "run failed"
	}
	record.snapshot.FinalResult = nil
	if m.activeRun == runID {
		m.activeRun = ""
	}
	m.latestRun = runID
	if err := persistSnapshot(record.snapshot); err != nil {
		record.snapshot.State = previousState
		record.snapshot.FinishedAt = nil
		record.snapshot.FinalError = ""
		m.activeRun = runID
		return Snapshot{}, err
	}
	closeSubscribers(record.subscribers)
	return cloneSnapshot(record.snapshot), nil
}

/* AppendEvent retains one structured progress event on the run snapshot. */
func (m *Manager) AppendEvent(runID string, event progress.Event) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	record, err := m.lookup(runID)
	if err != nil {
		return Snapshot{}, err
	}

	cloned := cloneEvent(event)
	record.snapshot.LatestEvent = &cloned
	record.snapshot.Events = append(record.snapshot.Events, cloned)
	if len(record.snapshot.Events) > m.eventLimit {
		record.snapshot.Events = append([]progress.Event(nil), record.snapshot.Events[len(record.snapshot.Events)-m.eventLimit:]...)
	}
	m.latestRun = runID
	if err := persistSnapshot(record.snapshot); err != nil {
		return Snapshot{}, err
	}
	for _, subscriber := range record.subscribers {
		select {
		case subscriber <- cloneEvent(cloned):
		default:
		}
	}
	return cloneSnapshot(record.snapshot), nil
}

/* Reporter returns a structured event sink that appends to this manager. */
func (m *Manager) Reporter(runID string) progress.Reporter {
	return progress.FuncReporter(func(event progress.Event) {
		_, _ = m.AppendEvent(runID, event)
	})
}

/* HasActive reports whether one run currently occupies the active slot. */
func (m *Manager) HasActive() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeRun != ""
}

/* ActiveSnapshot returns the current active run when one exists. */
func (m *Manager) ActiveSnapshot() (Snapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeRun == "" {
		return Snapshot{}, false
	}
	record := m.runs[m.activeRun]
	if record == nil {
		return Snapshot{}, false
	}
	return cloneSnapshot(record.snapshot), true
}

/* LatestSnapshot returns the most recent run snapshot when available. */
func (m *Manager) LatestSnapshot() (Snapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.latestRun == "" {
		return Snapshot{}, false
	}
	record := m.runs[m.latestRun]
	if record == nil {
		return Snapshot{}, false
	}
	return cloneSnapshot(record.snapshot), true
}

/* Snapshot returns the stored snapshot for a specific run id. */
func (m *Manager) Snapshot(runID string) (Snapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	record := m.runs[runID]
	if record == nil {
		return Snapshot{}, false
	}
	return cloneSnapshot(record.snapshot), true
}

/* Subscribe returns a snapshot plus a live event stream for one running run. */
func (m *Manager) Subscribe(runID string) (Snapshot, <-chan progress.Event, func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	record, err := m.lookup(runID)
	if err != nil {
		return Snapshot{}, nil, nil, err
	}

	snapshot := cloneSnapshot(record.snapshot)
	if snapshot.State == StateCompleted || snapshot.State == StateFailed {
		ch := make(chan progress.Event)
		close(ch)
		return snapshot, ch, func() {}, nil
	}

	subID := m.nextSubID
	m.nextSubID++
	ch := make(chan progress.Event, 32)
	record.subscribers[subID] = ch

	cancel := func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		current := m.runs[runID]
		if current == nil {
			return
		}
		subscriber := current.subscribers[subID]
		if subscriber == nil {
			return
		}
		delete(current.subscribers, subID)
		close(subscriber)
	}

	return snapshot, ch, cancel, nil
}

/* lookup returns the mutable record for a run id or ErrRunNotFound when absent. */
func (m *Manager) lookup(runID string) (*runRecord, error) {
	record := m.runs[runID]
	if record == nil {
		return nil, ErrRunNotFound
	}
	return record, nil
}

/* cloneSnapshot deep-copies mutable snapshot fields before returning them to callers. */
func cloneSnapshot(snapshot Snapshot) Snapshot {
	cloned := snapshot
	cloned.Workspace = cloneWorkspace(snapshot.Workspace)
	if snapshot.StartedAt != nil {
		startedAt := *snapshot.StartedAt
		cloned.StartedAt = &startedAt
	}
	if snapshot.FinishedAt != nil {
		finishedAt := *snapshot.FinishedAt
		cloned.FinishedAt = &finishedAt
	}
	if snapshot.LatestEvent != nil {
		latest := cloneEvent(*snapshot.LatestEvent)
		cloned.LatestEvent = &latest
	}
	if len(snapshot.Events) > 0 {
		cloned.Events = make([]progress.Event, len(snapshot.Events))
		for i, event := range snapshot.Events {
			cloned.Events[i] = cloneEvent(event)
		}
	}
	return cloned
}

/* cloneWorkspace copies workspace metadata so callers cannot mutate manager state. */
func cloneWorkspace(ws *workspace.RunWorkspace) *workspace.RunWorkspace {
	if ws == nil {
		return nil
	}
	cloned := *ws
	return &cloned
}

/* cloneEvent deep-copies optional progress fields for safe snapshot retention and fanout. */
func cloneEvent(event progress.Event) progress.Event {
	cloned := event
	if event.Percent != nil {
		pct := *event.Percent
		cloned.Percent = &pct
	}
	if len(event.Metrics) > 0 {
		cloned.Metrics = make(map[string]any, len(event.Metrics))
		for key, value := range event.Metrics {
			cloned.Metrics[key] = value
		}
	}
	return cloned
}

/* persistSnapshot writes the latest run snapshot into the workspace metadata file when configured. */
func persistSnapshot(snapshot Snapshot) error {
	if snapshot.Workspace == nil || snapshot.Workspace.MetadataPath == "" {
		return nil
	}
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal run metadata: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(snapshot.Workspace.MetadataPath), 0o755); err != nil {
		return fmt.Errorf("create run metadata directory: %w", err)
	}
	if err := os.WriteFile(snapshot.Workspace.MetadataPath, append(data, '\n'), 0o644); err != nil {
		return fmt.Errorf("write run metadata %q: %w", snapshot.Workspace.MetadataPath, err)
	}
	return nil
}

/* closeSubscribers closes and removes all live event subscribers for a finished run. */
func closeSubscribers(subscribers map[int]chan progress.Event) {
	for id, ch := range subscribers {
		close(ch)
		delete(subscribers, id)
	}
}
