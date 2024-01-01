package runstore

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type RunStatus string

type ControlSignal string

const (
	StatusQueued    RunStatus = "queued"
	StatusRunning   RunStatus = "running"
	StatusCompleted RunStatus = "completed"
	StatusCanceled  RunStatus = "canceled"
	StatusFailed    RunStatus = "failed"
)

const (
	ControlNone   ControlSignal = "none"
	ControlCancel ControlSignal = "cancel"
)

var (
	ErrRunNotFound      = errors.New("run not found")
	ErrInvalidSignal    = errors.New("invalid control signal")
	ErrTerminalRun      = errors.New("run is in terminal status")
	ErrLeaseOwner       = errors.New("worker does not hold lease")
	ErrInvalidFinalRun  = errors.New("final status must be terminal")
	ErrInvalidLeaseTTL  = errors.New("lease ttl must be greater than zero")
	ErrInvalidWorkerID  = errors.New("worker id must not be empty")
	ErrInvalidRunStatus = errors.New("run is not in a leaseable status")
)

type Run struct {
	ID             string        `json:"id"`
	Input          string        `json:"input"`
	IdempotencyKey string        `json:"idempotency_key,omitempty"`
	Status         RunStatus     `json:"status"`
	Control        ControlSignal `json:"control"`
	LeaseOwner     string        `json:"lease_owner,omitempty"`
	LeaseUntil     time.Time     `json:"lease_until,omitempty"`
	Version        int64         `json:"version"`
	CreatedAt      time.Time     `json:"created_at"`
	UpdatedAt      time.Time     `json:"updated_at"`
}

type RunEvent struct {
	Seq        int64             `json:"seq"`
	RunID      string            `json:"run_id"`
	Type       string            `json:"type"`
	Message    string            `json:"message,omitempty"`
	At         time.Time         `json:"at"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type MemoryStore struct {
	mu              sync.Mutex
	clock           func() time.Time
	runs            map[string]*Run
	runOrder        []string
	idempotencyKeys map[string]string
	events          []RunEvent
	nextRunID       int64
	nextEventSeq    int64
	subscribers     map[int]chan RunEvent
	nextSubscriber  int
}

func NewMemoryStore() *MemoryStore {
	return NewMemoryStoreWithClock(time.Now)
}

func NewMemoryStoreWithClock(clock func() time.Time) *MemoryStore {
	return &MemoryStore{
		clock:           clock,
		runs:            map[string]*Run{},
		idempotencyKeys: map[string]string{},
		events:          make([]RunEvent, 0, 128),
		subscribers:     map[int]chan RunEvent{},
	}
}

func (s *MemoryStore) CreateRun(input, idempotencyKey string) (Run, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if idempotencyKey != "" {
		if existingID, ok := s.idempotencyKeys[idempotencyKey]; ok {
			run := s.runs[existingID]
			return copyRun(run), false, nil
		}
	}

	now := s.clock()
	s.nextRunID++
	runID := fmt.Sprintf("run-%06d", s.nextRunID)
	run := &Run{
		ID:             runID,
		Input:          input,
		IdempotencyKey: idempotencyKey,
		Status:         StatusQueued,
		Control:        ControlNone,
		Version:        1,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	s.runs[runID] = run
	s.runOrder = append(s.runOrder, runID)
	if idempotencyKey != "" {
		s.idempotencyKeys[idempotencyKey] = runID
	}

	s.appendEventLocked(RunEvent{
		RunID:   run.ID,
		Type:    "run.created",
		Message: "run queued",
		At:      now,
		Attributes: map[string]string{
			"status": string(run.Status),
		},
	})

	return copyRun(run), true, nil
}

func (s *MemoryStore) GetRun(runID string) (Run, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	run, ok := s.runs[runID]
	if !ok {
		return Run{}, false
	}
	return copyRun(run), true
}

func (s *MemoryStore) AcquireLease(workerID string, ttl time.Duration) (Run, bool, error) {
	if workerID == "" {
		return Run{}, false, ErrInvalidWorkerID
	}
	if ttl <= 0 {
		return Run{}, false, ErrInvalidLeaseTTL
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock()
	for _, runID := range s.runOrder {
		run := s.runs[runID]
		if run == nil {
			continue
		}
		if isTerminal(run.Status) {
			continue
		}
		if run.Status != StatusQueued && run.Status != StatusRunning {
			continue
		}
		if run.LeaseOwner != "" && now.Before(run.LeaseUntil) {
			continue
		}

		run.Status = StatusRunning
		run.LeaseOwner = workerID
		run.LeaseUntil = now.Add(ttl)
		run.Version++
		run.UpdatedAt = now

		s.appendEventLocked(RunEvent{
			RunID:   run.ID,
			Type:    "run.leased",
			Message: "run leased to worker",
			At:      now,
			Attributes: map[string]string{
				"worker_id": workerID,
				"status":    string(run.Status),
			},
		})
		return copyRun(run), true, nil
	}

	return Run{}, false, nil
}

func (s *MemoryStore) RequestControl(runID string, signal ControlSignal) (Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if signal != ControlCancel {
		return Run{}, ErrInvalidSignal
	}

	run, ok := s.runs[runID]
	if !ok {
		return Run{}, ErrRunNotFound
	}
	if isTerminal(run.Status) {
		return Run{}, ErrTerminalRun
	}
	if run.Control == signal {
		return copyRun(run), nil
	}

	run.Control = signal
	run.Version++
	run.UpdatedAt = s.clock()

	s.appendEventLocked(RunEvent{
		RunID:   run.ID,
		Type:    "run.control_requested",
		Message: "control signal queued",
		At:      run.UpdatedAt,
		Attributes: map[string]string{
			"signal": string(signal),
		},
	})

	return copyRun(run), nil
}

func (s *MemoryStore) CompleteRun(runID, workerID string, finalStatus RunStatus) (Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !isTerminal(finalStatus) {
		return Run{}, ErrInvalidFinalRun
	}

	run, ok := s.runs[runID]
	if !ok {
		return Run{}, ErrRunNotFound
	}
	if isTerminal(run.Status) {
		return Run{}, ErrTerminalRun
	}
	if run.Status != StatusRunning {
		return Run{}, ErrInvalidRunStatus
	}
	if workerID != "" && run.LeaseOwner != workerID {
		return Run{}, ErrLeaseOwner
	}

	now := s.clock()
	effectiveStatus := finalStatus
	// Honor cancel control intent even if the caller raced and attempted completion.
	if run.Control == ControlCancel && finalStatus == StatusCompleted {
		effectiveStatus = StatusCanceled
	}

	run.Status = effectiveStatus
	run.Control = ControlNone
	run.LeaseOwner = ""
	run.LeaseUntil = time.Time{}
	run.Version++
	run.UpdatedAt = now

	eventType := "run.completed"
	message := "run completed"
	switch effectiveStatus {
	case StatusCanceled:
		eventType = "run.canceled"
		message = "run canceled"
	case StatusFailed:
		eventType = "run.failed"
		message = "run failed"
	}

	s.appendEventLocked(RunEvent{
		RunID:   run.ID,
		Type:    eventType,
		Message: message,
		At:      now,
		Attributes: map[string]string{
			"status": string(effectiveStatus),
		},
	})

	return copyRun(run), nil
}

func (s *MemoryStore) ApplyCallback(runID, callbackType, message string) (Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	run, ok := s.runs[runID]
	if !ok {
		return Run{}, ErrRunNotFound
	}

	now := s.clock()
	eventType := "run.callback"
	if callbackType != "" {
		eventType = "run.callback." + callbackType
	}

	s.appendEventLocked(RunEvent{
		RunID:   run.ID,
		Type:    eventType,
		Message: message,
		At:      now,
	})

	if isTerminal(run.Status) {
		return copyRun(run), nil
	}

	switch callbackType {
	case "completed":
		run.Status = StatusCompleted
		run.Control = ControlNone
		run.LeaseOwner = ""
		run.LeaseUntil = time.Time{}
		run.Version++
		run.UpdatedAt = now
		s.appendEventLocked(RunEvent{
			RunID:   run.ID,
			Type:    "run.completed",
			Message: "run completed via callback",
			At:      now,
			Attributes: map[string]string{
				"status": string(run.Status),
			},
		})
	case "failed":
		run.Status = StatusFailed
		run.Control = ControlNone
		run.LeaseOwner = ""
		run.LeaseUntil = time.Time{}
		run.Version++
		run.UpdatedAt = now
		s.appendEventLocked(RunEvent{
			RunID:   run.ID,
			Type:    "run.failed",
			Message: "run failed via callback",
			At:      now,
			Attributes: map[string]string{
				"status": string(run.Status),
			},
		})
	}

	return copyRun(run), nil
}

func (s *MemoryStore) ListEventsSince(lastSeq int64) []RunEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.events) == 0 {
		return nil
	}

	start := 0
	for start < len(s.events) && s.events[start].Seq <= lastSeq {
		start++
	}
	if start >= len(s.events) {
		return nil
	}

	out := make([]RunEvent, 0, len(s.events)-start)
	for i := start; i < len(s.events); i++ {
		out = append(out, copyEvent(s.events[i]))
	}
	return out
}

func (s *MemoryStore) Subscribe() (<-chan RunEvent, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextSubscriber++
	subID := s.nextSubscriber
	ch := make(chan RunEvent, 32)
	s.subscribers[subID] = ch

	cancel := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		existing := s.subscribers[subID]
		if existing != nil {
			delete(s.subscribers, subID)
			close(existing)
		}
	}

	return ch, cancel
}

func (s *MemoryStore) appendEventLocked(ev RunEvent) {
	s.nextEventSeq++
	ev.Seq = s.nextEventSeq
	if ev.At.IsZero() {
		ev.At = s.clock()
	}
	eventCopy := copyEvent(ev)
	s.events = append(s.events, eventCopy)
	for id, sub := range s.subscribers {
		select {
		case sub <- eventCopy:
		default:
			// Subscriber is too slow; close it to avoid silent event loss.
			delete(s.subscribers, id)
			close(sub)
		}
	}
}

func isTerminal(status RunStatus) bool {
	switch status {
	case StatusCompleted, StatusCanceled, StatusFailed:
		return true
	default:
		return false
	}
}

func copyRun(in *Run) Run {
	if in == nil {
		return Run{}
	}
	return Run{
		ID:             in.ID,
		Input:          in.Input,
		IdempotencyKey: in.IdempotencyKey,
		Status:         in.Status,
		Control:        in.Control,
		LeaseOwner:     in.LeaseOwner,
		LeaseUntil:     in.LeaseUntil,
		Version:        in.Version,
		CreatedAt:      in.CreatedAt,
		UpdatedAt:      in.UpdatedAt,
	}
}

func copyEvent(in RunEvent) RunEvent {
	out := in
	if in.Attributes != nil {
		out.Attributes = make(map[string]string, len(in.Attributes))
		for k, v := range in.Attributes {
			out.Attributes[k] = v
		}
	}
	return out
}

// Refinement.
