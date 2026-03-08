package service

import (
	"errors"
	"time"

	"github.com/ayoelutilo/nexus-workforce-orchestrator/internal/runstore"
)

type Service struct {
	store *runstore.MemoryStore
}

func New(store *runstore.MemoryStore) *Service {
	return &Service{store: store}
}

func (s *Service) Invoke(input, idempotencyKey string) (runstore.Run, bool, error) {
	return s.store.CreateRun(input, idempotencyKey)
}

func (s *Service) Control(runID, signal string) (runstore.Run, error) {
	switch signal {
	case string(runstore.ControlCancel):
		return s.store.RequestControl(runID, runstore.ControlCancel)
	default:
		return runstore.Run{}, errors.New("unsupported control signal")
	}
}

func (s *Service) Callback(runID, callbackType, message string) (runstore.Run, error) {
	return s.store.ApplyCallback(runID, callbackType, message)
}

func (s *Service) AcquireLease(workerID string, ttl time.Duration) (runstore.Run, bool, error) {
	return s.store.AcquireLease(workerID, ttl)
}

func (s *Service) Complete(runID, workerID string, status runstore.RunStatus) (runstore.Run, error) {
	return s.store.CompleteRun(runID, workerID, status)
}

func (s *Service) GetRun(runID string) (runstore.Run, bool) {
	return s.store.GetRun(runID)
}

func (s *Service) EventsSince(seq int64) []runstore.RunEvent {
	return s.store.ListEventsSince(seq)
}

func (s *Service) Subscribe() (<-chan runstore.RunEvent, func()) {
	return s.store.Subscribe()
}

// Refinement.

// Refinement.
