package dispatcher

import (
	"context"
	"log"
	"time"

	"github.com/ayoelutilo/nexus-workforce-orchestrator/internal/runstore"
	"github.com/ayoelutilo/nexus-workforce-orchestrator/internal/service"
)

type Dispatcher struct {
	svc          *service.Service
	workerID     string
	pollInterval time.Duration
	leaseTTL     time.Duration
	processDelay time.Duration
}

func New(svc *service.Service, workerID string) *Dispatcher {
	return &Dispatcher{
		svc:          svc,
		workerID:     workerID,
		pollInterval: 200 * time.Millisecond,
		leaseTTL:     2 * time.Second,
		processDelay: 150 * time.Millisecond,
	}
}

func (d *Dispatcher) Start(ctx context.Context) {
	go d.loop(ctx)
}

func (d *Dispatcher) loop(ctx context.Context) {
	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.dispatchOnce(ctx)
		}
	}
}

func (d *Dispatcher) dispatchOnce(ctx context.Context) {
	run, ok, err := d.svc.AcquireLease(d.workerID, d.leaseTTL)
	if err != nil {
		log.Printf("dispatcher acquire lease failed: %v", err)
		return
	}
	if !ok {
		return
	}

	if err := d.handleRun(ctx, run.ID); err != nil {
		log.Printf("dispatcher run %s failed: %v", run.ID, err)
	}
}

func (d *Dispatcher) handleRun(ctx context.Context, runID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d.processDelay):
	}

	run, ok := d.svc.GetRun(runID)
	if !ok {
		return runstore.ErrRunNotFound
	}

	finalStatus := runstore.StatusCompleted
	if run.Control == runstore.ControlCancel {
		finalStatus = runstore.StatusCanceled
	}

	_, err := d.svc.Complete(runID, d.workerID, finalStatus)
	return err
}
