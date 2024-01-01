package runstore

import (
	"testing"
	"time"
)

func TestCreateRunIdempotency(t *testing.T) {
	store := NewMemoryStore()

	first, created, err := store.CreateRun("first-input", "idem-123")
	if err != nil {
		t.Fatalf("CreateRun first failed: %v", err)
	}
	if !created {
		t.Fatalf("expected first create to be new")
	}

	second, created, err := store.CreateRun("second-input", "idem-123")
	if err != nil {
		t.Fatalf("CreateRun second failed: %v", err)
	}
	if created {
		t.Fatalf("expected second create to dedupe by idempotency key")
	}
	if first.ID != second.ID {
		t.Fatalf("expected same run id for deduped invoke; got %s and %s", first.ID, second.ID)
	}
	if second.Input != "first-input" {
		t.Fatalf("expected original payload to be preserved, got %q", second.Input)
	}
}

func TestLeaseAcquireAndExpiry(t *testing.T) {
	store := NewMemoryStore()
	run, _, err := store.CreateRun("payload", "")
	if err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}

	leaseOne, ok, err := store.AcquireLease("worker-a", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("AcquireLease worker-a failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected worker-a to acquire lease")
	}
	if leaseOne.ID != run.ID {
		t.Fatalf("expected lease on created run, got %s", leaseOne.ID)
	}

	_, ok, err = store.AcquireLease("worker-b", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("AcquireLease worker-b failed: %v", err)
	}
	if ok {
		t.Fatalf("worker-b should not acquire active lease")
	}

	time.Sleep(75 * time.Millisecond)

	leaseTwo, ok, err := store.AcquireLease("worker-b", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("AcquireLease worker-b after expiry failed: %v", err)
	}
	if !ok {
		t.Fatalf("worker-b should acquire expired lease")
	}
	if leaseTwo.ID != run.ID {
		t.Fatalf("expected same run to be re-leased, got %s", leaseTwo.ID)
	}
	if leaseTwo.LeaseOwner != "worker-b" {
		t.Fatalf("expected worker-b as lease owner, got %q", leaseTwo.LeaseOwner)
	}
}

func TestControlSignalTransitions(t *testing.T) {
	store := NewMemoryStore()
	run, _, err := store.CreateRun("payload", "")
	if err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}

	updated, err := store.RequestControl(run.ID, ControlCancel)
	if err != nil {
		t.Fatalf("RequestControl failed: %v", err)
	}
	if updated.Control != ControlCancel {
		t.Fatalf("expected cancel control signal, got %s", updated.Control)
	}

	leased, ok, err := store.AcquireLease("worker-a", time.Second)
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected run to be leaseable")
	}
	if leased.Status != StatusRunning {
		t.Fatalf("expected run status running after lease, got %s", leased.Status)
	}

	terminal, err := store.CompleteRun(run.ID, "worker-a", StatusCanceled)
	if err != nil {
		t.Fatalf("CompleteRun failed: %v", err)
	}
	if terminal.Status != StatusCanceled {
		t.Fatalf("expected canceled status, got %s", terminal.Status)
	}

	_, err = store.RequestControl(run.ID, ControlCancel)
	if err == nil {
		t.Fatalf("expected terminal run to reject new control signals")
	}
}

func TestCompleteRunHonorsQueuedCancelIntent(t *testing.T) {
	store := NewMemoryStore()
	run, _, err := store.CreateRun("payload", "")
	if err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}

	_, ok, err := store.AcquireLease("worker-a", time.Second)
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected run to be leaseable")
	}

	_, err = store.RequestControl(run.ID, ControlCancel)
	if err != nil {
		t.Fatalf("RequestControl failed: %v", err)
	}

	completed, err := store.CompleteRun(run.ID, "worker-a", StatusCompleted)
	if err != nil {
		t.Fatalf("CompleteRun failed: %v", err)
	}
	if completed.Status != StatusCanceled {
		t.Fatalf("expected queued cancel to win, got %s", completed.Status)
	}
}

// Refinement.
