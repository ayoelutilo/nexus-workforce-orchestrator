package httpapi

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/oss-showcase/nexus-workforce-orchestrator/internal/runstore"
	"github.com/oss-showcase/nexus-workforce-orchestrator/internal/service"
)

func TestInvokeEndpointIdempotency(t *testing.T) {
	svc := service.New(runstore.NewMemoryStore())
	ts := httptest.NewServer(NewHandler(svc))
	defer ts.Close()

	body := []byte(`{"input":"hello world"}`)

	firstStatus, firstRunID, firstCreated := invokeRun(t, ts.Client(), ts.URL+"/v1/runs/invoke", body, "idem-key-1")
	if firstStatus != http.StatusAccepted {
		t.Fatalf("expected first status 202, got %d", firstStatus)
	}
	if !firstCreated {
		t.Fatalf("expected first call to create run")
	}

	secondStatus, secondRunID, secondCreated := invokeRun(t, ts.Client(), ts.URL+"/v1/runs/invoke", body, "idem-key-1")
	if secondStatus != http.StatusOK {
		t.Fatalf("expected second status 200, got %d", secondStatus)
	}
	if secondCreated {
		t.Fatalf("expected second call to dedupe")
	}
	if firstRunID != secondRunID {
		t.Fatalf("expected same run id, got %s and %s", firstRunID, secondRunID)
	}
}

func TestSSEStreamEmitsCreatedEvents(t *testing.T) {
	svc := service.New(runstore.NewMemoryStore())
	ts := httptest.NewServer(NewHandler(svc))
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/events/stream", nil)
	if err != nil {
		t.Fatalf("failed to build request: %v", err)
	}

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("failed to connect stream: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from stream endpoint, got %d", resp.StatusCode)
	}

	eventFound := make(chan struct{}, 1)
	reader := bufio.NewReader(resp.Body)
	go func() {
		for {
			line, readErr := reader.ReadString('\n')
			if readErr != nil {
				return
			}
			if strings.HasPrefix(line, "event: run.created") {
				eventFound <- struct{}{}
				return
			}
		}
	}()

	time.Sleep(25 * time.Millisecond)
	invokeRun(t, ts.Client(), ts.URL+"/v1/runs/invoke", []byte(`{"input":"trigger"}`), "stream-key")

	select {
	case <-eventFound:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for run.created event")
	}
}

func invokeRun(t *testing.T, client *http.Client, url string, body []byte, idempotencyKey string) (status int, runID string, created bool) {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to build invoke request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("invoke request failed: %v", err)
	}
	defer resp.Body.Close()

	var payload struct {
		Created bool `json:"created"`
		Run     struct {
			ID string `json:"id"`
		} `json:"run"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode invoke response: %v", err)
	}

	return resp.StatusCode, payload.Run.ID, payload.Created
}
