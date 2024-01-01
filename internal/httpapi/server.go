package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/oss-showcase/nexus-workforce-orchestrator/internal/runstore"
	"github.com/oss-showcase/nexus-workforce-orchestrator/internal/service"
)

type Server struct {
	svc *service.Service
}

const maxJSONBodyBytes = 1 << 20

func NewHandler(svc *service.Service) http.Handler {
	s := &Server{svc: svc}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/v1/runs/invoke", s.handleInvoke)
	mux.HandleFunc("/v1/runs/", s.handleRunSubresource)
	mux.HandleFunc("/v1/events/stream", s.handleStream)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleInvoke(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		Input          string `json:"input"`
		IdempotencyKey string `json:"idempotency_key"`
	}
	if err := decodeJSON(limitBody(w, r), &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" {
		idempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	}

	run, created, err := s.svc.Invoke(req.Input, idempotencyKey)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	statusCode := http.StatusAccepted
	if !created {
		statusCode = http.StatusOK
	}

	writeJSON(w, statusCode, map[string]any{
		"created":         created,
		"idempotency_key": idempotencyKey,
		"run":             run,
	})
}

func (s *Server) handleRunSubresource(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/runs/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	runID := parts[0]
	subresource := parts[1]

	switch subresource {
	case "control":
		s.handleControl(w, r, runID)
	case "callback":
		s.handleCallback(w, r, runID)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) handleControl(w http.ResponseWriter, r *http.Request, runID string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		Signal string `json:"signal"`
	}
	if err := decodeJSON(limitBody(w, r), &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	run, err := s.svc.Control(runID, strings.TrimSpace(req.Signal))
	if err != nil {
		status := http.StatusBadRequest
		switch err {
		case runstore.ErrRunNotFound:
			status = http.StatusNotFound
		case runstore.ErrTerminalRun:
			status = http.StatusConflict
		}
		writeError(w, status, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"run": run})
}

func (s *Server) handleCallback(w http.ResponseWriter, r *http.Request, runID string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	}
	if err := decodeJSON(limitBody(w, r), &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	run, err := s.svc.Callback(runID, strings.TrimSpace(req.Type), req.Message)
	if err != nil {
		status := http.StatusBadRequest
		if err == runstore.ErrRunNotFound {
			status = http.StatusNotFound
		}
		writeError(w, status, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"run": run})
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	lastSeq := int64(0)
	if raw := strings.TrimSpace(r.URL.Query().Get("since")); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "since must be an integer")
			return
		}
		lastSeq = parsed
	}

	headers := w.Header()
	headers.Set("Content-Type", "text/event-stream")
	headers.Set("Cache-Control", "no-cache")
	headers.Set("Connection", "keep-alive")
	headers.Set("X-Accel-Buffering", "no")

	for _, event := range s.svc.EventsSince(lastSeq) {
		if err := writeSSE(w, flusher, event); err != nil {
			return
		}
		lastSeq = event.Seq
	}

	events, cancel := s.svc.Subscribe()
	defer cancel()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			if _, err := io.WriteString(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case event, ok := <-events:
			if !ok {
				return
			}
			if event.Seq <= lastSeq {
				continue
			}
			if err := writeSSE(w, flusher, event); err != nil {
				return
			}
			lastSeq = event.Seq
		}
	}
}

func writeSSE(w io.Writer, flusher http.Flusher, event runstore.RunEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.Seq, event.Type, payload); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

func decodeJSON(body io.ReadCloser, out any) error {
	defer body.Close()
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("invalid request body: multiple JSON values")
	}
	return nil
}

func limitBody(w http.ResponseWriter, r *http.Request) io.ReadCloser {
	return http.MaxBytesReader(w, r.Body, maxJSONBodyBytes)
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, map[string]any{"error": message})
}

// Refinement.
