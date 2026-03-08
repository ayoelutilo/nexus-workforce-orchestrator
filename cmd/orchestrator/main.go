package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ayoelutilo/nexus-workforce-orchestrator/internal/dispatcher"
	"github.com/ayoelutilo/nexus-workforce-orchestrator/internal/httpapi"
	"github.com/ayoelutilo/nexus-workforce-orchestrator/internal/runstore"
	"github.com/ayoelutilo/nexus-workforce-orchestrator/internal/service"
)

func main() {
	store := runstore.NewMemoryStore()
	svc := service.New(store)
	d := dispatcher.New(svc, "worker-1")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	d.Start(ctx)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	httpServer := &http.Server{
		Addr:              ":" + port,
		Handler:           httpapi.NewHandler(svc),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	log.Printf("nexus-workforce-orchestrator listening on :%s", port)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server failed: %v", err)
	}
}

// Refinement.

// Refinement.

// Refinement.
