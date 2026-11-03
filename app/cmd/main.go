package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"awesomeProject7/internal/fakes"
	"awesomeProject7/internal/limitservice"
	"awesomeProject7/internal/postgres"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))

	dsn := envOr("DATABASE_URL", "postgres://outbox:outbox@localhost:5432/outbox?sslmode=disable")
	addr := envOr("HTTP_ADDR", ":8080")

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		slog.Error("open db", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := waitForDB(db, 30*time.Second); err != nil {
		slog.Error("db is not reachable", "error", err)
		os.Exit(1)
	}

	repo, err := postgres.NewRepository(db, 3*time.Second)
	if err != nil {
		slog.Error("create repository", "error", err)
		os.Exit(1)
	}
	svc := limitservice.NewService(3*time.Second, repo, fakes.NewLender())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	workerDone := make(chan struct{})
	go func() {
		svc.BackgroundWorker(ctx)
		close(workerDone)
	}()

	mux := http.NewServeMux()

	mux.HandleFunc("POST /applications/{id}/block-limit", func(rw http.ResponseWriter, r *http.Request) {
		app, err := svc.BlockLenderLimit(r.Context(), r.PathValue("id"))
		if err != nil {
			writeError(rw, err)
			return
		}
		writeJSON(rw, http.StatusOK, toJSON(app))
	})

	// Отладочные эндпоинты, чтобы смотреть состояние без psql.
	mux.HandleFunc("GET /applications/{id}", func(rw http.ResponseWriter, r *http.Request) {
		app, err := repo.FindByID(r.Context(), r.PathValue("id"))
		if err != nil {
			writeError(rw, err)
			return
		}
		writeJSON(rw, http.StatusOK, toJSON(app))
	})
	mux.HandleFunc("GET /operations", func(rw http.ResponseWriter, r *http.Request) {
		list, err := listOperations(r.Context(), db)
		if err != nil {
			writeError(rw, err)
			return
		}
		writeJSON(rw, http.StatusOK, list)
	})
	mux.HandleFunc("GET /outbox-events", func(rw http.ResponseWriter, r *http.Request) {
		list, err := listEvents(r.Context(), db)
		if err != nil {
			writeError(rw, err)
			return
		}
		writeJSON(rw, http.StatusOK, list)
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		slog.Info("http server started", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown", "error", err)
	}
	select {
	case <-workerDone:
		slog.Info("background worker stopped")
	case <-shutdownCtx.Done():
		slog.Warn("background worker did not stop in time")
	}
}

type applicationJSON struct {
	ID                   string `json:"id"`
	ClientID             string `json:"client_id"`
	LenderID             string `json:"lender_id"`
	Status               string `json:"status"`
	RequestedAmountMinor int64  `json:"requested_amount_minor"`
	Currency             string `json:"currency"`
	LenderBlockID        string `json:"lender_block_id"`
	Version              int64  `json:"version"`
}

func toJSON(a limitservice.Application) applicationJSON {
	return applicationJSON{
		ID: a.ID, ClientID: a.ClientID, LenderID: a.LenderID, Status: string(a.Status),
		RequestedAmountMinor: a.RequestedAmountMinor, Currency: a.Currency,
		LenderBlockID: a.LenderBlockID, Version: a.Version,
	}
}

func writeError(rw http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, limitservice.ErrInvalidInput):
		status = http.StatusBadRequest
	case errors.Is(err, limitservice.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, limitservice.ErrInvalidStatus), errors.Is(err, limitservice.ErrConflict):
		status = http.StatusConflict
	case errors.Is(err, limitservice.ErrLimitDeclined):
		status = http.StatusUnprocessableEntity
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
	case errors.Is(err, limitservice.ErrBlockPending):
		status = http.StatusServiceUnavailable
	}
	if status == http.StatusInternalServerError {
		slog.Error("request failed", "error", err)
	}
	writeJSON(rw, status, map[string]string{"error": err.Error()})
}

func writeJSON(rw http.ResponseWriter, status int, v any) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(status)
	enc := json.NewEncoder(rw)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

type operationJSON struct {
	ApplicationID string    `json:"application_id"`
	OperationID   string    `json:"operation_id"`
	LenderID      string    `json:"lender_id"`
	AmountMinor   int64     `json:"amount_minor"`
	Currency      string    `json:"currency"`
	State         string    `json:"state"`
	BlockID       string    `json:"block_id"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func listOperations(ctx context.Context, db *sql.DB) ([]operationJSON, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT application_id, operation_id, lender_id, amount_minor, currency, state, block_id, created_at, updated_at
		FROM limit_block_operation ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := []operationJSON{}
	for rows.Next() {
		var o operationJSON
		if err := rows.Scan(&o.ApplicationID, &o.OperationID, &o.LenderID, &o.AmountMinor, &o.Currency, &o.State, &o.BlockID, &o.CreatedAt, &o.UpdatedAt); err != nil {
			return nil, err
		}
		list = append(list, o)
	}
	return list, rows.Err()
}

type eventJSON struct {
	EventID     string          `json:"event_id"`
	Topic       string          `json:"topic"`
	Key         string          `json:"msg_key"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
	PublishedAt *time.Time      `json:"published_at"`
}

func listEvents(ctx context.Context, db *sql.DB) ([]eventJSON, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT event_id, topic, msg_key, payload, created_at, published_at
		FROM outbox_events ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := []eventJSON{}
	for rows.Next() {
		var e eventJSON
		var payload []byte
		var published sql.NullTime
		if err := rows.Scan(&e.EventID, &e.Topic, &e.Key, &payload, &e.CreatedAt, &published); err != nil {
			return nil, err
		}
		e.Payload = payload
		if published.Valid {
			e.PublishedAt = &published.Time
		}
		list = append(list, e)
	}
	return list, rows.Err()
}

func waitForDB(db *sql.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := db.PingContext(ctx)
		cancel()
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(time.Second)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
