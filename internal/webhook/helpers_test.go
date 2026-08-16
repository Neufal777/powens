package webhook

// Shared test helpers.
//
// Tests run against a real SQLite file and a real httptest server: the
// interesting behaviour of this package IS the SQL and the HTTP, so mocks would
// mostly test the mocks. Timings are compressed to milliseconds, so the whole
// suite takes ~2s.

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"
)

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := openDB(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func testCfg() config {
	return config{
		secret:          "test-secret",
		workers:         4,
		maxAttempts:     3,
		backoffBase:     2 * time.Millisecond,
		deliveryTimeout: 2 * time.Second,
	}
}

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestJob(url string, maxAttempts int) Job {
	now := time.Now().UTC()
	return Job{
		ID:             newID(),
		EventType:      "payment.completed",
		Payload:        json.RawMessage(`{"amount":4200,"currency":"EUR"}`),
		DestinationURL: url,
		Status:         statusPending,
		MaxAttempts:    maxAttempts,
		NextAttemptAt:  now,
		CreatedAt:      now,
	}
}

// startWorker runs a delivery worker for the duration of a test and guarantees
// it is stopped afterwards, so a hung test cannot leak into the next one.
func startWorker(t *testing.T, db *sql.DB, cfg config) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runWorker(ctx, db, cfg, quietLog())
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("worker did not stop")
		}
	})
}

// waitFor polls until a job reaches the wanted status, or fails the test.
// Polling rather than sleeping a fixed amount keeps tests both fast and robust.
func waitFor(t *testing.T, db *sql.DB, id, want string) Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last Job
	for time.Now().Before(deadline) {
		jobs, err := list(db, "", 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range jobs {
			if j.ID == id {
				last = j
			}
		}
		if last.Status == want {
			return last
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("job never reached %q (status=%q attempts=%d err=%q)",
		want, last.Status, last.Attempts, last.LastError)
	return last
}
