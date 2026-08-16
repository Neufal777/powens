package webhook

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	// Pure-Go SQLite driver, so CGO_ENABLED=0 still works and the Docker image
	// needs no C toolchain. mattn/go-sqlite3 would require cgo.
	_ "modernc.org/sqlite"
)

// Job is a webhook waiting to be delivered.
//
// The queue is this table, not a broker. A row carries its own status, attempt
// count and next_attempt_at, which means the backoff schedule and the "who owns
// this job" lease both survive a restart — no in-memory state to lose.
//
// Status lifecycle:
//
//	pending -> delivering -> succeeded          (2xx)
//	                      -> pending            (retryable failure, backoff set)
//	                      -> dead               (attempts exhausted, or a 4xx)
type Job struct {
	ID             string          `json:"id"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	DestinationURL string          `json:"destination_url"`
	Status         string          `json:"status"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	NextAttemptAt  time.Time       `json:"next_attempt_at"`
	LastError      string          `json:"last_error,omitempty"`
	LastStatusCode int             `json:"last_status_code,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

const (
	statusPending    = "pending"
	statusDelivering = "delivering"
	statusSucceeded  = "succeeded"
	statusDead       = "dead"
)

const cols = `id, event_type, payload, destination_url, status, attempts,
	max_attempts, next_attempt_at, last_error, last_status_code, created_at`

func openDB(path string) (*sql.DB, error) {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	// Two pragmas carry real weight here:
	//   synchronous(FULL) fsyncs on commit. This is what makes the 202 response
	//     an honest promise — the job is on disk before we answer. NORMAL is
	//     faster but can lose the last commits on power loss.
	//   journal_mode(WAL) keeps readers from blocking the writer.
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}

	// SQLite allows one writer at a time and this service writes on every
	// attempt, so serialising access through a single connection is simpler and
	// faster than letting N connections fight over SQLITE_BUSY. This is also the
	// design's scalability ceiling — see the README on moving to Postgres.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS jobs (
			id               TEXT PRIMARY KEY,
			event_type       TEXT NOT NULL,
			payload          BLOB NOT NULL,
			destination_url  TEXT NOT NULL,
			status           TEXT NOT NULL,
			attempts         INTEGER NOT NULL DEFAULT 0,
			max_attempts     INTEGER NOT NULL,
			next_attempt_at  INTEGER NOT NULL,
			last_error       TEXT NOT NULL DEFAULT '',
			last_status_code INTEGER NOT NULL DEFAULT 0,
			created_at       INTEGER NOT NULL
		);
		-- The hot query is "pending jobs whose timer expired, oldest first".
		CREATE INDEX IF NOT EXISTS ix_claim ON jobs (status, next_attempt_at);`); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return db, nil
}

// insert stores a new job. It returns once the row is durably on disk.
func insert(db *sql.DB, j Job) error {
	_, err := db.Exec(`INSERT INTO jobs (`+cols+`) VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		j.ID, j.EventType, []byte(j.Payload), j.DestinationURL, j.Status, j.Attempts,
		j.MaxAttempts, ms(j.NextAttemptAt), j.LastError, j.LastStatusCode, ms(j.CreatedAt))
	return err
}

// claimOne atomically leases the next due job to the calling worker.
//
// This single UPDATE ... RETURNING is the entire locking strategy: selecting the
// row, flipping it to "delivering" and reading it back happen in one statement,
// so two workers can never get the same job. (In Postgres the equivalent is
// SELECT ... FOR UPDATE SKIP LOCKED.)
//
// Note it does not touch `attempts`: an attempt is only counted once it finishes,
// so a worker that dies mid-delivery does not silently burn the job's budget.
func claimOne(db *sql.DB, now time.Time) (Job, bool, error) {
	row := db.QueryRow(`
		UPDATE jobs SET status = ?
		WHERE id = (
			SELECT id FROM jobs
			WHERE status = ? AND next_attempt_at <= ?
			ORDER BY next_attempt_at, created_at
			LIMIT 1
		)
		RETURNING `+cols,
		statusDelivering, statusPending, ms(now))

	j, err := scan(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, nil // queue is empty, not an error
	}
	return j, err == nil, err
}

// update records the outcome of an attempt: succeeded, dead, or back to pending
// for a retry at `next`.
//
// One function for all three because they are the same UPDATE — the only
// difference is which status goes in and whether `next` matters (it is ignored
// for terminal states, since nothing will ever claim them again). Keeping it as
// one statement means the caller has a single error path instead of three.
func update(db *sql.DB, id, status string, attempts, code int, errMsg string, next time.Time) error {
	_, err := db.Exec(
		`UPDATE jobs SET status=?, attempts=?, last_status_code=?, last_error=?, next_attempt_at=? WHERE id=?`,
		status, attempts, code, trunc(errMsg, 300), ms(next), id)
	return err
}

// recoverStuck requeues jobs whose worker died without finishing them. Called
// once at startup; this is the crash-recovery half of at-least-once delivery.
func recoverStuck(db *sql.DB) (int, error) {
	res, err := db.Exec(`UPDATE jobs SET status=? WHERE status=?`, statusPending, statusDelivering)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// list returns jobs newest-first, optionally filtered by status. Passing
// status="dead" is how dead jobs are exposed — the dead-letter queue is just a
// query, so it cannot drift out of sync with the queue it came from.
func list(db *sql.DB, status string, limit int) ([]Job, error) {
	q := `SELECT ` + cols + ` FROM jobs`
	args := []any{}
	if status != "" {
		q += ` WHERE status = ?`
		args = append(args, status)
	}
	q += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := []Job{}
	for rows.Next() {
		j, err := scan(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// scanner lets scan() work with both *sql.Row and *sql.Rows.
type scanner interface{ Scan(...any) error }

func scan(s scanner) (Job, error) {
	var j Job
	var payload []byte
	var next, created int64
	err := s.Scan(&j.ID, &j.EventType, &payload, &j.DestinationURL, &j.Status,
		&j.Attempts, &j.MaxAttempts, &next, &j.LastError, &j.LastStatusCode, &created)
	// Copy the payload: the driver may reuse the backing array on the next scan.
	j.Payload = append(json.RawMessage(nil), payload...)
	j.NextAttemptAt, j.CreatedAt = at(next), at(created)
	return j, err
}

// Timestamps are stored as Unix milliseconds. Integers index and compare
// correctly with no text-format or timezone surprises.
func ms(t time.Time) int64 { return t.UnixMilli() }
func at(v int64) time.Time { return time.UnixMilli(v).UTC() }

// trunc bounds text we did not write: some servers answer with megabyte-sized
// HTML error pages, and that belongs neither in the database nor in a log line.
func trunc(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}
