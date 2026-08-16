// Package webhook implements a webhook delivery service: it accepts jobs over
// HTTP, stores them durably in SQLite, and delivers them concurrently with HMAC
// signatures and retries.
//
// The whole service lives in this one package, split by responsibility:
//
//	run.go      configuration, wiring, signal handling, shutdown order
//	api.go      the HTTP ingest and inspection endpoints
//	store.go    SQLite — the durable queue, and the only file that knows SQL
//	worker.go   the delivery loop: claim, POST, sign, retry
//	receiver.go a test receiver, used only for local demos and tests
//
// It exports just two entry points, Run and RunReceiver, one per binary under
// cmd/. Everything else is unexported: at this size the boundary worth enforcing
// is around the *service*, not between its internal layers, so the internals can
// be reorganised without breaking any caller. store.go is the seam that would
// matter if this grew — swapping SQLite for Postgres touches that file alone.
package webhook

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type config struct {
	addr   string
	dbPath string
	secret string // HMAC key used to sign every delivery

	// workers is the delivery parallelism: each worker handles one job at a time.
	workers     int
	maxAttempts int

	// These two are fields rather than constants only so tests can compress them
	// to milliseconds; nothing reads them from the environment.
	backoffBase     time.Duration
	deliveryTimeout time.Duration
}

// Constants rather than env vars. Every knob is another thing to document,
// validate and explain, so only the two the challenge actually calls for
// (parallelism and the attempt budget) are tunable, plus the deployment basics.
const (
	pollInterval    = 500 * time.Millisecond // how often an idle worker looks for work
	backoffBase     = 2 * time.Second        // first retry delay; doubles from here
	backoffMax      = 5 * time.Minute        // ceiling on the exponential backoff
	deliveryTimeout = 10 * time.Second       // per attempt; also bounds shutdown
	maxPayloadSize  = 1 << 20                // 1MB, rejected at ingest
)

func loadConfig() (config, error) {
	cfg := config{
		addr:            env("ADDR", ":8080"),
		dbPath:          env("DB_PATH", "data/webhooks.db"),
		secret:          os.Getenv("WEBHOOK_SECRET"),
		workers:         envInt("WORKERS", 8),
		maxAttempts:     envInt("MAX_ATTEMPTS", 5),
		backoffBase:     backoffBase,
		deliveryTimeout: deliveryTimeout,
	}
	// Refusing to boot without a secret is intentional: a generated-per-boot
	// secret would break every receiver's verification after a restart, and the
	// failure would look like a signing bug instead of a config mistake.
	if cfg.secret == "" {
		return cfg, errors.New("WEBHOOK_SECRET is required")
	}
	return cfg, nil
}

// Run starts the dispatcher: the HTTP API and the delivery workers. It blocks
// until SIGINT/SIGTERM, then shuts down gracefully.
//
// It returns an error rather than calling os.Exit so that deferred cleanup (most
// importantly closing the database) always runs; os.Exit in a caller's main
// would skip it.
func Run(log *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	db, err := openDB(cfg.dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	// Any job left "delivering" belongs to a process that is gone (SIGKILL, OOM,
	// power loss), so it is unreachable until someone resets it. Doing this at
	// startup is what makes a hard kill cost a retry instead of a lost job.
	if n, err := recoverStuck(db); err != nil {
		return err
	} else if n > 0 {
		log.Warn("requeued jobs orphaned by a previous crash", "count", n)
	}

	// SIGTERM (docker stop) and SIGINT (Ctrl-C) both become a cancelled context.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{
		Addr:    cfg.addr,
		Handler: (&api{db: db, cfg: cfg, log: log}).routes(),
		// Without timeouts a single slow client can hold a connection and its
		// goroutine open forever.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
	}

	var wg sync.WaitGroup
	for i := range cfg.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWorker(ctx, db, cfg, log.With("worker", i))
		}()
	}

	go func() {
		log.Info("listening", "addr", cfg.addr, "workers", cfg.workers)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "error", err)
			stop() // a process that cannot accept jobs should not linger half-alive
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	// Order matters. Stop the HTTP server FIRST: Shutdown drains in-flight
	// requests, so every job that was answered with 202 is already committed to
	// disk. Then wait for the workers, which finish their current delivery and
	// exit (see runWorker). The deferred db.Close() runs last, once nobody can
	// touch it.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "error", err)
	}

	wg.Wait()
	log.Info("shutdown complete")
	return nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envInt falls back to the default on a malformed value. These are operational
// knobs with safe defaults; the one value where a mistake is unrecoverable (the
// secret) is validated explicitly in loadConfig.
func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return def
}
