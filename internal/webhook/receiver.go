package webhook

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// RunReceiver runs a test webhook receiver, so deliveries can be watched
// end-to-end locally. It does three useful things:
//
//  1. Verifies the HMAC exactly as a real client would, which proves the signing
//     scheme is usable and not just implemented.
//  2. Logs duplicate deliveries by job id, making the at-least-once guarantee
//     observable instead of merely claimed.
//  3. Simulates failure modes, so retries and dead-lettering can be demonstrated
//     without hacking the dispatcher.
func RunReceiver(log *slog.Logger) error {
	// Must match the dispatcher's secret, or every delivery fails verification.
	secret := os.Getenv("WEBHOOK_SECRET")
	if secret == "" {
		return errors.New("WEBHOOK_SECRET is required (must match the dispatcher's)")
	}
	addr := env("RECEIVER_ADDR", ":9090")

	// seen tracks job ids already accepted. This is what a real receiver must do
	// under at-least-once delivery — in production a unique index on a
	// processed_events table rather than a map, so it survives a restart.
	var mu sync.Mutex
	seen := map[string]int{}

	// handle wraps the shared work (verify, dedupe, log) around a per-endpoint
	// response code, so each simulated behaviour below is one line.
	handle := func(status func(attempt int) int) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(io.LimitReader(r.Body, maxPayloadSize))
			id := r.Header.Get(hdrID)
			attempt := r.Header.Get(hdrAttempt)

			// Verify BEFORE trusting the payload. 5 minutes of tolerance absorbs
			// clock skew between containers while still bounding replay.
			if err := verify(secret, r.Header.Get(hdrSignature), r.Header.Get(hdrTimestamp), body, 5*time.Minute); err != nil {
				log.Error("REJECTED: bad signature", "job_id", id, "error", err)
				// 401 is non-retryable by design: if the signature is wrong,
				// retrying identical bytes fails identically.
				http.Error(w, "invalid signature", http.StatusUnauthorized)
				return
			}

			n, _ := strconv.Atoi(attempt)
			if code := status(n); code >= 300 {
				log.Warn("simulated failure", "job_id", id, "attempt", attempt, "returning", code)
				http.Error(w, "simulated failure", code)
				return
			}

			// Only count deliveries we actually accept, so a retry after a
			// simulated failure is not reported as a duplicate.
			mu.Lock()
			seen[id]++
			times := seen[id]
			mu.Unlock()

			if times > 1 {
				log.Warn("DUPLICATE delivery (at-least-once at work; receivers must dedupe on X-Webhook-Id)",
					"job_id", id, "times_seen", times)
			}
			log.Info("RECEIVED", "job_id", id, "event", r.Header.Get(hdrEvent),
				"attempt", attempt, "signature", "valid", "payload", string(body))
			w.Write([]byte(`{"ok":true}`))
		}
	}

	mux := http.NewServeMux()
	// Always succeeds.
	mux.HandleFunc("POST /hook", handle(func(int) int { return 200 }))
	// Fails twice then succeeds: demonstrates backoff and automatic recovery.
	// Keyed on the attempt number so the demo is reproducible, not random.
	mux.HandleFunc("POST /hook/flaky", handle(func(attempt int) int {
		if attempt < 3 {
			return 503
		}
		return 200
	}))
	// Always fails: the job retries until its budget runs out, then goes dead.
	mux.HandleFunc("POST /hook/fail", handle(func(int) int { return 500 }))
	// A 4xx: non-retryable, so the job is dead-lettered on the first attempt.
	mux.HandleFunc("POST /hook/reject", handle(func(int) int { return 422 }))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})

	log.Info("test receiver listening", "addr", addr,
		"endpoints", "/hook /hook/flaky /hook/fail /hook/reject")
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return srv.ListenAndServe()
}
