package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

// Headers sent on every delivery.
const (
	hdrSignature = "X-Webhook-Signature"
	hdrTimestamp = "X-Webhook-Timestamp"
	hdrID        = "X-Webhook-Id"
	hdrEvent     = "X-Webhook-Event"
	hdrAttempt   = "X-Webhook-Attempt"
)

// client is shared by all workers so they reuse TCP/TLS connections. A client
// per request would open a new connection every time and leak file descriptors.
var client = &http.Client{
	// Do not follow redirects: destination URLs are customer-supplied, and
	// following a 302 would let one point us at an internal address (SSRF) or
	// silently move deliveries elsewhere. Treat it as a misconfiguration.
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// runWorker is one unit of delivery parallelism: claim a job, deliver it, repeat.
// WORKERS of these run concurrently, which is the whole concurrency model.
//
// Why this rather than a poller feeding a channel of workers: a claimed job is a
// *lease* — invisible to everyone else — so a job sitting in a channel buffer is
// work in limbo that shutdown would have to explicitly hand back. Claiming one
// job at a time per worker means we never hold a lease we are not acting on, and
// there is no queue to drain. The cost is one small query per idle worker per
// poll interval, which at these volumes is free.
func runWorker(ctx context.Context, db *sql.DB, cfg config, log *slog.Logger) {
	for {
		// Checked here rather than mid-delivery: a worker stops picking up NEW
		// work as soon as we are shutting down, but always finishes the job it
		// already has.
		if ctx.Err() != nil {
			return
		}

		j, ok, err := claimOne(db, time.Now())
		if err != nil {
			log.Error("claim failed", "error", err)
		}
		if !ok {
			// Nothing to do. Sleep, but wake immediately on shutdown.
			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
			}
			continue
		}

		handle(db, cfg, log, j)
	}
}

// handle performs one attempt and records the outcome.
//
// It decides the new state first, then persists it once. Writing the decision
// and the write as two separate steps keeps the retry policy readable and leaves
// exactly one place where persistence can fail.
func handle(db *sql.DB, cfg config, log *slog.Logger, j Job) {
	attempt := j.Attempts + 1

	// context.Background(), NOT the shutdown context, on purpose: aborting a
	// request we have already sent is the worst option available, because the
	// receiver may well have processed it and cancelling would turn a completed
	// delivery into a duplicate on every deploy. deliveryTimeout bounds it, so
	// this terminates — and it is what bounds shutdown time too.
	ctx, cancel := context.WithTimeout(context.Background(), cfg.deliveryTimeout)
	defer cancel()

	code, retryable, err := send(ctx, cfg.secret, j, attempt)

	status, next, msg := statusSucceeded, time.Now(), ""
	switch {
	case err == nil:
		// delivered
	case !retryable || attempt >= j.MaxAttempts:
		status, msg = statusDead, err.Error()
	default:
		status, msg = statusPending, err.Error()
		next = time.Now().Add(backoff(attempt, cfg.backoffBase))
	}

	log = log.With("job_id", j.ID, "event", j.EventType, "attempt", attempt, "status", code)

	if uerr := update(db, j.ID, status, attempt, code, msg, next); uerr != nil {
		// If the delivery succeeded but this write failed, startup recovery will
		// re-deliver it. That is precisely the at-least-once window, and why
		// receivers must deduplicate on X-Webhook-Id.
		log.Error("could not record attempt outcome", "outcome", status, "error", uerr)
		return
	}

	switch status {
	case statusSucceeded:
		log.Info("delivered")
	case statusDead:
		reason := "attempts exhausted"
		if !retryable {
			reason = "non-retryable response"
		}
		log.Error("job dead", "reason", reason, "error", err)
	default:
		log.Warn("delivery failed, will retry",
			"retry_in", time.Until(next).Truncate(time.Millisecond), "error", err)
	}
}

// send POSTs the payload and classifies the response.
//
// retryable answers "is it worth trying the identical request again?"
func send(ctx context.Context, secret string, j Job, attempt int) (code int, retryable bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, j.DestinationURL, bytes.NewReader(j.Payload))
	if err != nil {
		return 0, false, err // a URL this broken will never work
	}

	ts := time.Now()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(hdrID, j.ID)
	req.Header.Set(hdrEvent, j.EventType)
	req.Header.Set(hdrAttempt, strconv.Itoa(attempt))
	req.Header.Set(hdrTimestamp, strconv.FormatInt(ts.Unix(), 10))
	req.Header.Set(hdrSignature, sign(secret, ts, j.Payload))

	resp, err := client.Do(req)
	if err != nil {
		// DNS failure, refused connection, timeout, reset: the classic transient
		// cases, all worth retrying.
		return 0, true, err
	}
	defer resp.Body.Close()

	// Read a bounded amount of the body so the connection can be reused, and
	// keep a snippet for diagnostics.
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	io.Copy(io.Discard, resp.Body)

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return resp.StatusCode, false, nil

	case resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500:
		// 5xx is the receiver's problem and usually temporary; 408 and 429 are
		// explicit "come back later" signals.
		return resp.StatusCode, true, respErr(resp.StatusCode, body)

	default:
		// Other 4xx means the request itself is wrong (bad path, rejected
		// signature, malformed body). Retrying identical bytes against a
		// deterministic rejection is waste, so fail fast to the dead-letter
		// queue where a human can look at it.
		return resp.StatusCode, false, respErr(resp.StatusCode, body)
	}
}

func respErr(code int, body []byte) error {
	return fmt.Errorf("status %d: %s", code, trunc(string(bytes.TrimSpace(body)), 120))
}

// backoff returns how long to wait before the next attempt:
// exponential, capped, with jitter.
//
// Exponential because a receiver that is down needs time to recover — retrying
// at a constant rate is a self-inflicted DoS on something already struggling.
// Jitter because failures come in correlated batches (one outage fails
// everything in flight); without it they retry in lockstep forever and hit the
// recovering receiver as a thundering herd.
func backoff(attempt int, base time.Duration) time.Duration {
	d := base
	// Loop-and-cap rather than base << attempt: shifting overflows int64 at high
	// attempt counts and wraps negative, which would retry instantly in a tight
	// loop.
	for range attempt - 1 {
		d *= 2
		if d >= backoffMax {
			d = backoffMax
			break
		}
	}
	return d/2 + rand.N(d/2) // jitter: somewhere in [d/2, d)
}

// sign returns the HMAC of "<unix-seconds>.<payload>", hex-encoded.
//
// The timestamp is signed along with the body (the Stripe/GitHub scheme) rather
// than the body alone: a payload-only signature is valid forever, so anyone who
// captures one delivery could replay it verbatim at any time. Because the
// timestamp is inside the MAC it cannot be edited in transit, which lets the
// receiver reject anything outside a tolerance window.
//
// The "v1=" prefix means the algorithm can be rotated later without breaking
// existing receivers.
func sign(secret string, ts time.Time, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%d.", ts.Unix())
	mac.Write(payload)
	return "v1=" + hex.EncodeToString(mac.Sum(nil))
}

// verify is the receiver side of sign(). It lives here so the test receiver and
// the tests exercise exactly the same code the dispatcher signs with.
func verify(secret, sigHeader, tsHeader string, payload []byte, tolerance time.Duration) error {
	secs, err := strconv.ParseInt(tsHeader, 10, 64)
	if err != nil {
		return fmt.Errorf("bad timestamp header: %w", err)
	}
	ts := time.Unix(secs, 0)
	// Reject stale (replayed) deliveries, and future ones too so a skewed clock
	// cannot mint long-lived signatures.
	if d := time.Since(ts); d > tolerance || d < -tolerance {
		return fmt.Errorf("timestamp outside tolerance (%s)", d.Truncate(time.Second))
	}
	// hmac.Equal is constant time. A plain == leaks how many leading bytes
	// matched, which is enough to forge a signature one byte at a time.
	if !hmac.Equal([]byte(sign(secret, ts, payload)), []byte(sigHeader)) {
		return fmt.Errorf("signature mismatch")
	}
	return nil
}
