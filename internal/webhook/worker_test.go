package webhook

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// --- signing -----------------------------------------------------------------

// The signature is the security boundary of the system, so these are the
// adversarial cases: each one is a way a receiver could be tricked into trusting
// a forged delivery.
func TestSignAndVerify(t *testing.T) {
	ts := time.Now()
	payload := []byte(`{"amount":100}`)
	sig := sign("secret", ts, payload)
	tsHdr := strconv.FormatInt(ts.Unix(), 10)

	if err := verify("secret", sig, tsHdr, payload, time.Minute); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}

	bad := []struct {
		name    string
		secret  string
		sig     string
		ts      string
		payload []byte
	}{
		{"wrong secret", "other", sig, tsHdr, payload},
		// Intercept a delivery, change the amount, keep the signature.
		{"tampered payload", "secret", sig, tsHdr, []byte(`{"amount":999999}`)},
		// Reuse a valid signature with a fresh timestamp.
		{"tampered timestamp", "secret", sig, strconv.FormatInt(ts.Unix()+1, 10), payload},
		// A genuinely-signed delivery, captured and replayed an hour later.
		{"replayed (too old)", "secret", sign("secret", ts.Add(-time.Hour), payload),
			strconv.FormatInt(ts.Add(-time.Hour).Unix(), 10), payload},
		{"garbage timestamp", "secret", sig, "not-a-number", payload},
		{"empty signature", "secret", "", tsHdr, payload},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			if err := verify(tc.secret, tc.sig, tc.ts, tc.payload, time.Minute); err == nil {
				t.Fatal("expected verification to fail")
			}
		})
	}
}

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	base := time.Second
	var prev time.Duration
	for attempt := 1; attempt <= 6; attempt++ {
		d := backoff(attempt, base)
		if d <= 0 || d > backoffMax {
			t.Fatalf("attempt %d: delay %v out of range", attempt, d)
		}
		// Jitter makes exact values non-deterministic, so assert the property
		// that matters: the window grows until it hits the cap.
		if attempt > 1 && d < prev/2 {
			t.Errorf("attempt %d: delay %v shrank unexpectedly from %v", attempt, d, prev)
		}
		prev = d
	}
	// Regression guard: a naive `base << attempt` overflows int64 and wraps
	// negative at high attempt counts, which would retry instantly in a tight
	// loop. Nothing in normal operation would catch that.
	for _, attempt := range []int{40, 64, 1000} {
		if d := backoff(attempt, base); d <= 0 || d > backoffMax {
			t.Errorf("backoff(%d) = %v; must stay within (0, %v]", attempt, d, backoffMax)
		}
	}
}

// --- delivery, end to end ----------------------------------------------------

func TestDeliversAndSignsRequest(t *testing.T) {
	type got struct {
		id, event, attempt, body string
		sigOK                    bool
	}
	received := make(chan got, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		// Verify exactly as a real client would, which proves the scheme works
		// on the wire rather than just that sign() is self-consistent.
		err := verify("test-secret", r.Header.Get(hdrSignature), r.Header.Get(hdrTimestamp), body, time.Minute)
		received <- got{r.Header.Get(hdrID), r.Header.Get(hdrEvent), r.Header.Get(hdrAttempt), string(body), err == nil}
	}))
	defer srv.Close()

	db := testDB(t)
	j := newTestJob(srv.URL, 3)
	if err := insert(db, j); err != nil {
		t.Fatal(err)
	}
	startWorker(t, db, testCfg())

	select {
	case r := <-received:
		if !r.sigOK {
			t.Error("receiver could not verify the signature")
		}
		if r.id != j.ID || r.event != "payment.completed" || r.attempt != "1" {
			t.Errorf("headers = %+v", r)
		}
		// The payload is what we sign, so it must arrive byte for byte.
		if r.body != `{"amount":4200,"currency":"EUR"}` {
			t.Errorf("body = %s", r.body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no delivery arrived")
	}

	if final := waitFor(t, db, j.ID, statusSucceeded); final.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", final.Attempts)
	}
}

func TestRetriesThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Two retryable failures, then accept.
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	db := testDB(t)
	j := newTestJob(srv.URL, 5)
	if err := insert(db, j); err != nil {
		t.Fatal(err)
	}
	startWorker(t, db, testCfg())

	final := waitFor(t, db, j.ID, statusSucceeded)
	if final.Attempts != 3 {
		t.Errorf("attempts = %d, want 3", final.Attempts)
	}
	if n := calls.Load(); n != 3 {
		t.Errorf("destination called %d times, want 3", n)
	}
}

func TestDiesAfterMaxAttempts(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	db := testDB(t)
	j := newTestJob(srv.URL, 3)
	if err := insert(db, j); err != nil {
		t.Fatal(err)
	}
	startWorker(t, db, testCfg())

	final := waitFor(t, db, j.ID, statusDead)
	if final.Attempts != 3 {
		t.Errorf("attempts = %d, want exactly max_attempts (3)", final.Attempts)
	}
	if final.LastStatusCode != 500 || final.LastError == "" {
		t.Errorf("a dead job must record why it died: %+v", final)
	}

	// A dead job stays dead.
	before := calls.Load()
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != before {
		t.Error("destination was called again after the job died")
	}
}

// Fail-fast: a 4xx means the request itself is wrong, so burning the whole
// attempt budget retrying identical bytes is waste.
func TestNonRetryableResponseDiesImmediately(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusUnprocessableEntity)
	}))
	defer srv.Close()

	db := testDB(t)
	j := newTestJob(srv.URL, 5)
	if err := insert(db, j); err != nil {
		t.Fatal(err)
	}
	startWorker(t, db, testCfg())

	if final := waitFor(t, db, j.ID, statusDead); final.Attempts != 1 {
		t.Errorf("attempts = %d, want 1 (a 422 must not be retried)", final.Attempts)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("destination called %d times, want 1", n)
	}
}

// Graceful shutdown: a worker mid-delivery finishes the attempt and records the
// result, rather than aborting it — aborting would risk turning a delivery the
// receiver already processed into a duplicate on the next run.
func TestShutdownFinishesInFlightDelivery(t *testing.T) {
	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		time.Sleep(150 * time.Millisecond) // still working when shutdown arrives
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	db := testDB(t)
	j := newTestJob(srv.URL, 3)
	if err := insert(db, j); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runWorker(ctx, db, testCfg(), quietLog())
	}()

	<-started // the request is on the wire...
	cancel()  // ...now ask the process to stop

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not exit")
	}

	// runWorker only returns after finishing, so the outcome is already durable
	// with no polling needed here.
	jobs, err := list(db, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].Status != statusSucceeded {
		t.Fatalf("status = %q, want succeeded: the in-flight delivery was cut short", jobs[0].Status)
	}
}
