package webhook

import (
	"testing"
	"time"
)

// The core safety property of the queue: a claimed job is leased exclusively, so
// two workers can never deliver the same job.
func TestClaimLeasesEachJobOnce(t *testing.T) {
	db := testDB(t)
	for range 3 {
		if err := insert(db, newTestJob("https://example.com/h", 3)); err != nil {
			t.Fatal(err)
		}
	}

	seen := map[string]bool{}
	for i := range 3 {
		j, ok, err := claimOne(db, time.Now())
		if err != nil || !ok {
			t.Fatalf("claim %d: ok=%v err=%v", i, ok, err)
		}
		if seen[j.ID] {
			t.Fatalf("job %s claimed twice", j.ID)
		}
		seen[j.ID] = true
		if j.Status != statusDelivering {
			t.Errorf("claimed job status = %q, want delivering", j.Status)
		}
	}
	// Everything is leased now, so there is nothing left to claim.
	if _, ok, _ := claimOne(db, time.Now()); ok {
		t.Error("claimed a job that was already leased")
	}
}

// Backoff is enforced by the claim query, so this covers the scheduling itself.
func TestClaimRespectsBackoffSchedule(t *testing.T) {
	db := testDB(t)
	j := newTestJob("https://example.com/h", 3)
	j.NextAttemptAt = time.Now().Add(time.Hour)
	if err := insert(db, j); err != nil {
		t.Fatal(err)
	}

	if _, ok, _ := claimOne(db, time.Now()); ok {
		t.Error("claimed a job whose backoff has not elapsed")
	}
	// Same job, clock advanced past its schedule.
	if _, ok, _ := claimOne(db, time.Now().Add(2*time.Hour)); !ok {
		t.Error("did not claim the job after its backoff elapsed")
	}
}

func TestTerminalJobsAreNeverRedelivered(t *testing.T) {
	db := testDB(t)
	for _, status := range []string{statusSucceeded, statusDead} {
		j := newTestJob("https://example.com/h", 3)
		if err := insert(db, j); err != nil {
			t.Fatal(err)
		}
		if _, _, err := claimOne(db, time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := update(db, j.ID, status, 1, 200, "", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok, _ := claimOne(db, time.Now().Add(time.Hour)); ok {
		t.Error("claimed a job that had already finished")
	}
}

// The crash-recovery guarantee: a SIGKILLed process leaves rows in "delivering",
// and the next boot must make them deliverable again rather than stranding them.
func TestRecoverStuckRequeuesOrphanedJobs(t *testing.T) {
	db := testDB(t)
	j := newTestJob("https://example.com/h", 3)
	if err := insert(db, j); err != nil {
		t.Fatal(err)
	}
	// Claim it and never finish it: this is what a hard kill looks like on disk.
	if _, _, err := claimOne(db, time.Now()); err != nil {
		t.Fatal(err)
	}

	n, err := recoverStuck(db)
	if err != nil || n != 1 {
		t.Fatalf("recovered %d jobs (err=%v), want 1", n, err)
	}
	// Idempotent: with nothing orphaned, a second run is a no-op.
	if n, _ := recoverStuck(db); n != 0 {
		t.Errorf("second run moved %d jobs, want 0", n)
	}
	if _, ok, _ := claimOne(db, time.Now()); !ok {
		t.Error("recovered job was not claimable; it would be stuck forever")
	}
}

// A job is only charged an attempt when one finishes, so claiming does not
// consume the budget — otherwise a worker that died mid-delivery would silently
// burn attempts the destination never actually saw.
func TestClaimDoesNotConsumeAnAttempt(t *testing.T) {
	db := testDB(t)
	if err := insert(db, newTestJob("https://example.com/h", 3)); err != nil {
		t.Fatal(err)
	}
	j, _, err := claimOne(db, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if j.Attempts != 0 {
		t.Errorf("attempts = %d after claiming, want 0", j.Attempts)
	}
}
