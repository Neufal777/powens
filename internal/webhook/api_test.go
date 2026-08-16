package webhook

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testAPI(t *testing.T) (http.Handler, *sql.DB) {
	t.Helper()
	db := testDB(t)
	return (&api{db: db, cfg: testCfg(), log: quietLog()}).routes(), db
}

func post(h http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", "/webhooks", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCreateAcceptsAndPersistsBeforeResponding(t *testing.T) {
	h, db := testAPI(t)
	rec := post(h, `{"event_type":"payment.completed","payload":{"amount":1000},
		"destination_url":"https://example.com/hook"}`)

	// 202, not 200: we have accepted responsibility for delivering it later.
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", rec.Code, rec.Body)
	}
	var j Job
	if err := json.Unmarshal(rec.Body.Bytes(), &j); err != nil {
		t.Fatal(err)
	}
	if j.ID == "" || j.Status != statusPending || j.MaxAttempts != 3 {
		t.Errorf("unexpected job: %+v", j)
	}

	// The durability claim: by the time the client holds its 202, the job is on
	// disk. This is what makes the accepted-jobs-are-never-lost promise real.
	jobs, err := list(db, statusPending, 10)
	if err != nil || len(jobs) != 1 || jobs[0].ID != j.ID {
		t.Fatalf("job not persisted at response time: %v %+v", err, jobs)
	}
	if string(jobs[0].Payload) != `{"amount":1000}` {
		t.Errorf("payload = %s, want it stored byte-for-byte", jobs[0].Payload)
	}
}

func TestCreateRejectsBadRequests(t *testing.T) {
	h, _ := testAPI(t)
	cases := []struct{ name, body string }{
		{"malformed json", `{"event_type":`},
		{"empty body", ``},
		{"no event_type", `{"payload":{"a":1},"destination_url":"https://x.com/h"}`},
		{"no payload", `{"event_type":"a.b","destination_url":"https://x.com/h"}`},
		{"no destination", `{"event_type":"a.b","payload":{"a":1}}`},
		// file:// and friends are the classic SSRF escalation from a
		// customer-supplied destination URL.
		{"file:// scheme", `{"event_type":"a.b","payload":{"a":1},"destination_url":"file:///etc/passwd"}`},
		{"relative url", `{"event_type":"a.b","payload":{"a":1},"destination_url":"/path"}`},
		{"no host", `{"event_type":"a.b","payload":{"a":1},"destination_url":"https://"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if rec := post(h, tc.body); rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400 (%s)", rec.Code, rec.Body)
			}
		})
	}
}

// GET /webhooks?status=dead is the dead-letter queue, so this covers both.
func TestListFiltersByStatus(t *testing.T) {
	h, db := testAPI(t)
	post(h, `{"event_type":"a.b","payload":{"a":1},"destination_url":"https://x.com/h"}`)

	// Drive the job to dead the way a worker would.
	jobs, _ := list(db, "", 10)
	if _, _, err := claimOne(db, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := update(db, jobs[0].ID, statusDead, 3, 500, "gave up", time.Now()); err != nil {
		t.Fatal(err)
	}

	get := func(query string) (code, count int) {
		req := httptest.NewRequest("GET", "/webhooks"+query, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		var out struct{ Count int }
		json.Unmarshal(rec.Body.Bytes(), &out)
		return rec.Code, out.Count
	}

	if code, n := get("?status=dead"); code != 200 || n != 1 {
		t.Errorf("dead list: code=%d count=%d, want 200/1", code, n)
	}
	if code, n := get("?status=succeeded"); code != 200 || n != 0 {
		t.Errorf("succeeded list: code=%d count=%d, want 200/0", code, n)
	}
	if code, n := get(""); code != 200 || n != 1 {
		t.Errorf("unfiltered list: code=%d count=%d, want 200/1", code, n)
	}
	// A typo'd filter must be an error, not a silently empty list that hides the
	// problem from whoever is debugging.
	if code, _ := get("?status=bogus"); code != http.StatusBadRequest {
		t.Errorf("bogus status: code=%d, want 400", code)
	}
}
