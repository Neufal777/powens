package webhook

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"time"
)

// api serves the ingest and inspection endpoints.
//
// HTTP+JSON rather than gRPC: the producers are services, but the people who
// need to inspect the dead-letter queue at 3am are humans, and with HTTP that is
// curl. We already depend on net/http for outbound deliveries, so this adds
// nothing, and JSON payloads pass through untouched (in protobuf they would be
// an opaque bytes field anyway, losing the schema benefit that justifies gRPC).
// At one small write per event, per-request overhead is irrelevant.
type api struct {
	db  *sql.DB
	cfg config
	log *slog.Logger
}

func (a *api) routes() http.Handler {
	// Go 1.22+ method+pattern routing, so no third-party router is needed.
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhooks", a.create)
	mux.HandleFunc("GET /webhooks", a.list)
	// Liveness only — deliberately does not touch the database, so a slow query
	// cannot get a healthy process killed.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})
	return mux
}

func (a *api) create(w http.ResponseWriter, r *http.Request) {
	var req struct {
		EventType      string          `json:"event_type"`
		Payload        json.RawMessage `json:"payload"`
		DestinationURL string          `json:"destination_url"`
		MaxAttempts    int             `json:"max_attempts,omitempty"`
	}

	// Bound the read before parsing, so a client cannot stream gigabytes into
	// memory before we validate anything.
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxPayloadSize)).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.EventType == "" {
		fail(w, http.StatusBadRequest, "event_type is required")
		return
	}
	if len(req.Payload) == 0 || !json.Valid(req.Payload) {
		fail(w, http.StatusBadRequest, "payload is required and must be valid JSON")
		return
	}
	u, err := url.Parse(req.DestinationURL)
	// Require an absolute http(s) URL: this blocks file://, gopher:// and the
	// other classic SSRF escalations from a customer-supplied URL. Full defence
	// (private-IP denylist, pinned DNS) is listed in the README as a gap.
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		fail(w, http.StatusBadRequest, "destination_url must be an absolute http(s) URL")
		return
	}
	if req.MaxAttempts <= 0 {
		req.MaxAttempts = a.cfg.maxAttempts
	}

	now := time.Now().UTC()
	j := Job{
		ID:             newID(),
		EventType:      req.EventType,
		Payload:        req.Payload,
		DestinationURL: u.String(),
		Status:         statusPending,
		MaxAttempts:    req.MaxAttempts,
		NextAttemptAt:  now, // deliver as soon as a worker is free
		CreatedAt:      now,
	}

	if err := insert(a.db, j); err != nil {
		a.log.Error("insert failed", "error", err)
		fail(w, http.StatusInternalServerError, "could not enqueue job")
		return
	}

	// 202 rather than 200: the job is fsynced to disk (synchronous=FULL) but not
	// yet delivered. From here the system owns it.
	writeJSON(w, http.StatusAccepted, j)
}

// list also serves as the dead-letter queue: GET /webhooks?status=dead.
func (a *api) list(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	switch status {
	case "", statusPending, statusDelivering, statusSucceeded, statusDead:
	default:
		// A typo'd filter must be an error, not a silently empty list that hides
		// the problem from whoever is debugging.
		fail(w, http.StatusBadRequest, "status must be pending, delivering, succeeded or dead")
		return
	}

	jobs, err := list(a.db, status, 100)
	if err != nil {
		a.log.Error("list failed", "error", err)
		fail(w, http.StatusInternalServerError, "could not list jobs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(jobs), "jobs": jobs})
}

// newID: 16 random bytes is the same entropy as a UUIDv4, without the dependency.
// The prefix makes ids self-describing in logs.
func newID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return "wh_" + hex.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
