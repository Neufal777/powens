// Command dispatcher runs the webhook delivery service: the ingest API and the
// delivery workers in one process.
//
// This file is deliberately trivial. Everything a cmd/ main should do is here —
// build the logger, call the service, translate an error into an exit code — and
// nothing else, so the logic stays in internal/webhook where it is testable.
package main

import (
	"log/slog"
	"os"

	"github.com/naoufaldahouli/webhook-dispatcher/internal/webhook"
)

func main() {
	// JSON to stdout: the twelve-factor default, and what any log aggregator
	// expects from a container.
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := webhook.Run(log); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}
