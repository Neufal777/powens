// Command receiver is a test webhook receiver for local demos and manual
// testing. It is not part of the production service.
//
// It exists as its own binary (rather than a --receiver flag on the dispatcher)
// now that the logic lives in internal/webhook: both binaries import the same
// signing code, so there is no duplication to avoid, and keeping them separate
// means the dispatcher image has no test-only endpoints in it.
package main

import (
	"log/slog"
	"os"

	"github.com/naoufaldahouli/webhook-dispatcher/internal/webhook"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := webhook.RunReceiver(log); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}
