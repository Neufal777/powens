.PHONY: up down logs test demo dead clean

API  = http://localhost:8080
# The destination is resolved from inside the dispatcher container, so it uses
# the compose service name rather than localhost.
HOOK = http://receiver:9090/hook

up:            ## build and start dispatcher + test receiver
	docker compose up --build -d

down:          ## stop and remove the volume
	docker compose down -v

logs:          ## follow logs from both services
	docker compose logs -f

test:          ## run the tests
	go test -race ./...

# One job per delivery path: succeeds, recovers after retries, dies after
# exhausting attempts, and dies immediately on a non-retryable 4xx.
demo: up       ## enqueue one job for each delivery outcome
	@sleep 2
	@$(MAKE) -s send PATH_=""        LABEL=succeeds-first-try
	@$(MAKE) -s send PATH_=/flaky    LABEL=succeeds-after-2-retries
	@$(MAKE) -s send PATH_=/fail     LABEL=dies-after-max-attempts
	@$(MAKE) -s send PATH_=/reject   LABEL=dies-immediately-422
	@echo
	@echo "Watch it happen:  make logs"
	@echo "Dead-letter queue: make dead   (the failing job needs ~30s to exhaust retries)"

send:
	@curl -sS -X POST $(API)/webhooks -H 'Content-Type: application/json' \
		-d '{"event_type":"payment.completed","payload":{"amount":4200,"case":"$(LABEL)"},"destination_url":"$(HOOK)$(PATH_)"}' \
		-o /dev/null -w '%{http_code} $(LABEL)\n'

dead:          ## show dead-lettered jobs
	@curl -sS '$(API)/webhooks?status=dead' | python3 -m json.tool

clean:         ## remove the local database
	rm -rf data
