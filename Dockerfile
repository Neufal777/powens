# One image containing both binaries (dispatcher + test receiver). They share the
# internal/webhook package, so building them together costs one extra link step
# and no duplicated code; the compose file picks which one to run.

FROM golang:1.26-alpine AS build
WORKDIR /src

# Manifests first: Docker caches this layer, so editing code does not re-download
# the module graph on every build.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Tests run in the build, so a broken commit cannot produce an image.
RUN go test ./...

# CGO_ENABLED=0 gives a static binary. It is only possible because we chose the
# pure-Go SQLite driver; mattn/go-sqlite3 would need a C toolchain here.
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/dispatcher ./cmd/dispatcher && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/receiver   ./cmd/receiver

FROM alpine:3.22
# ca-certificates is required to POST to https:// destinations; without it every
# TLS delivery fails with an x509 error that is confusing to debug.
RUN apk add --no-cache ca-certificates wget && adduser -D -u 10001 app
COPY --from=build /out/dispatcher /out/receiver /usr/local/bin/

# The data dir holds the SQLite file, i.e. every accepted job, and must be
# writable by the unprivileged user.
WORKDIR /app
RUN mkdir -p data && chown -R app:app /app

# Run unprivileged: this service makes outbound requests to customer-supplied
# URLs, so it should have as little privilege as possible.
USER app

EXPOSE 8080
ENTRYPOINT ["dispatcher"]
