# Goose — Go Firehose Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a lightweight Kafka-to-HTTP firehose in Go that replaces the heavy Java-based raystack firehose, with ~15MB binary, ~20-30MB RAM, channel-based backpressure, connection TTL, circuit breaker, and Prometheus+OTel observability.

**Architecture:** Worker pool model — 1 consumer goroutine polls Kafka, N worker goroutines process batches via a buffered Go channel (natural backpressure). Contiguity-gated offset manager ensures at-least-once delivery. Config-driven error routing (retry/DLQ/ignore) with circuit breaker.

**Tech Stack:** Go 1.22+, segmentio/kafka-go, net/http, prometheus/client_golang, opentelemetry-go, oliveagle/jsonpath, google.golang.org/protobuf

**Directories:**
- `/Users/arelligoutham/Documents/goose/` — the firehose service (production code)
- `/Users/arelligoutham/Documents/goose-integration-test/` — integration test infra (Docker Compose, test helpers)

**Design spec:** `firehose/docs/superpowers/specs/2026-09-22-go-firehose-design.md`

---

## File Structure

```
goose/
├── cmd/goose/main.go                ← entry point, wiring, graceful shutdown
├── internal/
│   ├── config/config.go             ← env-var config + StatusRange parsing
│   ├── consumer/consumer.go         ← Kafka consumer goroutine
│   ├── worker/{types,worker}.go     ← batch type + N worker goroutines
│   ├── offset/offsetmanager/        ← contiguity-gated offset commit
│   ├── filter/filter.go             ← JSON-path filter + NoOp
│   ├── sink/{sink,http_sink}.go     ← Sink interface + HTTP impl (TTL, batch/individual)
│   ├── error/                       ← error types, retry, circuit breaker, error handler, DLQ
│   ├── metrics/metrics.go           ← Prometheus metrics
│   ├── tracing/tracing.go           ← OpenTelemetry traces
│   └── schema/schema.go             ← JSON passthrough + protobuf descriptor fetching
├── go.mod / go.sum
├── Dockerfile                       ← multi-stage: golang:1.22-alpine → distroless
├── Makefile
└── helm/
    ├── Chart.yaml / values.yaml
    └── templates/{deployment,service,configmap}.yaml

goose-integration-test/
├── docker-compose.yml               ← Kafka + MockServer + Prometheus + Jaeger
├── prometheus.yml
├── go.mod
└── *_test.go                        ← end-to-end integration tests
```

---

## Phase 1: Project Bootstrap & Config

### Task 1: Initialize Go Module and Directory Structure

**Files:**
- Create: `/Users/arelligoutham/Documents/goose/go.mod`
- Create: `.gitignore`, `Makefile`

- [ ] **Step 1: Create goose directory and init module**

```bash
mkdir -p /Users/arelligoutham/Documents/goose
cd /Users/arelligoutham/Documents/goose
go mod init github.com/arelligoutham/goose
```

- [ ] **Step 2: Create directory structure**

```bash
cd /Users/arelligoutham/Documents/goose
mkdir -p cmd/goose internal/{config,consumer,worker,offset/offsetmanager,filter,sink,error,metrics,tracing,schema}
```

- [ ] **Step 3: Create .gitignore**

```
goose
/bin/
*.test
coverage.out
.DS_Store
.idea/
.vscode/
```

- [ ] **Step 4: Create Makefile**

```makefile
.PHONY: build test run clean lint docker
build:
	go build -o bin/goose ./cmd/goose
test:
	go test -v -race ./...
run:
	go run ./cmd/goose
clean:
	rm -rf bin/ coverage.out
lint:
	go vet ./...
docker:
	docker build -t goose:latest .
```

- [ ] **Step 5: Init git and commit**

```bash
cd /Users/arelligoutham/Documents/goose
git init
git add -A
git commit -m "chore: initialize goose project structure

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

### Task 2: Configuration Package

**Files:**
- Create: `internal/config/config.go`, `internal/config/config_test.go`

- [ ] **Step 1: Write failing test** — test defaults (WorkerPoolSize=10, RequestTimeoutMs=10000, ConnectionTtlMs=30000, etc.), env override (SOURCE_KAFKA_BROKERS, SINK_HTTP_SERVICE_URL), validation (missing required → error), and StatusRange parsing ("500-599" matches 503 but not 404, "429" matches 429, comma-separated lists).

- [ ] **Step 2: Run test → FAIL** (`go test -v ./internal/config/`)

- [ ] **Step 3: Write config.go** — `Config` struct with `KafkaConfig`, `HTTPConfig`, `SchemaConfig`, `MetricsConfig`, `TracingConfig` sub-structs. All fields loaded from env vars via `os.LookupEnv` with defaults matching the spec's full config reference. `StatusRange` struct with `Matches(code int) bool`, `StatusRangeList` with `Matches`, `ParseStatusRange("500-599")`, `ParseStatusRangeList("500-599,429,408")`.

- [ ] **Step 4: Write env.go** — `lookupEnv` wrapper around `os.LookupEnv`, plus `getEnv`, `getEnvInt`, `getEnvFloat`, `getEnvBool` helpers.

- [ ] **Step 5: Run test → PASS**

- [ ] **Step 6: Commit** — `feat: add config package with env-var-driven configuration`

---

## Phase 2: Offset Manager

### Task 3: Offset Manager — Contiguity-Gated Commit

**Files:**
- Create: `internal/offset/offsetmanager/offset_manager.go`, `offset_manager_test.go`

- [ ] **Step 1: Write failing test** — 5 tests covering: (1) AddBatch + SetCommittable → GetCommittable returns offset+1, (2) contiguity gating: batch-2 done before batch-1 → nothing committable, then batch-1 done → both commit, (3) multiple partitions independent, (4) AddOffsetsAndSetCommittable for filtered msgs, (5) GetCommittable idempotent.

- [ ] **Step 2: Run test → FAIL**

- [ ] **Step 3: Write implementation** — `OffsetManager` with `sync.Mutex`, `batches map[string][]offsetNode` (batchID → offsets), `sortedOffsets map[TopicPartition][]offsetNode` (sorted by offset). `offsetNode` has `tp TopicPartition`, `offset int64`, `committable bool`. `AddBatch`, `SetCommittable` (mark matching offsets in sortedOffsets), `AddOffsetsAndSetCommittable`, `GetCommittable` (walk sorted offsets from lowest, collect contiguous committable prefix, return offset+1).

- [ ] **Step 4: Run test → PASS**

- [ ] **Step 5: Commit** — `feat: add offset manager with contiguity-gated commit logic`

---

## Phase 3: Error Handling

### Task 4: Error Types and Status Classification

**Files:**
- Create: `internal/error/error_types.go`, `error_types_test.go`

- [ ] **Step 1: Write failing test** — `ErrorTypeFromStatusCode(200)=None`, `(404)=Sink4xx`, `(503)=Sink5xx`. Verify `FailedMessage` struct has `ErrorInfo` with `ErrorType`, `StatusCode`, `Message`.

- [ ] **Step 2: Run test → FAIL**

- [ ] **Step 3: Write implementation** — `ErrorType` string enum (None, Sink4xx, Sink5xx, Deserialization, InvalidMessage, UnknownFields, SinkUnknown, Default). `FailedMessage` struct with `Topic`, `Partition`, `Offset`, `Key`, `Value`, `Retried bool`, `ErrorInfo`. `ErrorTypeFromStatusCode(code int)` classifies by range.

- [ ] **Step 4: Run test → PASS**

- [ ] **Step 5: Commit** — `feat: add error types and status code classification`

---

### Task 5: Exponential Backoff Retry

**Files:**
- Create: `internal/error/retry.go`, `retry_test.go`

- [ ] **Step 1: Write failing test** — `Backoff(1)=100ms`, `Backoff(2)=200ms`, `Backoff(3)=400ms` (initial=100, multiplier=2.0). Cap test: `Backoff(5)=500ms` when max=500. Zero attempt: `Backoff(0)=100ms`.

- [ ] **Step 2: Run test → FAIL**

- [ ] **Step 3: Write implementation** — `ExponentialBackoff{InitialMs, MaxMs, Multiplier}`, `Backoff(attempt) = min(InitialMs * Multiplier^(attempt-1), MaxMs)`.

- [ ] **Step 4: Run test → PASS**

- [ ] **Step 5: Commit** — `feat: add exponential backoff retry logic`

---

### Task 6: Circuit Breaker

**Files:**
- Create: `internal/error/circuit_breaker.go`, `circuit_breaker_test.go`

- [ ] **Step 1: Write failing test** — 5 tests: (1) starts closed + Allow()=true, (2) opens at 81% failure rate (81 fail + 19 success over 100 window), (3) stays closed at 79%, (4) closes after reset timeout (50ms) + probe success, (5) sliding window evicts old failures (100 fail then 100 success → closed).

- [ ] **Step 2: Run test → FAIL**

- [ ] **Step 3: Write implementation** — `CircuitBreaker` with `sync.Mutex`, sliding window `[]bool` (true=success), `failureThreshold int` (percentage), `windowSize int`, `resetTimeout time.Duration`, `isOpen bool`, `openedAt time.Time`. `Allow()` returns true if closed, or if open+resetTimeout elapsed (half-open probe). `RecordSuccess/RecordFailure` append to window, evict if > windowSize, check threshold.

- [ ] **Step 4: Run test → PASS**

- [ ] **Step 5: Commit** — `feat: add circuit breaker with sliding window`

---

### Task 7: Error Handler — Retry/DLQ/Ignore Routing

**Files:**
- Create: `internal/error/error_handler.go`, `error_handler_test.go`

- [ ] **Step 1: Write failing test** — 5 tests: (1) 503 in retry range → ActionRetry, (2) 404 in DLQ range (not retry) → ActionDLQ, (3) 503 after retry exhausted (Retried=true) → ActionDLQ, (4) 302 not in any range → ActionIgnore, (5) 503 in fail range → ActionFail.

- [ ] **Step 2: Run test → FAIL**

- [ ] **Step 3: Write implementation** — `Action` enum (Retry, DLQ, Ignore, Fail). `ErrorHandler{retryRanges, dlqRanges, failRanges}`. `Route(msg)` — check fail first (highest priority), then retry (if !msg.Retried), then DLQ, default Ignore.

- [ ] **Step 4: Run test → PASS**

- [ ] **Step 5: Commit** — `feat: add error handler with config-driven retry/DLQ/ignore routing`

---

## Phase 4: HTTP Sink

### Task 8: HTTP Sink with Connection TTL and Batch/Individual Modes

**Files:**
- Create: `internal/sink/sink.go`, `http_sink.go`, `http_sink_test.go`

- [ ] **Step 1: Write failing test** — 6 tests using `httptest.NewServer`: (1) batch mode → body contains all msgs as JSON array, (2) individual mode (template set) → N separate requests, (3) 503 → returns failed msg with StatusCode=503, (4) 404 → returns failed msg with StatusCode=404, (5) connection TTL → two pushes 150ms apart with TTL=100ms (no error), (6) headers → verify Authorization header received.

- [ ] **Step 2: Run test → FAIL**

- [ ] **Step 3: Write sink.go** — `Sink` interface (`Push(msgs) ([]FailedMessage, error)`, `Close() error`), `Message` struct, `HTTPSinkConfig` struct.

- [ ] **Step 4: Write http_sink.go** — `HTTPSink` with `net/http.Client`, `connectionTracker` for TTL. `Push()` dispatches to `pushBatch` (no template → buildBatchBody joins msgs as JSON array, single POST) or `pushIndividual` (template set → one POST per msg). `connectionTracker` wraps `Transport.DialContext` to track conn age, `isStale(conn)` returns true if age > TTL. Transport config: `MaxIdleConns`, `MaxIdleConnsPerHost`, `IdleConnTimeout`, `ResponseHeaderTimeout`. `parseHeaders("Authorization:token,Content-Type:application/json")` → map.

- [ ] **Step 5: Run test → PASS**

- [ ] **Step 6: Commit** — `feat: add HTTP sink with batch/individual modes and connection TTL`

---

## Phase 5: Filter

### Task 9: JSON-Path Filter

**Files:**
- Create: `internal/filter/filter.go`, `filter_test.go`

- [ ] **Step 1: Write failing test** — 4 tests: (1) NoOpFilter passes all, (2) JSONPathFilter "$..status" match "created" → 2 pass, 2 drop (including invalid JSON), (3) invalid JSON → dropped, (4) complex path "$.order.status" match "pending".

- [ ] **Step 2: Run test → FAIL**

- [ ] **Step 3: Write implementation** — `Filter` interface (`Apply(msgs) (passed, dropped)`). `NoOpFilter` returns all. `JSONPathFilter` uses `github.com/oliveagle/jsonpath` — compile expression, unmarshal msg.Value, lookup path, compare to matchValue. Invalid JSON or path not found → drop.

- [ ] **Step 4: Run test → PASS**

- [ ] **Step 5: Commit** — `feat: add JSON-path filter for message filtering`

---

## Phase 6: Metrics & Tracing

### Task 10: Prometheus Metrics

**Files:**
- Create: `internal/metrics/metrics.go`, `metrics_test.go`

- [ ] **Step 1: Write failing test** — verify counters increment, histogram observes, gauges set. Use `prometheus/client_golang/prometheus/testutil`.

- [ ] **Step 2: Run test → FAIL**

- [ ] **Step 3: Write implementation** — `Metrics` struct with 14 metrics from spec: `MessagesConsumed`, `MessagesFiltered`, `MessagesDelivered`, `MessagesRetried`, `MessagesDLQ`, `MessagesIgnored` (CounterVec), `SinkLatency` (HistogramVec), `ConsumerLag` (GaugeVec), `WorkerPoolQueueDepth`, `WorkerPoolSize` (Gauge), `CircuitBreakerOpen` (GaugeVec), `HTTPResponseCodes`, `OffsetCommits`, `SchemaUpdates` (CounterVec). Custom `Registry` (not default). `Handler()` returns `promhttp.HandlerFor(registry)`.

- [ ] **Step 4: Run test → PASS**

- [ ] **Step 5: Commit** — `feat: add Prometheus metrics package`

---

### Task 11: OpenTelemetry Tracing

**Files:**
- Create: `internal/tracing/tracing.go`, `tracing_test.go`

- [ ] **Step 1: Write failing test** — (1) disabled → no-op tracer doesn't panic, (2) enabled with no endpoint → doesn't panic.

- [ ] **Step 2: Run test → FAIL**

- [ ] **Step 3: Write implementation** — `Config{Enabled, OTLPEndpoint, ServiceName}`, `TracerProvider` wrapping `sdktrace.TracerProvider`. Disabled → no-op. Enabled → resource with `ServiceName`, OTLP gRPC exporter if endpoint set, batch span processor. `Tracer(name)`, `Shutdown(ctx)`.

- [ ] **Step 4: Run test → PASS**

- [ ] **Step 5: Commit** — `feat: add OpenTelemetry tracing package`

---

## Phase 7: Schema Manager

### Task 12: Schema Manager (JSON + Protobuf)

**Files:**
- Create: `internal/schema/schema.go`, `schema_test.go`

- [ ] **Step 1: Write failing test** — (1) JSON passthrough returns same bytes, (2) protobuf with registry disabled → passthrough, (3) factory creates correct type (json→JSONSchemaManager, protobuf→ProtobufSchemaManager), (4) RegistryClient.Fetch from httptest server returns descriptor data.

- [ ] **Step 2: Run test → FAIL**

- [ ] **Step 3: Write implementation** — `SchemaManager` interface (`Parse(rawBytes) ([]byte, error)`). `JSONSchemaManager` passthrough. `ProtobufSchemaManager` with `RegistryClient` for descriptor fetching (full DynamicMessage parsing deferred — v1 passes through when no parser; real proto parsing is a follow-up task). `NewSchemaManager(cfg)` factory.

- [ ] **Step 4: Run test → PASS**

- [ ] **Step 5: Commit** — `feat: add schema manager with JSON passthrough and protobuf support`

---

## Phase 8: Worker, Consumer, Main Wiring

### Task 13: Batch Type

**Files:**
- Create: `internal/worker/types.go`

- [ ] **Step 1: Write `Batch` struct** — `type Batch struct { ID string; Messages []sink.Message }`

- [ ] **Step 2: Commit** — `feat: add batch type for worker dispatch`

---

### Task 14: Worker Goroutine

**Files:**
- Create: `internal/worker/worker.go`, `worker_test.go`

- [ ] **Step 1: Write failing test** — (1) successful batch (200 OK), verify done signal sent, (2) 5xx retry test (503 twice then 200), verify ≥3 HTTP calls and done signal. Use httptest server + `sync.Mutex` for call count.

- [ ] **Step 2: Run test → FAIL**

- [ ] **Step 3: Write implementation** — `Worker` struct with `id`, `sink`, `errorHandler`, `backoff`, `circuitBreaker`, `doneChan`, `maxRetries`, `dlqWriter`. `Run(ctx, batchChan, wg)` — select on ctx.Done or batchChan. `processBatch` — check circuit breaker, push to sink, record CB result, handle failures, signal done. `handleFailures` — route each failed msg (retry/DLQ/ignore/fail). `retryMessage` — backoff sleep, retry single msg, on exhaustion check DLQ. `sendToDLQ` — write via DLQWriter.

- [ ] **Step 4: Run test → PASS**

- [ ] **Step 5: Commit** — `feat: add worker goroutine with retry, DLQ, and circuit breaker`

---

### Task 15: Kafka Consumer Goroutine

**Files:**
- Create: `internal/consumer/consumer.go`, `consumer_test.go`

- [ ] **Step 1: Write failing test** — (1) consumer creation with config (no Kafka connection needed — just struct creation), (2) NoOp filter integration (passes all msgs).

- [ ] **Step 2: Run test → FAIL**

- [ ] **Step 3: Write implementation** — `Consumer` struct with `kafka.Reader`, `batchChan`, `doneChan`, `offsetMgr`, `schemaMgr`, `filter`, `metrics`. `New()` creates reader (segmentio/kafka-go), schema manager, filter. `Run(ctx)` — loop: `reader.ReadMessage(ctx)`, schema parse, filter, create `Batch{ID: uuid.NewString()}`, `offsetMgr.AddBatch`, metrics, `batchChan <- batch` (blocks = backpressure). Handle dropped (filtered) msgs via `AddOffsetsAndSetCommittable`.

- [ ] **Step 4: Run test → PASS**

- [ ] **Step 5: Commit** — `feat: add Kafka consumer goroutine with schema, filter, and dispatch`

---

### Task 16: Main Entry Point and Wiring

**Files:**
- Create: `cmd/goose/main.go`

- [ ] **Step 1: Write main.go** — Load config, init tracing + metrics (start `/metrics` HTTP server), create offset manager + channels, create HTTP sink, create error handler + circuit breaker, start N worker goroutines, create + start consumer goroutine, start offset commit loop (ticker-based), wait for SIGINT/SIGTERM, graceful shutdown (cancel ctx, close channels, wait workers).

- [ ] **Step 2: Build** — `go build -o bin/goose ./cmd/goose` (fix any import/reference issues).

- [ ] **Step 3: Commit** — `feat: add main entry point — wires all components together`

---

## Phase 9: DLQ Writer

### Task 17: Kafka DLQ Writer

**Files:**
- Create: `internal/error/dlq_kafka.go`, `dlq_kafka_test.go`

- [ ] **Step 1: Write failing test** — verify `NewKafkaDLQWriter` creates non-nil writer.

- [ ] **Step 2: Run test → FAIL**

- [ ] **Step 3: Write implementation** — `KafkaDLQWriter` wrapping `kafka.Writer`. `Write(msgs)` — convert `FailedMessage` to `kafka.Message` with headers (original_topic, original_partition, original_offset, error_type, error_status_code, error_message). Write with 10s timeout. `Close()`.

- [ ] **Step 4: Run test → PASS**

- [ ] **Step 5: Commit** — `feat: add Kafka DLQ writer for failed messages`

---

## Phase 10: Docker and Helm

### Task 18: Multi-Stage Dockerfile

**Files:**
- Create: `Dockerfile`

- [ ] **Step 1: Write Dockerfile** — Stage 1: `golang:1.22-alpine`, copy go.mod, `go mod download`, copy source, `CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /goose ./cmd/goose`. Stage 2: `gcr.io/distroless/static-debian12:nonroot`, copy binary, ENTRYPOINT.

- [ ] **Step 2: Build image** — `docker build -t goose:latest .`

- [ ] **Step 3: Verify size** — `docker images goose:latest` (~15-20MB expected)

- [ ] **Step 4: Commit** — `feat: add multi-stage Dockerfile (~15MB distroless image)`

---

### Task 19: Helm Chart

**Files:**
- Create: `helm/Chart.yaml`, `helm/values.yaml`, `helm/templates/{deployment,service,configmap}.yaml`

- [ ] **Step 1: Write Chart.yaml** — apiVersion v2, name goose, version 1.0.0, appVersion 1.0.0

- [ ] **Step 2: Write values.yaml** — image repo/tag, replicaCount, env (all config vars from spec), metrics.serviceMonitor, resources (requests: 100m/30Mi, limits: 500m/100Mi)

- [ ] **Step 3: Write deployment.yaml** — Deployment with envFrom ConfigMap, metrics port 9090, resource limits

- [ ] **Step 4: Write service.yaml** — Service exposing metrics port for Prometheus scraping

- [ ] **Step 5: Write configmap.yaml** — ConfigMap from .Values.env

- [ ] **Step 6: Commit** — `feat: add Helm chart for Kubernetes deployment`

---

## Phase 11: Integration Test Infrastructure

### Task 20: Integration Test Setup

**Files:**
- Create: `/Users/arelligoutham/Documents/goose-integration-test/docker-compose.yml`
- Create: `prometheus.yml`, `go.mod`, `README.md`

- [ ] **Step 1: Init integration test dir**

```bash
mkdir -p /Users/arelligoutham/Documents/goose-integration-test
cd /Users/arelligoutham/Documents/goose-integration-test
go mod init github.com/arelligoutham/goose-integration-test
git init
```

- [ ] **Step 2: Write docker-compose.yml** — Kafka (bitnami/kafka:3.7 KRaft mode, port 9092), MockServer (mockserver/mockserver:5.15.0, port 1080), Prometheus (prom/prometheus:v2.51.0, port 9091), Jaeger (jaegertracing/all-in-one:1.55, ports 16686+4317).

- [ ] **Step 3: Write prometheus.yml** — scrape config targeting `host.docker.internal:9090` with 15s interval

- [ ] **Step 4: Write README.md** — document services, usage (`docker compose up -d`, `go test -v ./...`), test scenarios

- [ ] **Step 5: Commit** — `chore: add integration test infrastructure with Docker Compose`

---

### Task 21: End-to-End Integration Test

**Files:**
- Create: `goose-integration-test/e2e_test.go`

- [ ] **Step 1: Write E2E test** — Test that: (1) produces JSON messages to Kafka topic, (2) starts goose binary as subprocess with env vars pointing to compose services, (3) verifies mock HTTP server received the messages, (4) verifies Prometheus has `firehose_messages_consumed_total` metric. Use `kafka-go` writer for producing, HTTP client to query MockServer expectations, HTTP client to query Prometheus API.

- [ ] **Step 2: Run with docker compose up** — `docker compose up -d && go test -v -timeout 120s ./...`

- [ ] **Step 3: Commit** — `test: add end-to-end integration test`

---

### Task 22: Retry and DLQ Integration Tests

**Files:**
- Create: `goose-integration-test/retry_dlq_test.go`

- [ ] **Step 1: Write retry test** — Configure MockServer to return 503 twice then 200. Produce message, verify goose retries and eventually delivers. Verify Prometheus `firehose_messages_retried_total` incremented.

- [ ] **Step 2: Write DLQ test** — Configure MockServer to return 404. Produce message, verify goose sends to DLQ Kafka topic. Consume DLQ topic and verify message + headers.

- [ ] **Step 3: Run tests** — `docker compose up -d && go test -v -timeout 120s -run TestRetry ./...`

- [ ] **Step 4: Commit** — `test: add retry and DLQ integration tests`

---

### Task 23: Circuit Breaker Integration Test

**Files:**
- Create: `goose-integration-test/circuit_breaker_test.go`

- [ ] **Step 1: Write test** — Configure MockServer to always return 503. Produce 100+ messages. Verify circuit breaker opens (Prometheus `firehose_circuit_breaker_open=1`). Then configure MockServer to return 200. Verify circuit closes and messages resume.

- [ ] **Step 2: Run test** — `docker compose up -d && go test -v -timeout 120s -run TestCircuitBreaker ./...`

- [ ] **Step 3: Commit** — `test: add circuit breaker integration test`

---

## Phase 12: Final Polish

### Task 24: Add go.sum and verify all tests

- [ ] **Step 1: Run all goose unit tests** — `cd goose && go test -v -race ./...`

- [ ] **Step 2: Tidy modules** — `go mod tidy`

- [ ] **Step 3: Build binary** — `go build -o bin/goose ./cmd/goose`

- [ ] **Step 4: Verify binary size** — `ls -lh bin/goose` (~15MB expected)

- [ ] **Step 5: Commit** — `chore: tidy modules and verify all tests pass`

---

### Task 25: Update design spec with implementation notes

**Files:**
- Modify: `firehose/docs/superpowers/specs/2026-09-22-go-firehose-design.md`

- [ ] **Step 1: Add "Implementation Status" section** — Note which tasks are complete, any deviations from spec, known limitations (e.g., protobuf DynamicMessage parsing deferred).

- [ ] **Step 2: Commit** — `docs: update design spec with implementation notes`

---

## Self-Review

### Spec coverage check
- ✅ Config-driven (env vars) — Task 2
- ✅ Worker pool model (N goroutines) — Tasks 14, 16
- ✅ Channel backpressure — Task 15 (consumer blocks on batchChan)
- ✅ Offset manager (contiguity-gated) — Task 3
- ✅ HTTP sink (batch + individual) — Task 8
- ✅ Connection TTL + idle eviction + stale validation — Task 8
- ✅ Error routing (retry/DLQ/ignore/fail) — Tasks 4, 7
- ✅ Exponential backoff — Task 5
- ✅ Circuit breaker — Task 6
- ✅ DLQ (Kafka) — Task 17
- ✅ JSON-path filter — Task 9
- ✅ Schema manager (JSON + protobuf) — Task 12
- ✅ Prometheus metrics — Task 10
- ✅ OpenTelemetry tracing — Task 11
- ✅ Main wiring + graceful shutdown — Task 16
- ✅ Dockerfile (distroless, ~15MB) — Task 18
- ✅ Helm chart — Task 19
- ✅ Integration tests — Tasks 20-23

### Placeholder scan
No TBD/TODO/FIXME in the plan. All tasks have concrete file paths and test descriptions.

### Type consistency
- `FailedMessage` used consistently across error handler, worker, DLQ
- `Batch` struct consistent between worker types and consumer
- `Message` types: `sink.Message`, `filter.Message`, `offsetmanager.Message` — distinct types for distinct concerns, converted via helper functions in consumer
- `StatusRangeList` from config package used by error handler

### Known deviations from spec
- Protobuf DynamicMessage parsing deferred to a follow-up task (v1 passes through raw bytes when no parser). This is noted in Task 12.
- Consumer uses `segmentio/kafka-go` ReadMessage (one at a time) rather than batch poll. This is simpler and the channel buffer provides batching. Can be optimized later.
