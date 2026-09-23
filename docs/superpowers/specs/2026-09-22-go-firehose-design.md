# Go Firehose — Lightweight Kafka-to-HTTP Firehose

**Date:** 2026-09-22
**Status:** Design — Approved
**Author:** @Arelli-Goutham_pinegit

## Problem

The company uses a fork of raystack firehose (`plural-firehose-1.3.0`, jar `firehose-0.10.6`, package `com.gotocompany.firehose`) to consume Kafka messages and deliver them to HTTP endpoints. It works, but it is heavy and has known issues:

- **164 MB fat jar** with 50+ transitive dependencies, JVM overhead (~200-300 MB RAM at idle)
- **HTTP connection pinning** — Apache HttpClient has no connection TTL, no idle eviction, no stale validation. Each Sink thread's TCP connection stays pinned to one backend pod for the entire pod lifetime. Uneven load distribution across Kubernetes pods. Stale connection failures after pod restarts (verified by decompiling Plural's jar — same bug as upstream raystack)
- **Spin-loop backpressure** — when all Sink workers are busy, the consumer thread spins in a `while(true)` loop, burning CPU
- **5-10 second JVM startup** — slow K8s rolling deploys
- **Old observability stack** — Telegraf/StatsD → InfluxDB → Grafana. Company has Last9/Prometheus

We need a modern, lightweight replacement with full control over configuration, connection management, error handling, and observability.

## Goals

- **Light:** ~15 MB binary, ~20-30 MB RAM at idle, <1s startup
- **Fast:** goroutine-based concurrency, no spin loops, natural channel backpressure
- **Scalable:** config-driven worker pool (N workers, can exceed partition count), horizontal scaling via K8s
- **Config-driven:** all config via env vars (same deploy story as raystack), drop-in Helm chart
- **Observable:** native Prometheus metrics (scraped by Last9) + OpenTelemetry traces (Jaeger/Tempo)
- **Correct at-least-once delivery:** contiguity-gated offset commit (same proven logic as raystack)
- **No connection pinning:** connection TTL + idle eviction + stale validation built in from day one

## Non-Goals (v1)

- Multiple sink types (Redis, ES, JDBC) — pluggable Sink interface, add in later versions
- JEXL filters — JSON-path filter is simpler and sufficient
- Blob storage DLQ — Kafka DLQ only for v1
- Per-partition consumers — worker pool is simpler for v1
- Telegraf/StatsD support — Prometheus only

## Language: Go

Chosen for:
- Single binary (~15 MB), lowest memory (~20-30 MB at idle)
- Goroutines — millions of lightweight concurrent units, trivial N-worker model
- Built-in `net/http` client with connection pooling, keep-alive, timeouts
- Fast startup (<1s) — better for K8s rolling deploys
- `envconfig` library — same env-var config model as raystack, zero ops learning curve
- No JVM, no jar classpath issues, no Lombok/Gradle/protoc build tooling complexity
- Go is the most common language for data infrastructure (Kafka tools, K8s operators, observability agents)

## Architecture — Worker Pool Model

Matches raystack's async consumer mental model: 1 consumer goroutine polls Kafka, N worker goroutines process batches. N is config-driven and can exceed partition count.

```
Kafka Topic
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│  Consumer Goroutine (1)                                     │
│  • segmentio/kafka-go reader                                │
│  • poll() up to MAX_POLL_RECORDS messages                   │
│  • schema deserialize (protobuf→DynamicMessage or JSON pass) │
│  • apply JSON-path filter                                    │
│  • create Batch with UUID                                   │
│  • register offsets in OffsetManager (not committable yet)  │
│  • batchChan <- batch  (BLOCKS if full = backpressure)      │
└─────────────────────┬───────────────────────────────────────┘
                      │  chan *Batch (buffered, size=WORKER_POOL_BUFFER)
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  Worker Pool (N goroutines, SINK_WORKER_POOL_SIZE)          │
│  • worker reads <- batchChan                                │
│  • prepare HTTP request(s) — batch or individual mode       │
│  • check circuit breaker before executing                   │
│  • execute via net/http client (TTL + eviction + validate)  │
│  • on failure: retry → DLQ (config-driven error routing)    │
│  • signal batch done to OffsetManager                       │
└─────────────────────┬───────────────────────────────────────┘
                      │  done signal
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  Offset Manager                                             │
│  • per-partition sorted offset tracking (mutex-protected)   │
│  • SetCommittable(batchID) marks offsets committable        │
│  • GetCommittable() returns contiguous committable prefix   │
│  • Commit goroutine: commit to Kafka every COMMIT_INTERVAL  │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Observability (parallel)                                   │
│  • Prometheus /metrics endpoint (scraped by Last9)          │
│  • OpenTelemetry traces (exported to Jaeger/Tempo)          │
│  • Circuit breaker state exposed as metric                  │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Schema Manager (parallel)                                  │
│  • Fetches proto descriptors from schema registry URL       │
│  • Long-polls for schema updates → hot-swap parser          │
│  • Parses raw Kafka bytes → DynamicMessage → JSON for sink  │
└─────────────────────────────────────────────────────────────┘
```

### Key improvement over raystack: channel backpressure

Raystack's async consumer spins in a `while(true)` loop when all Sink workers are busy:
```java
while (true) {
    Future f = sinkPool.submitTask(messages);  // returns null if all sinks busy
    if (f == null) {
        sinkPool.fetchFinishedSinkTasks();  // spin, burn CPU
    } else {
        return f;
    }
}
```

The Go firehose uses a buffered channel instead:
```go
batchChan <- batch  // blocks — zero CPU, OS-level sleep, wakes when worker reads
```

When all N workers are busy and the channel buffer is full, the consumer goroutine goes to sleep at the OS level. No spin, no CPU waste, no risk of `max.poll.interval.ms` eviction from CPU spinning.

## Components

### File structure (~16 Go files, ~8 dependencies)

```
new-firehose/
├── main.go                ← entry point, config load, wiring, graceful shutdown
├── config/
│   └── config.go          ← envconfig struct + defaults + validation at startup
├── consumer/
│   └── consumer.go        ← Kafka consumer goroutine (poll → schema → filter → dispatch)
├── worker/
│   └── worker.go          ← N worker goroutines (receive batch → HTTP → retry/DLQ/error route)
├── offset/
│   └── offset_manager.go  ← batch tracking + contiguity-gated offset commit
├── filter/
│   └── filter.go          ← JSON-path filter (drop messages that don't match)
├── sink/
│   ├── sink.go            ← Sink interface (pluggable for future sinks)
│   └── http_sink.go       ← HTTP sink impl (batch/individual mode, connection TTL)
├── error/
│   ├── error_handler.go   ← error routing (retry/DLQ/ignore decision tree)
│   ├── retry.go           ← exponential backoff retry logic
│   ├── dlq.go             ← DLQ writer (Kafka topic)
│   └── circuit_breaker.go ← circuit breaker (sliding window, open/close)
├── metrics/
│   └── metrics.go         ← Prometheus metrics registration + /metrics endpoint
├── tracing/
│   └── tracing.go         ← OpenTelemetry tracer setup + span creation
├── schema/
│   ├── schema.go          ← SchemaManager interface + DynamicMessage parser
│   └── registry_client.go ← HTTP client for descriptor fetching (long-polling)
├── go.mod
├── Dockerfile             ← multi-stage build (golang:1.22-alpine → distroless)
└── helm/
    ├── Chart.yaml
    ├── values.yaml
    └── templates/
        ├── deployment.yaml
        ├── service.yaml      ← Prometheus metrics service
        └── configmap.yaml
```

### Dependencies (8 total)

| Dependency | Purpose |
|---|---|
| `github.com/segmentio/kafka-go` | Kafka consumer (pure Go, no librdkafka) |
| `github.com/kelseyhightower/envconfig` | Env var config loading |
| `github.com/prometheus/client_golang` | Prometheus metrics |
| `go.opentelemetry.io/otel` + exporters | OpenTelemetry traces |
| `github.com/oliveagle/jsonpath` | JSON-path filter evaluation |
| `google.golang.org/protobuf` | Protobuf parsing (DynamicMessage) |
| `github.com/google/uuid` | Batch UUID generation |
| `golang.org/x/sync/errgroup` | Graceful shutdown coordination |

## Data Flow — Step by Step

Setup example: `MAX_POLL_RECORDS=100`, `WORKER_POOL_SIZE=5`, `WORKER_POOL_BUFFER=10`, `INPUT_SCHEMA_DATA_TYPE=protobuf`, HTTP sink at `http://order-service:8080/events`.

### Step 1: Consumer polls Kafka
Consumer goroutine calls `reader.ReadMessages(ctx)` (blocks until Kafka has data), returns up to 100 raw byte messages mixed across partitions.

### Step 2: Schema deserialization
If `INPUT_SCHEMA_DATA_TYPE=protobuf`: schema manager parses each raw byte message via `DynamicMessage` parser (fetched from schema registry URL), then serializes to JSON for the HTTP sink. If `json`: bytes are already JSON, pass through unchanged.

### Step 3: Apply filter
If `SINK_HTTP_FILTER_JSONPATH` is configured, evaluate each message against the JSON-path expression. Messages that don't match are dropped (filtered). Filtered messages have their offsets marked committable immediately (same as raystack's `forceAddOffsetsAndSetCommittable`).

### Step 4: Create batch + register offsets
Consumer creates a `Batch` struct with a UUID and the filtered messages. Calls `offsetMgr.AddBatch(batchID, messages)` — registers all offsets in the OffsetManager under the batch ID, all marked not-committable.

### Step 5: Dispatch to worker pool
Consumer sends `batchChan <- batch`. This blocks if the channel buffer is full (natural backpressure, no spin loop). A free worker goroutine receives `batch := <-batchChan`.

### Step 6: Worker prepares HTTP request
- **Batch mode** (no `SINK_HTTP_JSON_BODY_TEMPLATE`): serialize all messages into one JSON array body, create one HTTP POST request.
- **Individual mode** (template or dynamic URL configured): create one HTTP request per message, each with its own URL/body from the template.

### Step 7: Circuit breaker check
Before executing HTTP call, worker checks `circuitBreaker.Allow()`. If open (failure rate > 80% over last 100 requests), the worker waits and periodically probes the sink. If closed, proceeds.

### Step 8: Worker executes HTTP call
Worker calls `httpClient.Do(req)`. The HTTP client has connection pooling with TTL (connections closed after 30s), idle eviction (idle connections closed after 30s), and stale validation (connections validated after 2s inactivity). This prevents the connection pinning bug.

### Step 9: Error routing (if any failures)
For each failed message, the error handler routes based on config:
1. **In RETRY range** (e.g., 500-599, 429)? → retry with exponential backoff (3 attempts, 100ms → 200ms → 400ms). If retry exhausts and also in DLQ range → DLQ.
2. **In DLQ range** (e.g., 400-499)? → DLQ immediately (no retry — 4xx won't fix itself).
3. **In FAIL range**? → Only used for infrastructure failures (can't connect to Kafka, can't write to DLQ). HTTP status codes should NEVER crash the consumer — use circuit breaker instead.
4. **Not in any range** → Ignore (message dropped, offset committable).

Each HTTP failure also updates the circuit breaker's sliding window.

### Step 10: Signal batch complete
Worker calls `offsetMgr.SetCommittable(batchID)` — flips all offsets under that batch ID to committable.

### Step 11: Offset commit (separate goroutine)
Commit goroutine runs every `COMMIT_INTERVAL_MS`, calls `offsetMgr.GetCommittable()`, which walks each partition's sorted offsets from the lowest and returns the contiguous committable prefix (same contiguity logic as raystack). Commits these offsets to Kafka.

### Step 12: Metrics + tracing (async)
- Prometheus counters/histograms: messages consumed, filtered, delivered, retried, DLQ'd, sink latency p50/p95/p99, consumer lag, worker pool queue depth, circuit breaker state, HTTP status codes.
- OpenTelemetry spans: batch processing span with attributes (batch ID, size, sink type, HTTP URL, status, retry count, duration).

## Error Handling

### Config-driven error routing

```properties
# Retry: transient errors (server errors, rate limit, request timeout)
SINK_HTTP_ERROR_RETRY_STATUS_CODES=500-599,429,408

# DLQ: permanent errors + retry-exhausted transient errors
SINK_HTTP_ERROR_DLQ_STATUS_CODES=400-428,430-499,500-599

# Fail (crash): NEVER for HTTP status codes — only infrastructure failures
SINK_HTTP_ERROR_FAIL_STATUS_CODES=

# Retry config
SINK_HTTP_RETRY_MAX_ATTEMPTS=3
SINK_HTTP_RETRY_BACKOFF_INITIAL_MS=100
SINK_HTTP_RETRY_BACKOFF_MAX_MS=10000
SINK_HTTP_RETRY_BACKOFF_MULTIPLIER=2.0

# DLQ config
SINK_HTTP_DLQ_ENABLED=true
SINK_HTTP_DLQ_TYPE=kafka
SINK_HTTP_DLQ_KAFKA_TOPIC=firehose-dlq
SINK_HTTP_DLQ_KAFKA_BROKERS=kafka:9092
```

### Decision tree

```
HTTP response received
    │
    ├── 2xx (200-299) ──────────→ SUCCESS (offset committable)
    │
    ├── In RETRY range? ────────→ retry with exponential backoff (up to MAX_ATTEMPTS)
    │       │
    │       └── retry exhausts?
    │           ├── In DLQ range? ──→ DLQ (message saved, offset committable)
    │           └── Not in DLQ? ────→ IGNORE (message dropped, offset committable)
    │
    ├── In DLQ range (not retry)? ──→ DLQ immediately
    │
    ├── In FAIL range? ──→ crash (ONLY for infrastructure, not HTTP codes)
    │
    └── None of the above? ──→ IGNORE (message dropped, offset committable)
```

### When to crash vs not crash

| Scenario | Crash? | Action |
|---|---|---|
| HTTP sink returns 503 | No | Retry → DLQ → circuit breaker |
| HTTP sink returns 502 | No | Same |
| Can't connect to Kafka | Yes | K8s restart → might get healthy broker |
| Can't write to DLQ Kafka | Yes | Data loss risk → restart → retry |
| Can't fetch proto descriptors | Yes | Can't deserialize → systemic config error |
| Invalid configuration at startup | Yes | Fail fast, don't start broken |
| Failure rate > 80% sustained | No | Circuit breaker (pause + alert) |

Principle: **crash only when firehose itself is broken, not when the downstream is broken.**

### Circuit breaker

Sliding window of last 100 HTTP requests. If failure rate > 80%:
- Open circuit: stop dispatching to workers, keep polling Kafka (avoid eviction), buffer in channel, emit `firehose_circuit_breaker_open=1` metric.
- Every 10s: send one probe request to sink.
- If probe succeeds: close circuit, resume processing, emit `firehose_circuit_breaker_open=0`.

## Schema Handling

### Config

```properties
INPUT_SCHEMA_DATA_TYPE=protobuf          # protobuf | json
SCHEMA_REGISTRY_ENABLED=true
SCHEMA_REGISTRY_URL=http://stencil:8080/descriptors/events.OrderEvent/latest
SCHEMA_REGISTRY_PROTO_CLASS=events.OrderEvent
SCHEMA_REGISTRY_REFRESH_STRATEGY=long_polling   # long_polling | periodic | none
SCHEMA_REGISTRY_REFRESH_INTERVAL_MS=300000       # for periodic (5 min)
SCHEMA_REGISTRY_FETCH_TIMEOUT_MS=10000
SCHEMA_REGISTRY_AUTH_BEARER_TOKEN=optional
```

### Protobuf mode
1. On startup: fetch proto descriptors (FileDescriptorSet) from `SCHEMA_REGISTRY_URL`.
2. Parse descriptor, find `SCHEMA_REGISTRY_PROTO_CLASS`, build `DynamicMessage` parser.
3. On each Kafka message: `parser.Parse(rawBytes) → DynamicMessage → protojson.Marshal → JSON string` for HTTP sink.
4. Background goroutine long-polls the URL for schema updates. On new descriptor: atomic swap of parser (zero downtime, no restart).

### JSON mode
No schema fetch. Raw bytes are already JSON. Pass through to filter and HTTP sink.

### Embedded fallback (optional, not v1 priority)
If no schema registry: compile proto descriptors into binary at build time. Requires rebuild when proto changes. Config: `SCHEMA_REGISTRY_ENABLED=false`, `SCHEMA_REGISTRY_EMBEDDED_DESCRIPTOR=events.OrderEvent`.

## Batch Creation

### Why UUID batch keys
Raystack uses `Future` objects as batch keys for the OffsetManager. Go doesn't have Futures, so we use UUID strings as batch keys — same purpose: unique identifier tying messages to their offset tracking entry.

### Example
```
poll() → 10 messages across 3 partitions
  msg[0]: partition=0, offset=50
  msg[1]: partition=0, offset=51
  msg[2]: partition=1, offset=100
  ...

Consumer:
  batch = &Batch{ID: "f47ac10b-...", Messages: [10 msgs]}
  offsetMgr.AddBatch("f47ac10b-...", msgs)
  → registers 10 offsets (all not-committable)
  batchChan <- batch

Worker:
  batch := <-batchChan
  → process (HTTP calls, retry, DLQ)
  offsetMgr.SetCommittable("f47ac10b-...")
  → flips all 10 offsets to committable

Commit goroutine:
  offsetMgr.GetCommittable()
  → partition 0: [50✅, 51✅] → commit offset 52
  → partition 1: [100✅] → commit offset 101
  → partition 2: [200✅, 201✅, ...] → commit offset N
  → Kafka commits offsets
```

The UUID is purely in-memory — never serialized. If the process crashes, in-flight batches are lost and Kafka re-reads from the last committed offset (at-least-once).

## HTTP Connection Management

Built-in from day one (fixing the raystack bug we identified):

| Setting | Config | Default | How (Go net/http) |
|---|---|---|---|
| Connection TTL | `SINK_HTTP_CONNECTION_TTL_MS` | 30000 | Custom `Transport.DialContext` tracks connection age, closes after TTL |
| Idle connection eviction | `SINK_HTTP_CONNECTION_IDLE_EVICT_MS` | 30000 | `Transport.IdleConnTimeout` |
| Stale validation | `SINK_HTTP_CONNECTION_VALIDATE_INACTIVITY_MS` | 2000 | `Transport.ResponseHeaderTimeout` + connection health check before reuse |
| Max connections | `SINK_HTTP_MAX_CONNECTIONS` | 10 | `Transport.MaxIdleConns` + `MaxIdleConnsPerHost` |
| Request timeout | `SINK_HTTP_REQUEST_TIMEOUT_MS` | 10000 | `http.Client.Timeout` |
| Retry status codes | `SINK_HTTP_RETRY_STATUS_CODE_RANGES` | 400-600 | Error handler (not transport-level) |

This ensures connections are periodically closed and re-opened, triggering kube-proxy DNAT re-resolution and redistributing traffic across backend pods. No more pinning to one pod for the firehose's lifetime.

## Observability

### Prometheus metrics (scraped by Last9)

| Metric | Type | Labels | Description |
|---|---|---|---|
| `firehose_messages_consumed_total` | counter | topic, partition | Messages polled from Kafka |
| `firehose_messages_filtered_total` | counter | topic | Messages dropped by filter |
| `firehose_messages_delivered_total` | counter | sink | Messages successfully delivered |
| `firehose_messages_retried_total` | counter | error_type | Messages that entered retry |
| `firehose_messages_dlq_total` | counter | error_type | Messages sent to DLQ |
| `firehose_messages_ignored_total` | counter | error_type | Messages dropped (no DLQ configured) |
| `firehose_sink_latency_seconds` | histogram | sink | HTTP call latency (p50, p95, p99) |
| `firehose_consumer_lag_messages` | gauge | topic, partition | Kafka consumer lag |
| `firehose_worker_pool_queue_depth` | gauge | — | Current batchChan buffer depth |
| `firehose_worker_pool_size` | gauge | — | Configured worker count |
| `firehose_circuit_breaker_open` | gauge | sink | 1=open, 0=closed |
| `firehose_http_response_code_total` | counter | status_code | HTTP response code distribution |
| `firehose_offset_commit_total` | counter | topic, partition | Offset commits to Kafka |
| `firehose_schema_updates_total` | counter | proto_class | Schema registry update count |

### OpenTelemetry traces

Span: `firehose.process_batch`
- Attributes: batch.id, batch.size, sink.type, http.url, http.status_code, retry.count, duration_ms, error.type

Span: `firehose.consume`
- Attributes: topic, partition, offset, message.size

Exported via OTLP to Jaeger/Tempo (configured via `OTEL_EXPORTER_OTLP_ENDPOINT`).

## Graceful Shutdown

```
SIGTERM (K8s pod termination):
  │
  ├── ctx.Cancel() → all goroutines receive Done signal
  ├── Consumer stops polling (ctx.Done in poll loop)
  ├── Workers finish current batch (don't read new from batchChan)
  ├── Offset manager commits final offsets (no data loss)
  ├── HTTP client closes idle connections
  ├── Kafka reader closes
  ├── Prometheus metrics endpoint stops
  └── Process exits within K8s terminationGracePeriodSeconds (default 30s)
```

## Kubernetes Deployment

### Docker image
Multi-stage build:
- Stage 1: `golang:1.22-alpine` — compile binary
- Stage 2: `gcr.io/distroless/static` — final image (~15 MB)

### Helm chart
Same env-var-driven model as raystack. Drop-in replacement:
```yaml
# values.yaml
image:
  repository: registry.example.com/go-firehose
  tag: 1.0.0

env:
  SOURCE_KAFKA_BROKERS: kafka:9092
  SOURCE_KAFKA_TOPIC: events
  SOURCE_KAFKA_CONSUMER_GROUP_ID: go-firehose
  SINK_TYPE: http
  SINK_HTTP_SERVICE_URL: http://order-service:8080/events
  INPUT_SCHEMA_DATA_TYPE: protobuf
  SCHEMA_REGISTRY_URL: http://stencil:8080/descriptors/events.OrderEvent/latest
  SCHEMA_REGISTRY_PROTO_CLASS: events.OrderEvent
  SINK_WORKER_POOL_SIZE: "10"
  SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS: "100"

metrics:
  enabled: true
  serviceMonitor:
    enabled: true          # for Prometheus operator / Last9 scraping
    interval: 15s

tracing:
  enabled: true
  OTEL_EXPORTER_OTLP_ENDPOINT: http://otel-collector:4317

resources:
  requests:
    cpu: 100m
    memory: 30Mi
  limits:
    cpu: 500m
    memory: 100Mi
```

Resource comparison:

| | Raystack (Java) | Go firehose |
|---|---|---|
| Image size | 786 MB | ~15 MB |
| RAM at idle | ~200-300 MB | ~20-30 MB |
| CPU at idle | ~50-100m | ~5-10m |
| Startup | 5-10s | <1s |

## Full Configuration Reference

```properties
# ═══ Kafka Consumer ═══
SOURCE_KAFKA_BROKERS=kafka:9092
SOURCE_KAFKA_TOPIC=events
SOURCE_KAFKA_CONSUMER_GROUP_ID=go-firehose
SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS=100
SOURCE_KAFKA_POLL_TIMEOUT_MS=1000
SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_INTERVAL_MS=300000
SOURCE_KAFKA_CONSUMER_CONFIG_SESSION_TIMEOUT_MS=10000
SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET=latest

# ═══ Schema ═══
INPUT_SCHEMA_DATA_TYPE=protobuf
SCHEMA_REGISTRY_ENABLED=true
SCHEMA_REGISTRY_URL=http://stencil:8080/descriptors/events.OrderEvent/latest
SCHEMA_REGISTRY_PROTO_CLASS=events.OrderEvent
SCHEMA_REGISTRY_REFRESH_STRATEGY=long_polling
SCHEMA_REGISTRY_FETCH_TIMEOUT_MS=10000

# ═══ Filter ═══
SINK_HTTP_FILTER_ENABLED=false
SINK_HTTP_FILTER_JSONPATH=$..status:created

# ═══ Worker Pool ═══
SINK_WORKER_POOL_SIZE=10
SINK_WORKER_POOL_BUFFER=10
SOURCE_KAFKA_COMMIT_INTERVAL_MS=5000

# ═══ HTTP Sink ═══
SINK_TYPE=http
SINK_HTTP_SERVICE_URL=http://order-service:8080/events
SINK_HTTP_REQUEST_METHOD=POST
SINK_HTTP_REQUEST_TIMEOUT_MS=10000
SINK_HTTP_MAX_CONNECTIONS=10
SINK_HTTP_CONNECTION_TTL_MS=30000
SINK_HTTP_CONNECTION_IDLE_EVICT_MS=30000
SINK_HTTP_CONNECTION_VALIDATE_INACTIVITY_MS=2000
SINK_HTTP_HEADERS=Authorization:token,Content-Type:application/json
SINK_HTTP_DATA_FORMAT=json
SINK_HTTP_JSON_BODY_TEMPLATE=

# ═══ Error Handling ═══
SINK_HTTP_ERROR_RETRY_STATUS_CODES=500-599,429,408
SINK_HTTP_ERROR_DLQ_STATUS_CODES=400-428,430-499,500-599
SINK_HTTP_ERROR_FAIL_STATUS_CODES=
SINK_HTTP_RETRY_MAX_ATTEMPTS=3
SINK_HTTP_RETRY_BACKOFF_INITIAL_MS=100
SINK_HTTP_RETRY_BACKOFF_MAX_MS=10000
SINK_HTTP_RETRY_BACKOFF_MULTIPLIER=2.0

# ═══ DLQ ═══
SINK_HTTP_DLQ_ENABLED=true
SINK_HTTP_DLQ_TYPE=kafka
SINK_HTTP_DLQ_KAFKA_TOPIC=firehose-dlq
SINK_HTTP_DLQ_KAFKA_BROKERS=kafka:9092

# ═══ Circuit Breaker ═══
SINK_HTTP_CIRCUIT_BREAKER_ENABLED=true
SINK_HTTP_CIRCUIT_BREAKER_FAILURE_THRESHOLD=80
SINK_HTTP_CIRCUIT_BREAKER_WINDOW_SIZE=100
SINK_HTTP_CIRCUIT_BREAKER_RESET_TIMEOUT_MS=10000

# ═══ Observability ═══
METRICS_PROMETHEUS_ENABLED=true
METRICS_PROMETHEUS_PORT=9090
OTEL_TRACING_ENABLED=true
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
OTEL_SERVICE_NAME=go-firehose

# ═══ OAuth2 (optional) ═══
SINK_HTTP_OAUTH2_ENABLED=false
SINK_HTTP_OAUTH2_ACCESS_TOKEN_URL=
SINK_HTTP_OAUTH2_CLIENT_NAME=
SINK_HTTP_OAUTH2_CLIENT_SECRET=
SINK_HTTP_OAUTH2_SCOPE=
```

## Testing

- **Unit tests:** each component tested independently (offset manager, filter, error handler, circuit breaker, retry logic, schema parser)
- **Integration tests:** Kafka + mock HTTP server (testcontainers-go for Kafka)
- **Offset manager tests:** same test cases as raystack's `OffsetManagerTest.java` — contiguity, multi-batch, multi-partition, out-of-order completion
- **Circuit breaker tests:** sliding window accuracy, open/close transitions, probe request logic
- **Error routing tests:** each error code path (retry → DLQ, DLQ immediate, ignore, crash for infrastructure)

## Future Scope (Post-v1)

- Pluggable sink interface (Redis, Elasticsearch, JDBC, MongoDB)
- Blob storage DLQ (GCS, S3)
- Per-partition consumer model (for even higher throughput)
- JEXL filter support (for full raystack compatibility)
- Schema registry: Confluent Schema Registry support (in addition to Stencil URL-based)
- Embedded proto descriptors (no registry needed)
