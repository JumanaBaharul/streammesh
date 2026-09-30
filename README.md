# StreamMesh

> **A high-throughput ingest and routing engine for operational and security telemetry, written in Go.**

One binary that collects logs, events and security signals from many sources,
normalises them into one canonical schema, applies redaction/sampling rules, and
routes them to many destinations - with real delivery guarantees, backpressure
and observability.

**Repo name:** `streammesh` · **Language:** Go (>= 1.22) · **License:** MIT

---

## 1. Why this project exists

Most teams eventually need the same plumbing: logs, events and security signals
arrive in a dozen formats, and something has to normalise, redact and route them
before they reach storage, search or alerting. Existing routers are either
heavyweight to operate or closed-source, and hand-wiring this into every service
is a standing maintenance tax.

StreamMesh is a deliberate, production-style Go service built around the
fundamentals of that problem space: durable ingest, backpressure, delivery
guarantees and observability, implemented with the concurrency primitives Go is
known for. It is designed to be read as well as run - the code is meant as a
small, honest reference implementation of a distributed data path.

## 2. What it does, end to end

```
        SOURCES                    CORE ENGINE                        SINKS
  ┌──────────────────┐      ┌───────────────────────┐      ┌────────────────────┐
  │ HTTP  /ingest    │      │  parse -> normalise   │      │ Kafka topic        │
  │ Syslog TCP/UDP   │─────▶│  validate (schema)    │─────▶│ HTTP webhook        │
  │ Log file tailer  │      │  rules: drop/sample/  │      │ Rotating file       │
  │ Webhook receiver │      │  redact/enrich/route  │      │ S3-compatible bucket│
  └──────────────────┘      └───────────┬───────────┘      │ In-memory (tests)  │
                                        │                  └────────────────────┘
                              WAL (durability) + dedup + DLQ + metrics
```

## 3. Features and functionality

### 3.1 Ingest (pluggable `Source` interface)
- **HTTP ingest** - `POST /ingest` accepting single JSON, NDJSON or batched JSON,
  with gzip/chunked bodies and a max-body guard.
- **Syslog listener** - TCP and UDP, RFC5424 and RFC3164 framing and parsing.
- **File tailer** - follows log files, survives truncation/rotation (inode-aware),
  resumes from a stored offset.
- **Webhook receiver** - generic per-provider endpoint with signature check.
- New sources are added by implementing one interface; no core changes.

### 3.2 Parse and normalise
- Per-source **parser registry** (JSON, CSV, syslog, key=value, regex-capture).
- Every event lands in one **canonical schema**: `timestamp, source_id, host,
  severity, category, actor, resource, trace_id, attrs{}` - unknown fields are
  preserved, never silently dropped.
- **Schema validation** at the edge: malformed records go to the dead-letter
  queue with the reason, instead of poisoning the pipeline.

### 3.3 Routing rules (declarative YAML)
- **Matchers:** field equality/prefix, regex, numeric comparison, severity >=,
  set membership, `AND`/`OR`/`NOT` composition.
- **Actions:** `drop`, `sample` (rate/percentage), `redact`, `enrich`, `route`
  (fan-out to N sinks), `tag`.
- Rules are hot-reloadable; a bad ruleset is rejected without taking down the
  running pipeline.

### 3.4 Delivery guarantees
- **Write-ahead log** with segment files and batched `fsync`; events are
  acknowledged only after they are durable.
- **At-least-once delivery** with per-sink retry, exponential backoff + jitter
  and a bounded retry budget.
- **Idempotent dedup** on event ID (TTL map + bloom filter) so retries do not
  duplicate downstream.
- **Dead-letter queue** with replay: `POST /replay` re-drives DLQ/WAL ranges for
  backfills after a bad rule or outage.
- **Backpressure instead of OOM**: bounded channels everywhere; when a sink
  stalls, upstream slows, the WAL grows, and the stall is visible in metrics.

### 3.5 DLP and masking
- Field-path and regex **redaction** (emails, card numbers, API keys, bearer
  tokens) applied before anything is written or forwarded.
- **SHA-256 tokenisation** for stable pseudonymous correlation.
- Applied as a pipeline stage, so it is testable in isolation and impossible to
  bypass by adding a new sink.

### 3.6 Observability and control plane
- **Prometheus** `/metrics`: events in/out/dropped/retried per source and sink,
  queue depth, WAL lag, rule match counts, p50/p95/p99 stage latency.
- **Structured JSON logs** via `log/slog` with correlation IDs threaded through
  `context`.
- `/healthz` (liveness) and `/readyz` (readiness - reflects sink and WAL state).
- **REST control plane:** list/create/update pipelines, `GET /stats`, `POST
  /replay`, hot reload on `SIGHUP`.
- **Graceful shutdown:** drain in-flight events on SIGTERM, close WAL cleanly.

### 3.7 Go engineering on display
- Goroutine worker pools per stage with fan-in/fan-out over channels.
- `context` cancellation and deadlines propagated to every connector.
- Interfaces + dependency injection (every connector swappable, all fakes
  in-process - the test suite needs no Docker).
- `sync.Pool` for hot-path buffers, preallocated slices, zero-copy parsing where
  it matters; allocation counts tracked with benchmarks.
- `go test -race`, table-driven tests, fuzz test on the syslog/JSON parsers.
- `golangci-lint` + `go vet` + `staticcheck` clean.

### 3.8 AI-assisted enrichment (optional stage)
- One `enrich` action can call an LLM (Ollama or any OpenAI-compatible endpoint)
  to classify an event into a category or extract a field, with a strict timeout,
  a deterministic rule fallback, and the result cached - so an LLM outage can
  never block the data path.

### 3.9 Packaging and demo
- `Dockerfile` (distroless, multi-stage) + `docker compose up` demo that starts a
  synthetic event generator, StreamMesh, and a sink, then prints live throughput.
- `Makefile` targets: `build`, `test`, `test-race`, `bench`, `lint`, `run`, `demo`.
- GitHub Actions CI: build, vet, staticcheck, `go test -race`, coverage, and a
  benchmark run posting throughput on every PR.
- Ship cross-platform binaries with `goreleaser` tags on release.

### 3.10 Measured performance

Everything below was measured on **2026-09-30** with the repository's own load
generator (`streammesh gen`), Go 1.27.1, on an 8-logical-core Windows
workstation. Nothing here is estimated.

| Configuration | Sustained ingest | Notes |
|---|---|---|
| Defaults (WAL + dedup on) | **~21.5k events/s** | 24 HTTP workers, 1000-event NDJSON batches; queue depth 0, zero saturated sends |
| WAL disabled | ~29.9k events/s | isolates the write-ahead log as the dominant ingest cost (~30%) |
| dedup disabled (WAL on) | ~20.9k events/s | dedup is **not** a bottleneck at this rate |

Correctness under the same load: 29,600 events in, **0 false duplicate drops**,
4,271 dropped by the healthcheck-severity rule (exactly 1/7 of the synthetic
categories, as the rule predicts), 25,329 archived to the sink, emails redacted in
every delivered record.

Honest caveats, because a benchmark without them is marketing:

- The generator and the engine ran on the **same 8 cores**, so these figures are
  a floor for the engine, not its ceiling. Queues stayed empty and nothing was
  dropped to backpressure, so the limit here is the ingest path, not the sinks.
- `go test -race` has **not** been run locally: this workstation has no C
  toolchain and Docker Desktop was not running. Race detection is wired into CI
  and must pass there before this is quoted anywhere.
- Two real bugs were found and fixed by these runs: an unconfigured bloom filter
  sized itself to 64 bits and then flagged 86,362 of 86,400 events as duplicates,
  and `ReplayDLQ` reported "delivered" for merely re-queuing. Both are covered by
  tests now.

Reproduce with `make bench` or:

```bash
./streammesh run -config examples/bench.yaml &
./streammesh gen -url http://localhost:8081/ingest -rate 120000 -duration 12s -batch 1000 -workers 24
curl -s localhost:8080/stats
```

## 4. Non-goals

- Not a Kafka replacement - Kafka is a supported *sink/source*, not a
  reimplementation.
- No web UI. No auth provider - a single static API token guards the control plane.
- No exactly-once delivery claims: the guarantee is documented honestly as
  at-least-once plus idempotent dedup.

## 5. Definition of done

The project is production-ready when all of the following are true **and
measured, not estimated**:

1. `docker compose up` routes a synthetic stream end to end on a clean machine.
2. ~~Sustained throughput and p99 stage latency are measured and recorded in the
   README (numbers come from a real benchmark run on real hardware).~~
   Throughput is measured and recorded in [3.10](#310-measured-performance);
   per-stage p99 latency is exposed on `/stats` but still needs a dedicated run
   before it is quoted.
3. `go test -race ./...` passes with meaningful coverage, plus benchmarks for the
   hot path.
4. Killing a sink mid-stream loses no events once the sink returns (demonstrated
   in an integration test).
5. CI is green and the README shows the demo in a short GIF.

Only measured numbers go in this document - nothing is invented.

## 6. Milestones

| # | Milestone | Deliverable |
|---|---|---|
| M0 | Skeleton and contracts | module, `Event` schema, `Source`/`Sink`/`Stage` interfaces, config loader |
| M1 | Working vertical slice | HTTP ingest -> parse -> route -> file sink, running with `go run` |
| M2 | All sources and sinks | syslog, tailer, webhook, Kafka, webhook sink, S3 sink, in-memory fakes |
| M3 | Reliability layer | WAL, retry/backoff, dedup, DLQ + replay, backpressure, graceful shutdown |
| M4 | Rules engine + DLP | YAML matchers/actions, hot reload, redaction, sampling, enrichment |
| M5 | Observability + control plane | Prometheus metrics, health/ready, REST API, `slog` correlation IDs |
| M6 | Hardening and release | tests/fuzz/benchmarks, CI, Docker compose demo, README with measured numbers, v1.0 tag |

## 7. Author

Jumana B - [github.com/JumanaBaharul](https://github.com/JumanaBaharul)
