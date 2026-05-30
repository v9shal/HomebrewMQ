# HomebrewMQ

A Redis-backed, Cluster-compatible distributed job queue written from scratch in TypeScript. Built to learn the hard parts: priority + delayed scheduling, visibility timeouts, worker fencing, dead-worker recovery, circuit breaking, and a live event stream — all coordinated through atomic Lua scripts.

> Status: working end-to-end. Single-node Redis verified; Cluster-safe by design (all keys use the `{homebrewmq}` hash tag).

---

## Table of Contents

1. [Why](#why)
2. [Feature Matrix](#feature-matrix)
3. [Architecture at a Glance](#architecture-at-a-glance)
4. [Redis Data Model](#redis-data-model)
5. [Job Lifecycle](#job-lifecycle)
6. [Core Components](#core-components)
7. [Lua Scripts (Atomic Operations)](#lua-scripts-atomic-operations)
8. [Leader-Elected Pollers](#leader-elected-pollers)
9. [Circuit Breaker](#circuit-breaker)
10. [Worker Liveness & Recovery](#worker-liveness--recovery)
11. [Observability: Stats + Realtime](#observability-stats--realtime)
12. [Project Layout](#project-layout)
13. [Running Locally](#running-locally)

---

## Why

Most queues hide the interesting parts. HomebrewMQ exposes them: every state transition is a single atomic Redis primitive or Lua script, and every coordination decision (who polls, who reaps, when to trip) is explicit. The goal is correctness under partial failure — workers crashing mid-job, lost network packets between claim and complete, slow consumers swamping a downstream — without a single global lock.

## Feature Matrix

| Capability                | Mechanism                                                                 |
| ------------------------- | ------------------------------------------------------------------------- |
| Priority scheduling       | `ZADD` to `readyQueue` with priority as score                             |
| Delayed jobs              | `ZADD` to `delayedQueue` with `runAt` as score, promoted every 500ms      |
| Worker affinity (sticky)  | Per-worker `ready:{workerId}` ZSET, checked first in `claim.lua`          |
| Atomic claim              | `claim.lua` (CB check → ZREM → ZADD processing → HINCRBY attempts)        |
| Visibility timeout / lease| `processingQueue` ZSET scored by deadline; `JobHandle.extend()` slides it |
| Lease fencing             | `JobHandle.isOwner()` re-reads `workerId` before ack to prevent double-ack|
| Retries with backoff      | Exponential + jitter (`backoff.ts`); rebounds via `delayedQueue`          |
| Dead-letter queue         | `failedQueue` ZSET, populated once `attempts >= maxRetries`               |
| Bounded queue size        | `ZCARD` check inside `enqueue.lua` → `QUEUE_FULL` error reply             |
| Heartbeats                | `worker:heartbeat:{id}` `SETEX` every 10s, TTL 30s                        |
| Dead-worker reaper        | `requeue.lua` returns affinity + in-flight jobs to global ready queue     |
| Leader election           | `SET NX PX 10000` + atomic Lua renewal; loser retries after TTL           |
| Circuit breaker           | Sliding-window failure rate per queue → open / half-open / closed         |
| Half-open probing         | `SET NX EX 5` probe lock — exactly one worker tries at a time             |
| Realtime event stream     | Redis Pub/Sub on `{homebrewmq}:events`                                    |
| Pull-based snapshots      | `getStats`, `getFailureRate`, `getDLQJobs`                                |
| Redis Cluster safe        | All keys share the `{homebrewmq}` hash tag → single slot                  |

---

## Architecture at a Glance

```
                                       ┌────────────────────────────┐
                                       │  Producers (queue.enqueue) │
                                       └──────────────┬─────────────┘
                                                      │ enqueue.lua
                                                      ▼
       ┌─────────────────────────┐    promote.lua   ┌─────────────────────────┐
       │     delayedQueue        │ ───────────────► │       readyQueue        │
       │  (ZSET score = runAt)   │   (every 500ms)  │ (ZSET score = priority) │
       └─────────────────────────┘                  └──────────────┬──────────┘
                                                                   │ claim.lua
                                                                   ▼
                                                       ┌────────────────────────┐
                                                       │    processingQueue     │
                                                       │ (ZSET score = deadline)│
                                                       └──────┬─────────────────┘
                                                              │
                                            success ◄─────────┼─────────► failure
                                                              │
                              complete()                      │              fail()
                                  │                           │                │
                                  ▼                           ▼                ▼
                              DEL job:*       (attempts < max)            (attempts ≥ max)
                                              ZADD delayedQueue           ZADD failedQueue
                                              + backoff()                 + status=failed
                                                                          (DLQ)
```

Coordination layer (single leader, elected via `lock:delayedPoller`):

```
┌──────────────────────────────────────────────────────────────────┐
│                       Leader-only tickers                        │
├──────────────────────────────────────────────────────────────────┤
│  delayedPoll        every  500ms  ── promote.lua                 │
│  circuitBreakerPoll every    5s   ── trip / half-open per queue  │
│  affinityReaper     every  150s   ── requeue.lua                 │
│  renewLock          every    5s   ── atomic EXPIRE if still mine │
└──────────────────────────────────────────────────────────────────┘
```

---

## Redis Data Model

All keys use the hash tag `{homebrewmq}` so they collocate on a single Redis Cluster slot. Job hashes (`job:{id}`) are NOT hash-tagged because they are only accessed by one component at a time and never combined with queue keys in the same script.

| Key                                          | Type   | Purpose                                                  |
| -------------------------------------------- | ------ | -------------------------------------------------------- |
| `{homebrewmq}:readyQueue`                    | ZSET   | Globally-available jobs, score = priority (higher first? — current impl uses lowest score first via `ZRANGE 0 0`) |
| `{homebrewmq}:delayedQueue`                  | ZSET   | Future-scheduled or backoff-rescheduled jobs, score = `runAt` ms |
| `{homebrewmq}:processingQueue`               | ZSET   | In-flight jobs, score = visibility deadline (ms)         |
| `{homebrewmq}:failedQueue`                   | ZSET   | DLQ, score = failure timestamp                           |
| `{homebrewmq}:ready:{workerId}`              | ZSET   | Per-worker affinity / sticky queue                       |
| `{homebrewmq}:queues`                        | SET    | All registered queue names                               |
| `{homebrewmq}:workers`                       | SET    | All registered worker IDs                                |
| `job:{id}`                                   | HASH   | Job record: payload, status, attempts, workerId, lastError |
| `worker:{id}`                                | HASH   | Worker metadata: hostname, pid, queue, registeredAt      |
| `worker:heartbeat:{id}`                      | STRING | TTL'd liveness key (`SETEX 30 alive`, pulsed every 10s)  |
| `lock:delayedPoller`                         | STRING | Leader-election lock (NX PX 10000)                       |
| `{homebrewmq}:cb:state:{queue}`              | STRING | `closed` (default) / `open` / `half-open`                |
| `{homebrewmq}:cb:openedAt:{queue}`           | STRING | Epoch ms the CB tripped                                  |
| `{homebrewmq}:cb:success:{queue}`            | ZSET   | Sliding-window successes, score = ts                     |
| `{homebrewmq}:cb:failure:{queue}`            | ZSET   | Sliding-window failures, score = ts                      |
| `{homebrewmq}:cb:probe:{queue}`              | STRING | Half-open single-probe lock (`NX EX 5`)                  |
| `{homebrewmq}:events`                        | PUBSUB | Realtime event channel                                   |

---

## Job Lifecycle

```
            enqueue()
                │
       delay>0? │  yes ─► delayedQueue ──[promote.lua]──┐
                │                                       │
                └──── no ──────────────────────────────►┴───► readyQueue
                                                              │  (or ready:{workerId} if affinity)
                                                              │
                                         claim.lua ◄──────────┘
                                              │
                                ┌─────────────┴─────────────┐
                       CB open? │                           │
                          ▼     │                           │
                  error: 'circuit_open'                     │
                  (worker sleeps 5s)                        ▼
                                                  ZREM ready → ZADD processing
                                                  HINCRBY attempts
                                                  HSET status=processing,workerId
                                                              │
                                                              ▼
                                                       processor(job)
                                                              │
                                                ┌─────────────┴─────────────┐
                                            success                       throw
                                                │                           │
                                       JobHandle.isOwner()                  │
                                       (fencing check)                      │
                                                │                           │
                                          complete()                      fail()
                                                │                           │
                                  ZREM processing                ZREM processing
                                  DEL job:*                              │
                                  publish job:completed                  │
                                  cb:success ZADD                cb:failure ZADD
                                                          ┌──────────────┴──────────────┐
                                                attempts<max                       attempts≥max
                                                ZADD delayedQueue                  ZADD failedQueue
                                                (backoff+jitter)                   HSET status=failed
                                                publish job:failed                 publish job:dlq
```

### State Machine

```
enqueued ──► ready ──► processing ──┬──► completed (DEL)
                  ▲                 │
                  │                 ├──► delayed ──► ready  (retry path)
                  │                 │
                  └─── (reaper) ────┴──► failed (DLQ, terminal)
```

### Key Invariants

- **Single-claim**: `ZREM` returns the number of elements removed. If two workers race, only one gets `removed == 1`; the loser returns `nil`.
- **Visibility timeout**: a job in `processingQueue` has its score set to `now + 30s`. If a worker dies, the affinityReaper/dead-worker logic returns it to ready before another component re-scores it.
- **Fencing**: before calling `complete()`, the worker checks `JobHandle.isOwner()` — re-reads `job:{id}.workerId` and refuses to ack if it isn't the current owner. This prevents a slow worker from acking a job that has been reassigned.
- **Lease extension**: long-running jobs call `JobHandle.extend(30000)` every 15s via `setInterval`. `ZADD XX` ensures the score is only updated if the job is still in `processingQueue`.

---

## Core Components

### `Queue` (`backend/src/queue.ts`)

- `enqueue(payload, opts)` — picks a target ZSET (delayed / affinity / global ready) and atomically size-checks + writes via `enqueue.lua`.
- `claim(workerId)` — runs `claim.lua`; returns a `Job`, `null`, or the sentinel `'circuit_open'`.
- `complete(jobId)` — `ZREM processing` + `DEL job:*` in a `MULTI`.
- `fail(job, error)` — branches on `attempts >= maxRetries`: terminal failures land in `failedQueue`; retryable ones rebound through `delayedQueue` with exponential backoff.

### `Worker` (`backend/src/worker.ts`)

- Generates a stable `workerId` (`<host>-<pid>-<uuid8>`).
- `register()` adds itself to `{homebrewmq}:workers` and writes a `worker:{id}` metadata hash.
- `start()` runs the claim loop:
  - `claim()` → `'circuit_open'` ⇒ sleep ~5s (jittered) and retry.
  - `null` ⇒ idle sleep 500ms.
  - otherwise ⇒ `process(job)`.
- `process(job)` publishes `job:claimed`, starts a 15s lease-extension interval, runs the user processor, fences via `isOwner()`, then `complete()` or `fail()`. Records success/failure timestamps in the circuit breaker windows. If a successful probe finishes while CB is `half-open`, it resets the CB to `closed`.

### `HeartBeat` (`backend/src/utils/heartbeat.ts`)

`SETEX worker:heartbeat:{id} 30 alive` every 10s. TTL of 30s gives 3 missed pulses before the key disappears.

### `JobHandle` (`backend/src/utils/jobHandle.ts`)

Two methods:
- `extend(ttl)` — `ZADD XX` against `processingQueue`; no-op if the job is no longer there.
- `isOwner()` — fencing check before ack.

### `CircuitBreaker` (`backend/src/utils/circuitBreaker.ts`)

State transitions only. Tripping decisions live in the poller.
- `getFailureRate(queue)` — over the last 60s; returns `null` if fewer than 10 samples.
- `trip(queue)` — `MSET state=open openedAt=now` + publish `circuit:open`.
- `halfOpen(queue)` — `SET state=half-open` + publish `circuit:half-open`.
- `reset(queue)` — `MULTI: SET state=closed; DEL openedAt` + publish `circuit:closed`.

---

## Lua Scripts (Atomic Operations)

### `enqueue.lua`
- Atomic `ZCARD` cap check → `redis.error_reply('QUEUE_FULL')` if full.
- `HSET` the job hash + `ZADD` it to the target queue in the same script.

### `claim.lua`
Single script that handles affinity, CB gating, retry-exhaustion sweeping, and claiming. In order:
1. Peek at the per-worker affinity ZSET; fall back to `readyQueue`.
2. Read the job's `queue` field, then check `cb:state:{queue}`.
   - `open` → `error_reply('circuit_open')`.
   - `half-open` → try `SET cb:probe:{queue} workerId NX EX 5`. If lost, return `nil` (another worker is probing).
3. `ZREM` from the source queue — if it's already gone (race), return `nil`.
4. If `attempts >= maxRetries` already, sweep it directly into `failedQueue` and return `nil`.
5. Otherwise `ZADD processingQueue` with deadline, `HINCRBY attempts`, `HSET status=processing workerId=…`, and return the full hash.

### `promote.lua`
`ZRANGEBYSCORE delayedQueue -INF now LIMIT 0 100`. For each due job: move to `readyQueue` with its original priority, flip `status=ready`. Runs every 500ms on the leader.

### `requeue.lua`
Dead-worker reaper:
1. `SMEMBERS {homebrewmq}:workers`; for each, check `worker:heartbeat:{id}`. Missing ⇒ mark dead; `SREM` from set; `DEL worker:{id}`.
2. For each dead worker, drain its affinity ZSET (`ready:{workerId}`) back into `readyQueue`. Bypasses `maxQueueSize` — stranded jobs must not be dropped.
3. Walk `processingQueue` and for any job whose `workerId` is dead, return it to `readyQueue` at its original priority and clear `status`+`workerId`.

---

## Leader-Elected Pollers

`leaderElection.ts` runs the same pattern every node:

```
SET lock:delayedPoller <workerId> NX PX 10000
   ├── acquired → start tickers; renew every 5s via atomic Lua
   │              (EXPIRE iff GET == workerId)
   └── lost     → retry in 5s (lock TTL)
```

When the renewal Lua returns 0 (lost the lock — e.g., GC pause longer than 10s), all tickers are cleared and the node re-enters the election after a 1s grace period. The other pollers (`affinityReaper`, `circuitBreakerPoll`) also re-check `GET lock:delayedPoller == workerId` defensively, so a tick that fires across an election boundary is a no-op.

---

## Circuit Breaker

Per-queue, lazy, sliding-window. Two ZSETs of timestamps; `ZCOUNT` over `[now-60s, now]` gives the recent rates.

### Trip Condition

Evaluated every 5s by the leader's `circuitBreakerPoll`:
- For each registered queue:
  - If `closed` and `failureRate > 0.5` (with ≥10 samples), `trip()`.
  - If `open` and `Date.now() - openedAt > 30_000`, `halfOpen()`.

### Half-Open Probe

The state alone isn't enough to single-thread the probe — `claim.lua` enforces it with a `SET cb:probe:{queue} workerId NX EX 5`. Only one worker holds the probe lock at a time; everyone else returns `nil` from `claim()` until either the probe succeeds (worker resets state to `closed` in `process()`) or fails and feeds the failure ZSET again, re-tripping on the next 5s tick.

### State Diagram

```
                 failureRate > 0.5
       ┌──────────────────────────────────┐
       │                                  ▼
   ┌────────┐  openedAt+30s   ┌─────────────┐  successful probe   ┌────────┐
   │ closed │ ──────────────► │  half-open  │ ──────────────────► │ closed │
   └────────┘                 └─────────────┘                     └────────┘
                                     │
                              failed probe
                              (rate > 0.5)
                                     ▼
                              back to `open`
```

---

## Worker Liveness & Recovery

- Each `Worker` runs `HeartBeat.pulse()` every 10s. The key TTL is 30s — three strikes.
- The leader's `affinityReaper` runs `requeue.lua` every 150s (in addition to demand-driven cases). The script handles two distinct stranded-job classes:
  1. Affinity-queued jobs that targeted a now-dead worker.
  2. In-flight jobs in `processingQueue` whose `workerId` is no longer alive.
- Both classes go back to `readyQueue` with their original priority and a cleared `workerId`/`affinity`. The next worker to call `claim()` picks them up like any other job, including the standard `HINCRBY attempts` — so retry counts are preserved.

---

## Observability: Stats + Realtime

Two complementary surfaces. A dashboard would pull `getStats()` periodically to render gauges, and subscribe to the realtime channel to render a streaming feed.

### Pull-based (`backend/src/dashboard/stats.ts`)

```ts
getStats(redis, 'orders')
// {
//   ready: 12, processing: 4, delayed: 7, failed: 1,
//   circuitState: 'closed'
// }

getFailureRate(redis, 'orders')   // 0..1 (or 0 if <10 samples)

getDLQJobs(redis)                 // [{ id, attempts, lastError, failedAt, ... }, ...]
```

All four ZCARDs + the CB state GET fire in parallel via `Promise.all`.

### Push-based (`backend/src/dashboard/realtime.ts`)

A Redis Pub/Sub channel `{homebrewmq}:events` carrying JSON envelopes:

```json
{ "event": "job:claimed", "jobId": "...", "queue": "orders", "attempts": 1, "ts": 1780171313621 }
```

Event taxonomy:

| Event              | Emitted by                                | Fields                              |
| ------------------ | ----------------------------------------- | ----------------------------------- |
| `job:enqueued`     | `Queue.enqueue()`                         | jobId, queue, priority              |
| `job:claimed`      | `Worker.process()` (after claim)          | jobId, queue, attempts              |
| `job:completed`    | `Queue.complete()`                        | jobId, queue                        |
| `job:failed`       | `Queue.fail()` when attempts < max        | jobId, queue, error, attempts       |
| `job:dlq`          | `Queue.fail()` when attempts ≥ max        | jobId, queue, error, attempts       |
| `circuit:open`     | `CircuitBreaker.trip()`                   | queue                               |
| `circuit:half-open`| `CircuitBreaker.halfOpen()`               | queue                               |
| `circuit:closed`   | `CircuitBreaker.reset()`                  | queue                               |
| `worker:dead`      | (reserved — not yet wired into requeue.lua) | workerId                          |

The `RealtimeSubscriber` opens a dedicated `ioredis` connection (Pub/Sub mode is exclusive) and exposes `start(onEvent)` / `stop()`.

---

## Project Layout

```
backend/src/
├── queue.ts                  # Queue: enqueue, claim, complete, fail
├── worker.ts                 # Worker loop, CB sample recording, fencing
├── type.ts                   # Job interface
├── dashboard/
│   ├── stats.ts              # Pull-based snapshots
│   └── realtime.ts           # Pub/Sub publisher + subscriber
├── lua/
│   ├── enqueue.lua           # atomic size-cap + HSET + ZADD
│   ├── claim.lua             # affinity → CB gate → ZREM → processing
│   ├── promote.lua           # delayed → ready
│   └── requeue.lua           # dead-worker reaper
├── pollers/
│   ├── leaderElection.ts     # SET NX PX + atomic renewal
│   ├── delayedPoller.ts      # 500ms promote.lua tick
│   ├── timeoutPoller.ts      # (currently mirrors delayedPoller — kept for naming)
│   ├── affinityReaper.ts     # 150s requeue.lua tick
│   └── circuitBreaker.ts     # 5s trip/halfOpen decisions
├── utils/
│   ├── backoff.ts            # exponential + jitter, capped at 30s
│   ├── heartbeat.ts          # SETEX every 10s, TTL 30s
│   ├── idempotency.ts        # placeholder
│   └── jobHandle.ts          # extend() + isOwner() fencing
└── scratch/                  # end-to-end test scripts
    ├── test-worker.ts
    ├── test-lua.ts
    └── test-dashboard.ts
```

---

## Running Locally

Prereqs: Redis 7+ on `localhost:6379`, Node 20+.

```bash
cd backend
npm install

# Dashboard smoke test — runs a worker, enqueues 6 jobs (some failing),
# streams events from {homebrewmq}:events, and prints stats every 2s.
cd src/scratch
npx ts-node test-dashboard.ts
```

Watch the Redis side:

```bash
redis-cli MONITOR
# or, just the event stream:
redis-cli SUBSCRIBE '{homebrewmq}:events'
```

---

## Design Notes & Tradeoffs

- **Lua over MULTI** for anything that branches on read values. `MULTI/EXEC` is atomic but can't make decisions; Lua gives both atomicity and conditionals at the cost of inline scripting.
- **Hash tags everywhere** so a future `Redis.Cluster` swap is a one-line change. The price is that all queue traffic concentrates on one slot — fine for thousands of jobs/sec, not for millions.
- **No explicit consumer groups**. Stream-based queues (Redis Streams + XREADGROUP) give some of this for free; ZSETs were chosen for ordered priority + delayed scheduling in the same primitive.
- **Idempotency** has a placeholder file but isn't wired. Currently a worker that crashes between `processor()` succeeding and `complete()` will retry the side effect. The fencing check (`isOwner()`) addresses double-ack, not double-execution — those are different problems.
- **Lock TTL = 10s, renewal every 5s.** Survives one missed renewal. Longer TTL → longer downtime on leader crash; shorter → flakier under GC pauses. 10s/5s is the conventional sweet spot for Redis-based locks.
