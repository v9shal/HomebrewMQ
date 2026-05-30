# Circuit Breaker — Concept & Implementation Guide

## What Is a Circuit Breaker?

When the thing your jobs depend on (a downstream API, a database, an email
provider) goes down, every worker keeps claiming jobs, failing them, and
re-queuing them on a delay. That:

- wastes CPU
- floods logs with the same error
- hammers the already-broken service, making recovery slower
- burns through retry budgets so jobs end up in the DLQ when they shouldn't

A **circuit breaker** notices this pattern (high failure rate) and **stops
workers from claiming jobs** for a cool-off period. After the cool-off, it
sends through a small "probe" — if the probe succeeds, normal operation
resumes; if it fails, the cool-off restarts.

It's borrowed from electrical engineering: when too much current flows, the
breaker trips and cuts the circuit before something catches fire.

---

## The Three States

```
        failure rate > 50%
CLOSED ─────────────────────► OPEN
  ▲                              │
  │                              │ 30s elapsed
  │  probe job succeeds          ▼
  └──────────────────────── HALF-OPEN
                       (lets ~10% through as probes)
```

| State        | Behaviour                                                            |
|--------------|----------------------------------------------------------------------|
| **CLOSED**   | Normal. Workers claim jobs. Success/failure is tracked.              |
| **OPEN**     | Tripped. `claim()` returns an error; workers back off.               |
| **HALF-OPEN**| Trial period. ~10% of `claim()` calls succeed; rest return nil.      |

---

## Redis Keys Used

| Key                                   | Type       | Purpose                                |
|---------------------------------------|------------|----------------------------------------|
| `{homebrewmq}:cb:state:{queue}`       | String     | `closed` / `open` / `half-open`        |
| `{homebrewmq}:cb:openedAt:{queue}`    | String     | Unix ms when the circuit was tripped   |
| `{homebrewmq}:cb:success:{queue}`     | Sorted Set | Timestamps of successful jobs          |
| `{homebrewmq}:cb:failure:{queue}`     | Sorted Set | Timestamps of failed jobs              |

The success/failure sets use Redis sorted sets scored by `Date.now()`. To
compute the failure rate, we `ZCOUNT` both sets from `now - 60_000` to `+inf`
— a **sliding 60-second window**. Anything older than 60 s is ignored.

---

## Implementation Steps

### Day 12 — Build the breaker logic

#### Step 1 — `src/utils/circuitBreaker.ts`

Create a `CircuitBreaker` class wrapping a Redis client. Five methods:

| Method                                | What it does                                                                 |
|---------------------------------------|------------------------------------------------------------------------------|
| `getFailureRate(queue)`               | `ZCOUNT` success + failure in last 60 s. Return `failures / total`. Return `0` if total `< 5` samples (not enough data). |
| `getState(queue)`                     | `GET cb:state:{queue}` → default `'closed'` if absent.                       |
| `getOpenedAt(queue)`                  | `GET cb:openedAt:{queue}` → parsed int, `0` if absent.                       |
| `trip(queue)`                         | `MSET state='open', openedAt=Date.now()`.                                    |
| `reset(queue)`                        | `SET state='closed'`.                                                        |

Constants:
- `WINDOW_MS = 60_000`
- `MIN_SAMPLES = 5`

#### Step 2 — `src/pollers/circuitBreaker.ts`

Export `circuitBreakerPoll(workerId)`. Runs every 5 s, **leader only** (same
pattern as `affinityReaper.ts` — check `lock:delayedPoller == workerId` first).

For each queue in `SMEMBERS {homebrewmq}:queues`:

1. If state is `open` and `Date.now() - openedAt > 30_000` → set state to `half-open`.
2. If state is `closed` and `getFailureRate(queue) > 0.5` → call `trip(queue)`.
3. `half-open` needs no poller action — probes flow through `claim.lua`, and the
   worker resets the state back to `closed` on a successful probe.

---

### Day 13 — Wire it into the hot path

#### Step 3 — Update `src/lua/claim.lua`

Add a gate at the very top, **before any `ZPOPMIN`**. Read `queueName` from a
new `ARGV[4]`:

```lua
local queueName = ARGV[4]
if queueName and queueName ~= '' then
  local state = redis.call('GET', '{homebrewmq}:cb:state:' .. queueName)
  if state == 'open' then
    return redis.error_reply('circuit_open')
  end
  if state == 'half-open' then
    if math.random(10) > 1 then  -- 90% rejected, 10% allowed as probe
      return nil
    end
  end
end
```

#### Step 4 — Update `src/queue.ts` `claim()`

- Pass `this.name` as `ARGV[4]` in the `redis.eval` call.
- Change return type to `Promise<Job | null | 'circuit_open'>`.
- Wrap the `eval` in `try/catch`; if `err.message === 'circuit_open'` return
  the string `'circuit_open'`, else re-throw.

#### Step 5 — Update `src/worker.ts`

**In the constructor:** instantiate `this.cb = new CircuitBreaker(this.redis)`.

**In `start()` loop:** after calling `claim()`:
```ts
if (job === 'circuit_open') {
  await new Promise(r => setTimeout(r, 5000 + Math.random() * 1000)); // jitter
  continue;
}
```

**In `process()`:**
- After `complete(job.id)` → `ZADD cb:success:{queue} Date.now() job.id`
- After a successful job, if `getState(queue) === 'half-open'` → call `reset(queue)`
- After `fail(job, err)` → `ZADD cb:failure:{queue} Date.now() job.id`

#### Step 6 — Wire poller into `src/pollers/leaderElection.ts`

When the leader is acquired, alongside `delayedPoll` and `affinity`:
```ts
const cbInterval = setInterval(() => circuitBreakerPoll(workerId), 5000);
```
Remember to also `clearInterval(cbInterval)` in the renewal-failure branch.

---

## Mental Model — End-to-End Flow

1. Worker calls `queue.claim(workerId)`.
2. `claim.lua` checks `cb:state:{queue}`.
   - **closed** → claims normally.
   - **open** → returns `circuit_open` error → worker sleeps 5 s + jitter.
   - **half-open** → 10% chance to claim, 90% returns `nil`.
3. Worker processes the job and records `success` or `failure` in the sliding-window sorted set.
4. Every 5 s the leader's `circuitBreakerPoll`:
   - If failure rate > 50% (closed) → trips to **open**.
   - If 30 s passed since trip (open) → moves to **half-open**.
5. When a probe job succeeds in **half-open**, the worker resets to **closed**.

---

## Why Jitter on the Back-Off?

If every worker sleeps exactly 5 s when the circuit opens, they all wake up at
the same instant and pile in together — a **thundering herd**. Adding
`Math.random() * 1000` spreads them across a 1-second window so they probe
independently.

---

## Testing Notes

To exercise the breaker manually in `scratch/`:

1. Enqueue 10 jobs to a queue.
2. Run a worker whose processor always `throw`s.
3. After a few failures, `redis-cli GET '{homebrewmq}:cb:state:test'` should
   show `open`.
4. Wait 30 s → poller moves it to `half-open`.
5. Swap the processor to one that succeeds → on the next probe, state resets to
   `closed`.
