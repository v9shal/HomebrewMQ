/**
 * End-to-end test for the circuit breaker.
 *
 * Phase 1: enqueue many jobs, run a worker that always throws.
 *          Verify failures accumulate and the poller trips the circuit to 'open'.
 * Phase 2: simulate 30s of cool-off, invoke poller, verify state moves to 'half-open'.
 * Phase 3: swap to a succeeding processor, verify a probe job succeeds and resets to 'closed'.
 */

import Redis from 'ioredis';
import { Queue } from '../queue';
import { Worker } from '../worker';
import { CircuitBreaker } from '../utils/circuitBreaker';
import { circuitBreakerPoll } from '../pollers/circuitBreaker';

const QUEUE_NAME = 'cbtest';
const redis = new Redis();
const cb = new CircuitBreaker(redis);

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

async function cleanup() {
    const keys = await redis.keys('{homebrewmq}:*');
    if (keys.length) await redis.del(...keys);
    const jobKeys = await redis.keys('job:*');
    if (jobKeys.length) await redis.del(...jobKeys);
    const workerKeys = await redis.keys('worker:*');
    if (workerKeys.length) await redis.del(...workerKeys);
    await redis.del('lock:delayedPoller');
}

async function showState(label: string) {
    const state    = await cb.getState(QUEUE_NAME);
    const openedAt = await cb.getOpenedAt(QUEUE_NAME);
    const success  = await redis.zcard(`{homebrewmq}:cb:success:${QUEUE_NAME}`);
    const failure  = await redis.zcard(`{homebrewmq}:cb:failure:${QUEUE_NAME}`);
    const rate     = await cb.getFailureRate(QUEUE_NAME);
    console.log(
        `\n── ${label} ──\n` +
        `  state=${state}  openedAt=${openedAt || '-'}\n` +
        `  success=${success}  failure=${failure}  failureRate=${rate}`
    );
}

async function main() {
    await cleanup();
    console.log('Clean slate.');

    const queue = new Queue(QUEUE_NAME, redis);
    await queue.register();

    // Phase 1 — make the worker fail repeatedly ---------------------------
    let throwing = true;
    const worker = new Worker(queue, async (job) => {
        if (throwing) throw new Error('boom');
        console.log(`  ✓ processed ${job.id}`);
    }, redis);
    await worker.register();

    // Pretend this worker is the leader so circuitBreakerPoll runs
    await redis.set('lock:delayedPoller', worker.workerId);

    // Enqueue 12 jobs with maxRetries=1 so each contributes exactly one failure
    for (let i = 0; i < 12; i++) {
        await queue.enqueue({ i }, { maxRetries: 1 });
    }
    console.log('Enqueued 12 failing jobs.');

    void worker.start(); // run in background

    // Wait for failures to accumulate (need >= MIN_SAMPLES=10 in the window)
    await sleep(4000);
    await showState('After ~4s of failing');

    // Manually invoke the poller (in production this runs every 5s)
    await circuitBreakerPoll(worker.workerId);
    await showState('After 1st poll → expect state=open');

    // Phase 2 — simulate 30s cool-off ------------------------------------
    // Backdate openedAt so we don't actually wait 30 real seconds
    await redis.set(
        `{homebrewmq}:cb:openedAt:${QUEUE_NAME}`,
        String(Date.now() - 31_000)
    );
    await circuitBreakerPoll(worker.workerId);
    await showState('After backdate + poll → expect state=half-open');

    // Phase 3 — swap to a succeeding processor ---------------------------
    throwing = false;
    console.log('\nProcessor switched to succeed. Enqueuing probe jobs...');

    // Enqueue several jobs so at least one gets past the 10% probe gate
    for (let i = 0; i < 30; i++) {
        await queue.enqueue({ probe: i }, { maxRetries: 1 });
    }

    // Worker will probe; on success it resets the breaker
    await sleep(8000);
    await showState('After probe attempts → expect state=closed');

    worker.stop();
    await sleep(200);
    await redis.quit();
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
