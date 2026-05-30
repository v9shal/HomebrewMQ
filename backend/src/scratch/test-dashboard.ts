/**
 * Test the dashboard:
 *   - Subscribes to {homebrewmq}:events and prints every event in real time
 *   - Drives traffic (success + failure jobs) so events fire
 *   - Periodically prints getStats() snapshots
 */

import Redis from 'ioredis';
import { Queue } from '../queue';
import { Worker } from '../worker';
import { RealtimeSubscriber } from '../dashboard/realtime';
import { getStats, getFailureRate, getDLQJobs } from '../dashboard/stats';

const QUEUE_NAME = 'dashtest';
const redis = new Redis();

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

async function cleanup() {
    const buckets = await Promise.all([
        redis.keys('{homebrewmq}:*'),
        redis.keys('job:*'),
        redis.keys('worker:*'),
    ]);
    const keys = buckets.flat();
    if (keys.length) await redis.del(...keys);
}

async function main() {
    await cleanup();

    // 1. Start the realtime subscriber FIRST so we don't miss events
    const sub = new RealtimeSubscriber();
    await sub.start((event) => {
        console.log('EVENT:', JSON.stringify(event));
    });
    console.log('Subscriber listening on {homebrewmq}:events\n');

    // 2. Setup a queue + worker that sometimes throws
    const queue = new Queue(QUEUE_NAME, redis);
    await queue.register();

    let processed = 0;
    const worker = new Worker(queue, async (job) => {
        processed++;
        if (processed % 3 === 0) throw new Error('simulated failure');
    }, redis);
    await worker.register();
    void worker.start();

    // 3. Enqueue a handful of jobs
    for (let i = 0; i < 6; i++) {
        await queue.enqueue({ i }, { maxRetries: 1 });
    }
    console.log('Enqueued 6 jobs.\n');

    // 4. Poll stats while the worker drains
    for (let tick = 0; tick < 4; tick++) {
        await sleep(2000);
        const stats = await getStats(redis, QUEUE_NAME);
        const rate  = await getFailureRate(redis, QUEUE_NAME);
        console.log(`STATS @ t+${(tick + 1) * 2}s:`, stats, ` failureRate=${rate}`);
    }

    // 5. Inspect DLQ
    const dlq = await getDLQJobs(redis);
    console.log(`\nDLQ contents (${dlq.length} jobs):`);
    for (const j of dlq) {
        console.log(`  - ${j.id} attempts=${j.attempts} lastError=${j.lastError}`);
    }

    worker.stop();
    await sub.stop();
    await sleep(300);
    await redis.quit();
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
