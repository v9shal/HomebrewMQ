import Redis from 'ioredis';
import { Queue } from '../../queue';
import { Worker } from '../../worker';

const redis = new Redis();
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

afterAll(() => redis.quit());

describe('Concurrency correctness', () => {
  beforeEach(async () => {
    const keys = await redis.keys('{homebrewmq}:*');
    const jobKeys = await redis.keys('job:*');
    const workerKeys = await redis.keys('worker:*');
    const all = [...keys, ...jobKeys, ...workerKeys];
    if (all.length) await redis.del(...all);
  });

  it('BENCHMARK: N workers, zero duplicate processing', async () => {
    const WORKER_COUNT = 100;
    const JOB_COUNT = 100000;

    const q = new Queue('concurrency-test', redis, { maxQueueSize: JOB_COUNT });
    await q.register();

    const processed = new Map<string, string[]>();
    const workers: Worker[] = [];

    for (let w = 0; w < WORKER_COUNT; w++) {
      const worker = new Worker(q, async (job) => {
        const existing = processed.get(job.id) ?? [];
        processed.set(job.id, [
          ...existing,
          worker.workerId
        ]);
      }, redis,{concurrency:5});

      await worker.register();
      workers.push(worker);
    }

    // enqueue all jobs in parallel — sequential ZCARD checks would take ~100s for large counts
    const jobIds = await Promise.all(
      Array.from({ length: JOB_COUNT }, (_, i) => q.enqueue({ i }))
    );

    // start all workers simultaneously
    const start = Date.now();
    workers.forEach(w => void w.start());

    // wait for completion
    while (
      processed.size < JOB_COUNT &&
      Date.now() - start < 180_000
    ) {
      await sleep(100);
    }
    workers.forEach(w => w.stop());

    const elapsed = Date.now() - start;

    // check for duplicates
    const duplicates = [...processed.entries()]
      .filter(([, workers]) => workers.length > 1);

    const throughput = Math.round(
      processed.size / (elapsed / 1000)
    );

    console.log(`\n  Concurrency test (${WORKER_COUNT} workers):`);
    console.log(`  ${processed.size}/${JOB_COUNT} jobs processed`);
    console.log(`  Duplicates: ${duplicates.length}`);
    console.log(`  Throughput: ${throughput} jobs/second`);
    console.log(`  Elapsed: ${elapsed}ms`);

    expect(duplicates.length).toBe(0);
    expect(processed.size).toBe(JOB_COUNT);
  }, 240_000);

  it('BENCHMARK: priority ordering is respected', async () => {
    const q = new Queue('priority-test', redis);
    const order: number[] = [];

    const worker = new Worker(q, async (job) => {
      order.push(JSON.parse(job.payload).priority);
    }, redis);
    await worker.register();

    // enqueue low priority first
    await q.enqueue({ priority: 3 }, { priority: 3 });
    await q.enqueue({ priority: 3 }, { priority: 3 });
    await q.enqueue({ priority: 0 }, { priority: 0 });
    await q.enqueue({ priority: 0 }, { priority: 0 });
    await q.enqueue({ priority: 1 }, { priority: 1 });

    void worker.start();
    await sleep(2000);
    worker.stop();

    console.log(`\n  Processing order: ${order.join(' → ')}`);

    // first two should be priority 0
    expect(order[0]).toBe(0);
    expect(order[1]).toBe(0);
    expect(order[2]).toBe(1);
  }, 10_000);
});