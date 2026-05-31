import Redis from 'ioredis';
import { Queue } from '../../queue';
import { Worker } from '../../worker';

const redis = new Redis();
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

async function safeFlush(pattern: string) {
  let cursor = '0';
  const pipeline = redis.pipeline();

  do {
    const [next, keys] = await redis.scan(
      cursor,
      'MATCH',
      pattern,
      'COUNT',
      1000
    );

    cursor = next;
    keys.forEach(k => pipeline.del(k));
  } while (cursor !== '0');

  await pipeline.exec();
}

afterAll(() => redis.quit());

describe('🚀 Production Concurrency Benchmark Suite', () => {
  beforeEach(async () => {
    await safeFlush('{homebrewmq}:*');
    await safeFlush('job:*');
    await safeFlush('worker:*');
  });

  it('CONCURRENCY: zero duplicate execution under heavy load', async () => {
    const WORKERS = 100;
    const JOBS = 50000;

    const q = new Queue('bench-concurrency', redis, {
      maxQueueSize: JOBS
    });

    await q.register();

    const processed = new Map<string, number>();
    const workers: Worker[] = [];

    for (let i = 0; i < WORKERS; i++) {
      const worker = new Worker(q, async (job) => {
        processed.set(job.id, (processed.get(job.id) ?? 0) + 1);
      }, redis);

      await worker.register();
      workers.push(worker);
    }

    const start = performance.now();

    await Promise.all(
      Array.from({ length: JOBS }, (_, i) =>
        q.enqueue({ i })
      )
    );

    workers.forEach(w => void w.start());

    while (processed.size < JOBS) {
      await sleep(100);
    }

    workers.forEach(w => w.stop());

    const elapsed = performance.now() - start;

    let duplicates = 0;
    for (const [, count] of processed) {
      if (count > 1) duplicates++;
    }

    const throughput = Math.round(JOBS / (elapsed / 1000));

    const report = {
      workers: WORKERS,
      jobs: JOBS,
      elapsed_ms: Math.round(elapsed),
      throughput_jobs_sec: throughput,
      duplicates
    };

    console.log('\n📊 CONCURRENCY REPORT');
    console.log(JSON.stringify(report, null, 2));

    expect(duplicates).toBe(0);
    expect(processed.size).toBe(JOBS);
  }, 300000);

  it('PRIORITY: ordering correctness', async () => {
    const q = new Queue('bench-priority', redis);
    const order: number[] = [];

    const worker = new Worker(q, async (job) => {
      order.push(JSON.parse(job.payload).priority);
    }, redis);

    await worker.register();

    await q.enqueue({ priority: 3 }, { priority: 3 });
    await q.enqueue({ priority: 0 }, { priority: 0 });
    await q.enqueue({ priority: 1 }, { priority: 1 });
    await q.enqueue({ priority: 0 }, { priority: 0 });
    await q.enqueue({ priority: 3 }, { priority: 3 });

    void worker.start();
    await sleep(2000);
    worker.stop();

    console.log('\n📊 PRIORITY ORDER:', order.join(' → '));

    expect(order[0]).toBe(0);
  }, 20000);
});