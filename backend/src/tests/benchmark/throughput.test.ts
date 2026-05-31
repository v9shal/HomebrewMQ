import Redis from 'ioredis';
import { Queue } from '../../queue';
import { Worker } from '../../worker';

const redis = new Redis();
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

afterAll(() => redis.quit());

describe('🚀 Production Throughput Benchmark Suite', () => {
  beforeEach(async () => {
    const keys = await redis.keys('{homebrewmq}:*');
    if (keys.length) await redis.del(...keys);
  });

  it('ENQUEUE: bulk throughput', async () => {
    const q = new Queue('bench-enqueue', redis);
    const N = 5000;

    const start = performance.now();

    await Promise.all(
      Array.from({ length: N }, (_, i) =>
        q.enqueue({ i }, { priority: i % 3 })
      )
    );

    const elapsed = performance.now() - start;

    const rps = Math.round(N / (elapsed / 1000));

    console.log('\n📊 ENQUEUE BENCH');
    console.log({ jobs: N, elapsed_ms: Math.round(elapsed), rps });

    expect(rps).toBeGreaterThan(5000);
  }, 60000);

  it('LATENCY: claim operation percentiles', async () => {
    const q = new Queue('bench-latency', redis);
    const N = 200;

    for (let i = 0; i < N; i++) {
      await q.enqueue({ i });
    }

    const latencies: number[] = [];

    for (let i = 0; i < N; i++) {
      const start = performance.now();
      await q.claim('bench-worker');
      latencies.push(performance.now() - start);
    }

    latencies.sort((a, b) => a - b);

    const p50 = latencies[Math.floor(N * 0.5)];
    const p95 = latencies[Math.floor(N * 0.95)];
    const p99 = latencies[Math.floor(N * 0.99)];

    const report = { p50, p95, p99 };

    console.log('\n📊 LATENCY REPORT');
    console.log(report);

    expect(p99).toBeLessThan(50);
  }, 30000);

  it('E2E: full system throughput', async () => {
    const q = new Queue('bench-e2e', redis);
    const N = 2000;

    let completed = 0;

    const worker = new Worker(q, async () => {
      completed++;
    }, redis, { concurrency: 10 });

    await worker.register();

    const start = performance.now();

    for (let i = 0; i < N; i++) {
      await q.enqueue({ i });
    }

    void worker.start();

    while (completed < N) {
      await sleep(50);
    }

    worker.stop();

    const elapsed = performance.now() - start;
    const tps = Math.round(N / (elapsed / 1000));

    const report = {
      jobs: N,
      elapsed_ms: Math.round(elapsed),
      throughput: tps
    };

    console.log('\n📊 E2E REPORT');
    console.log(report);

    expect(completed).toBe(N);
    expect(tps).toBeGreaterThan(1000);
  }, 60000);
});