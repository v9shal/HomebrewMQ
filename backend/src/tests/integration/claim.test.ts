import Redis from 'ioredis';
import { Queue } from '../../queue';

const redis = new Redis();
const WORKER = 'test-worker-1';

async function freshQueue(name: string, opts = {}) {
  await redis.del(
    '{homebrewmq}:readyQueue',
    '{homebrewmq}:processingQueue',
    '{homebrewmq}:failedQueue'
  );
  return new Queue(name, redis, opts);
}

afterAll(() => redis.quit());

describe('claim', () => {
  it('returns null when queue is empty', async () => {
    const q = await freshQueue('claim-empty');
    const job = await q.claim(WORKER);
    expect(job).toBeNull();
  });

  it('moves job from readyQueue to processingQueue', async () => {
    const q = await freshQueue('claim-move');
    const jobId = await q.enqueue({ x: 1 });
    await q.claim(WORKER);

    const inReady = await redis.zscore(
      '{homebrewmq}:readyQueue', jobId
    );
    const inProcessing = await redis.zscore(
      '{homebrewmq}:processingQueue', jobId
    );

    expect(inReady).toBeNull();
    expect(inProcessing).not.toBeNull();
  });

  it('sets visibility timeout ~30s in the future', async () => {
    const q = await freshQueue('claim-timeout');
    const jobId = await q.enqueue({});
    await q.claim(WORKER);

    const deadline = Number(
      await redis.zscore('{homebrewmq}:processingQueue', jobId)
    );
    const now = Date.now();

    expect(deadline).toBeGreaterThan(now + 25_000);
    expect(deadline).toBeLessThan(now + 35_000);
  });

  it('increments attempts at claim time', async () => {
    const q = await freshQueue('claim-attempts');
    const jobId = await q.enqueue({});
    const job = await q.claim(WORKER) as any;

    expect(job.attempts).toBe(1);
    const hash = await redis.hgetall(`job:${jobId}`);
    expect(hash.attempts).toBe('1');
  });

  it('sets workerId on the job hash', async () => {
    const q = await freshQueue('claim-workerid');
    const jobId = await q.enqueue({});
    await q.claim(WORKER);

    const workerId = await redis.hget(
      `job:${jobId}`, 'workerId'
    );
    expect(workerId).toBe(WORKER);
  });

  it('routes to DLQ when attempts >= maxRetries', async () => {
    const q = await freshQueue('claim-dlq', { maxRetries: 1 });
    const jobId = await q.enqueue({}, { maxRetries: 1 });

    // manually set attempts to maxRetries
    await redis.hset(`job:${jobId}`, 'attempts', '1');

    await q.claim(WORKER);

    const inFailed = await redis.zscore(
      '{homebrewmq}:failedQueue', jobId
    );
    const inProcessing = await redis.zscore(
      '{homebrewmq}:processingQueue', jobId
    );

    expect(inFailed).not.toBeNull();
    expect(inProcessing).toBeNull();
  });

  it('two workers cannot claim the same job', async () => {
    const q = await freshQueue('claim-race');
    await q.enqueue({});

    const [job1, job2] = await Promise.all([
      q.claim('worker-A'),
      q.claim('worker-B'),
    ]);

    const claimed = [job1, job2].filter(Boolean);
    expect(claimed.length).toBe(1);
  });
});