import Redis from 'ioredis';
import { Queue } from '../../queue';
import { Worker } from '../../worker';

const redis = new Redis();
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

beforeEach(async () => {
  const keys = await redis.keys('{homebrewmq}:*');
  const jobKeys = await redis.keys('job:*');
  const workerKeys = await redis.keys('worker:*');
  const all = [...keys, ...jobKeys, ...workerKeys];
  if (all.length) await redis.del(...all);
});

afterAll(() => redis.quit());

describe('retry + backoff', () => {
  it('moves job to delayedQueue on failure', async () => {
    const q = new Queue('retry-test', redis, { maxRetries: 3 });
    await q.register();

    let called = 0;
    const worker = new Worker(q, async () => {
      called++;
      throw new Error('fail');
    }, redis);
    await worker.register();
    void worker.start();

    await q.enqueue({});
    await sleep(1500);
    worker.stop();

    const delayed = await redis.zcard(
      '{homebrewmq}:delayedQueue'
    );
    expect(delayed).toBe(1);
    expect(called).toBeGreaterThanOrEqual(1);
  });

  it('sends to DLQ after maxRetries exhausted', async () => {
    const q = new Queue('dlq-test', redis, { maxRetries: 2 });
    await q.register();

    const worker = new Worker(q, async () => {
      throw new Error('permanent fail');
    }, redis);
    await worker.register();

    const jobId = await q.enqueue(
      {}, { maxRetries: 2 }
    );

    // Simulate exhausted retries by setting attempts
    await redis.hset(`job:${jobId}`, 'attempts', '2');

    const job = await q.claim(worker.workerId) as any;
    // should route to DLQ inside claim.lua
    expect(job).toBeNull();

    const inDLQ = await redis.zscore(
      '{homebrewmq}:failedQueue', jobId
    );
    expect(inDLQ).not.toBeNull();
  });

  it('stores lastError in job hash on failure', async () => {
    const q = new Queue('error-test', redis);
    await q.register();

    const jobId = await q.enqueue({}, { maxRetries: 3 });
    const job = await q.claim('w1') as any;

    await q.fail(job, new Error('disk full'));

    const lastError = await redis.hget(
      `job:${jobId}`, 'lastError'
    );
    expect(lastError).toBe('disk full');
  });
});