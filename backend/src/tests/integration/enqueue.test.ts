import Redis from 'ioredis';
import { Queue } from '../../queue';

const redis = new Redis();
const queue = new Queue('enqueue-test', redis, { maxQueueSize: 5 });

beforeEach(async () => {
  await redis.del(
    '{homebrewmq}:readyQueue',
    '{homebrewmq}:delayedQueue',
    '{homebrewmq}:queues'
  );
});

afterAll(() => redis.quit());

describe('enqueue', () => {
  it('writes job HASH with correct fields', async () => {
    const jobId = await queue.enqueue(
      { task: 'test' }, { priority: 2 }
    );
    const hash = await redis.hgetall(`job:${jobId}`);

    expect(hash.id).toBe(jobId);
    expect(hash.status).toBe('ready');
    expect(hash.attempts).toBe('0');
    expect(hash.queue).toBe('enqueue-test');
    expect(JSON.parse(hash.payload)).toEqual({ task: 'test' });
  });

  it('adds jobId to readyQueue ZSET', async () => {
    const jobId = await queue.enqueue({}, { priority: 3 });
    const score = await redis.zscore(
      '{homebrewmq}:readyQueue', jobId
    );
    expect(Number(score)).toBe(3);
  });

  it('routes delayed jobs to delayedQueue', async () => {
    const before = Date.now();
    const jobId = await queue.enqueue({}, { delay: 5000 });
    const score = await redis.zscore(
      '{homebrewmq}:delayedQueue', jobId
    );
    expect(Number(score)).toBeGreaterThanOrEqual(before + 5000);
  });

  it('throws when queue is full', async () => {
    for (let i = 0; i < 5; i++) {
      await queue.enqueue({ i });
    }
    await expect(queue.enqueue({ overflow: true }))
      .rejects.toThrow('Queue is full');
  });

  it('returns unique jobIds', async () => {
    const bigQueue = new Queue('enqueue-unique', redis);
    const ids = await Promise.all(
      Array.from({ length: 20 }, () => bigQueue.enqueue({}))
    );
    expect(new Set(ids).size).toBe(20);
  });
});