import Redis from 'ioredis';

const PREFIX = '{homebrewmq}';

export interface QueueStats {
    ready: number;
    processing: number;
    delayed: number;
    failed: number;
    circuitState: 'closed' | 'open' | 'half-open';
}

export async function getStats(redis: Redis, queueName: string): Promise<QueueStats> {
    const [ready, processing, delayed, failed, state] = await Promise.all([
        redis.zcard(`${PREFIX}:readyQueue`),
        redis.zcard(`${PREFIX}:processingQueue`),
        redis.zcard(`${PREFIX}:delayedQueue`),
        redis.zcard(`${PREFIX}:failedQueue`),
        redis.get(`${PREFIX}:cb:state:${queueName}`),
    ]);

    return {
        ready,
        processing,
        delayed,
        failed,
        circuitState: (state as QueueStats['circuitState']) ?? 'closed',
    };
}

export async function getFailureRate(redis: Redis, queueName: string): Promise<number> {
    const now = Date.now();
    const windowStart = now - 60_000;

    const [success, failure] = await Promise.all([
        redis.zcount(`${PREFIX}:cb:success:${queueName}`, windowStart, now),
        redis.zcount(`${PREFIX}:cb:failure:${queueName}`, windowStart, now),
    ]);

    const total = success + failure;
    if (total < 10) return 0;
    return failure / total;
}

export async function getDLQJobs(redis: Redis): Promise<Record<string, string>[]> {
    const flat = await redis.zrange(`${PREFIX}:failedQueue`, 0, 49, 'WITHSCORES');

    const jobs: Record<string, string>[] = [];
    for (let i = 0; i < flat.length; i += 2) {
        const jobId    = flat[i];
        const failedAt = flat[i + 1];
        const hash     = await redis.hgetall(`job:${jobId}`);
        jobs.push({ ...hash, failedAt });
    }
    return jobs;
}
