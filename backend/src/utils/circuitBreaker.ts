import Redis from "ioredis";
import { RealtimePublisher } from "../dashboard/realtime";

class CircuitBreaker {
    private readonly redis: Redis;
    private readonly publisher: RealtimePublisher;
    private readonly WINDOW_MS = 60_000;
    private readonly MIN_SAMPLES = 10;

    constructor(redisClient: Redis) {
        this.redis = redisClient;
        this.publisher = new RealtimePublisher(redisClient);
    }

    async getFailureRate(queueName: string): Promise<number | null> {
        const now = Date.now();

        const success = await this.redis.zcount(
            `{homebrewmq}:cb:success:${queueName}`,
            now - this.WINDOW_MS,
            now
        );

        const failure = await this.redis.zcount(
            `{homebrewmq}:cb:failure:${queueName}`,
            now - this.WINDOW_MS,
            now
        );

        const total = success + failure;

        if (total < this.MIN_SAMPLES) {
            return null;
        }

        return failure / total;
    }

    async getState(
        queue: string
    ): Promise<'open' | 'closed' | 'half-open'> {
        const state = await this.redis.get(
            `{homebrewmq}:cb:state:${queue}`
        );

        if (state == null) {
            return 'closed';
        }

        return state as 'open' | 'closed' | 'half-open';
    }

    async getOpenedAt(queue: string): Promise<number> {
        const result = await this.redis.get(
            `{homebrewmq}:cb:openedAt:${queue}`
        );

        if (result == null) {
            return 0;
        }

        return Number(result);
    }

    async trip(queue: string): Promise<void> {
        const now = Date.now();

        await this.redis.mset(
            `{homebrewmq}:cb:state:${queue}`,
            'open',
            `{homebrewmq}:cb:openedAt:${queue}`,
            String(now)
        );
        await this.publisher.publish('circuit:open', { queue });
    }

    async halfOpen(queue: string): Promise<void> {
        await this.redis.set(
            `{homebrewmq}:cb:state:${queue}`,
            'half-open'
        );
        await this.publisher.publish('circuit:half-open', { queue });
    }

    async reset(queue: string): Promise<void> {
        const multi = this.redis.multi();

        multi.set(
            `{homebrewmq}:cb:state:${queue}`,
            'closed'
        );

        multi.del(
            `{homebrewmq}:cb:openedAt:${queue}`
        );

        await multi.exec();
        await this.publisher.publish('circuit:closed', { queue });
    }
}

export { CircuitBreaker };