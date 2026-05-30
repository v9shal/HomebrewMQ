import Redis from "ioredis";

class JobHandle {
    private redis: Redis;
    private jobId: string;
    private workerId: string;

    constructor(
        redisClient: Redis,
        jobId: string,
        workerId: string,
    ) {
        this.redis = redisClient;
        this.jobId = jobId;
        this.workerId = workerId;
    }

    async extend(TTL = 30000): Promise<boolean> {
        const newDeadline = Date.now() + TTL;

        const result = await this.redis.zadd(
            "{homebrewmq}:processingQueue",
            "XX",
            newDeadline,
            this.jobId
        );

        return result !== null;
    }

    async isOwner(): Promise<boolean> {
        const [owner, token] = await this.redis.hmget(
            `job:${this.jobId}`,
            "workerId",
            "leaseToken"
        );

        return (
            owner === this.workerId &&
        );
    }
}

export { JobHandle };