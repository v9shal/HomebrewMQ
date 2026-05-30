
import Redis from 'ioredis';

const CHANNEL = '{homebrewmq}:events';

export type RealtimeEvent =
    | 'job:enqueued'
    | 'job:claimed'
    | 'job:completed'
    | 'job:failed'
    | 'job:dlq'
    | 'circuit:open'
    | 'circuit:half-open'
    | 'circuit:closed'
    | 'worker:dead';

export class RealtimePublisher {
    constructor(private redis: Redis) {}

    async publish(event: RealtimeEvent, data: object): Promise<void> {
        await this.redis.publish(
            CHANNEL,
            JSON.stringify({ event, ...data, ts: Date.now() })
        );
    }
}

export class RealtimeSubscriber {
    private sub: Redis;

    constructor() {
        this.sub = new Redis(); // dedicated connection
    }

    async start(onEvent: (event: Record<string, unknown>) => void): Promise<void> {
        await this.sub.subscribe(CHANNEL);
        this.sub.on('message', (_channel, message) => {
            try {
                onEvent(JSON.parse(message));
            } catch {
                // malformed payload — ignore
            }
        });
    }

    async stop(): Promise<void> {
        await this.sub.unsubscribe();
        this.sub.disconnect();
    }
}