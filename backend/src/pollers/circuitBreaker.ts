import { redis } from "../pollers/leaderElection";
import { CircuitBreaker } from "../utils/circuitBreaker";

const circuitBreaker = new CircuitBreaker(redis);

const OPEN_TIMEOUT_MS = 30_000;

async function circuitBreakerPoll(workerId: string) {
    try {
        const leader = await redis.get("lock:delayedPoller");

        if (leader !== workerId) {
            return;
        }

        const queues = await redis.smembers(
            "{homebrewmq}:queues"
        );

        const now = Date.now();

        for (const queue of queues) {
            const state =
                await circuitBreaker.getState(queue);

            if (state === "open") {
                const openedAt =
                    await circuitBreaker.getOpenedAt(
                        queue
                    );

                if (
                    now - openedAt >
                    OPEN_TIMEOUT_MS
                ) {
                    await redis.set(
                        `{homebrewmq}:cb:state:${queue}`,
                        "half-open"
                    );
                }

                continue;
            }

            if (state === "closed") {
                const failureRate =
                    await circuitBreaker.getFailureRate(
                        queue
                    );

                if (
                    failureRate !== null &&
                    failureRate > 0.5
                ) {
                    await circuitBreaker.trip(queue);
                }
            }
        }
    } catch (error) {
        console.error(
            "Circuit breaker poller failed:",
            error
        );
    }
}

export { circuitBreakerPoll };