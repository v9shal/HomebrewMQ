import Redis from "ioredis";
import { delayedPoll } from "./delayedPoller";
import { Worker } from "../worker";
import { affinity } from "./affinityReaper";
import { circuitBreakerPoll } from "./circuitBreaker";
export const redis = new Redis();

// Atomically renew the lock only if this worker still owns it
const RENEW_SCRIPT = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("EXPIRE", KEYS[1], ARGV[2])
else
  return 0
end
`;

async function leaderHoldKey(workerId: string) {
    const acquired = await redis.set(`lock:delayedPoller`, workerId, 'PX', 10000, 'NX');

    if (acquired) {
        const pollInterval = setInterval(() => delayedPoll(), 500);
          const affinityInterval =
        setInterval(() => affinity(workerId), 150000);
    const cbInterval=setInterval(() => {
    void circuitBreakerPoll(workerId);
}, 5000);

        const renewInterval = setInterval(async () => {
            const renewed = await redis.eval(RENEW_SCRIPT, 1, 'lock:delayedPoller', workerId, '10');
            if (!renewed) {
                // Lock was lost; stop polling and try to re-acquire
                clearInterval(pollInterval);
                clearInterval(renewInterval);
                clearInterval(affinityInterval);
                clearTimeout(cbInterval)
                setTimeout(() => leaderHoldKey(workerId), 1000);
            }
        }, 5000);
    } else {
        // Didn't win election; retry after the lock TTL
        setTimeout(() => leaderHoldKey(workerId), 5000);
    }
}

export default { leaderHoldKey };
