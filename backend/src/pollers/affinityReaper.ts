import { redis } from '../pollers/leaderElection';
import fs from 'fs';
import * as path from 'path';

const script = fs.readFileSync(path.join(__dirname, '..', 'lua', 'requeue.lua'), 'utf8');

async function affinity(workerId: string) {
    const owner =
        await redis.get('lock:delayedPoller');

    if (owner !== workerId) {
        return;
    }

    await redis.eval(
        script,
        2,
        '{homebrewmq}:readyQueue',
        '{homebrewmq}:processingQueue'
    );
}
export { affinity };
