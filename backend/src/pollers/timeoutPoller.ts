import { redis } from '../pollers/leaderElection';
import fs from 'fs';
import * as path from 'path';

const script = fs.readFileSync(path.join(__dirname, '..', 'lua', 'promote.lua'), 'utf8');

async function timeoutPoller() {
    try {
        await redis.eval(
            script,
            2,
            `{homebrewmq}:readyQueue`,
            `{homebrewmq}:delayedQueue`
        );
    } catch (error: any) {
        throw new Error(`delayedPoller: failed to promote jobs — ${error.message}`);
    }
}

export { timeoutPoller };
