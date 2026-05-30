import Redis from 'ioredis';
import fs from 'fs';
import path from 'path';
import { Queue } from '../queue';
import { Worker } from '../worker';

const redis = new Redis();

const requeueScript = fs.readFileSync(
    path.join(__dirname, '..', 'lua', 'requeue.lua'), 'utf8'
);

async function inspect(label: string) {
    const ready      = await redis.zrange('{homebrewmq}:readyQueue', 0, -1);
    const processing = await redis.zrange('{homebrewmq}:processingQueue', 0, -1);
    const workers    = await redis.smembers('{homebrewmq}:workers');
    const affKeys    = await redis.keys('{homebrewmq}:ready:*');

    console.log(`\n── ${label} ──`);
    console.log('readyQueue:     ', ready);
    console.log('processingQueue:', processing);
    console.log('workers set:    ', workers);
    console.log('affinity queues:', affKeys);

    for (const key of affKeys) {
        const members = await redis.zrange(key, 0, -1);
        console.log(`  ${key}:`, members);
    }
}

async function main() {
    const queue  = new Queue('test', redis);
    await queue.register();

    // 1. Create worker and register (starts heartbeat key in Redis)
    const worker = new Worker(queue, async () => {}, redis);
    await worker.register();
    // Manually write the heartbeat key so it exists before we expire it
    await redis.setex(`worker:heartbeat:${worker.workerId}`, 30, 'alive');

    console.log(`Worker ID: ${worker.workerId}`);

    // 2. Enqueue with affinity = workerId → lands in {homebrewmq}:ready:<workerId>
    const jobId = await queue.enqueue({ task: 'reaper-test' }, {
        affinity: worker.workerId,
        priority: 5,
    });
    console.log(`Enqueued job: ${jobId}`);

    await inspect('AFTER enqueue (job in affinity queue)');

    // 3. "Kill" the worker: delete its heartbeat key to simulate TTL expiry
    //    (in production you'd wait 30s; we skip that here)
    await redis.del(`worker:heartbeat:${worker.workerId}`);
    console.log('\nHeartbeat deleted — worker is now "dead"');

    // 4. Run the reaper
    await redis.eval(
        requeueScript,
        2,
        '{homebrewmq}:readyQueue',
        '{homebrewmq}:processingQueue'
    );
    console.log('Reaper ran.');

    await inspect('AFTER reaper (job should be in readyQueue)');

    // 5. Verify the job hash
    const jobHash = await redis.hgetall(`job:${jobId}`);
    console.log('\nJob hash after reaper:', jobHash);

    await redis.quit();
}

main().catch(console.error);
