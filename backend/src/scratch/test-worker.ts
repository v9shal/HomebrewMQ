
import Redis from 'ioredis';
import { Queue } from '../queue';
import { Worker } from '../worker';

const redis = new Redis();

async function main() {
    const queue = new Queue('test', redis);
    await queue.register();

    const worker = new Worker(queue, async (job) => {
        console.log('processing', job);
        worker.stop();
    });

    await queue.enqueue({ name: 'vishal' }, { priority: 1 });
    await worker.start();

    redis.disconnect();
}

main();
