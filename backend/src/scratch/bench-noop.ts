import Redis from 'ioredis';
import { Queue } from '../queue';
import { Worker } from '../worker';

const redis = new Redis();
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

(async () => {
  const keys = [...await redis.keys('{homebrewmq}:*'), ...await redis.keys('job:*'), ...await redis.keys('worker:*')];
  if (keys.length) await redis.del(...keys);

  const JOB_COUNT = 5000;
  const WORKER_COUNT = 5;
  const CONCURRENCY = 16;
  const q = new Queue('noop-bench', redis, { maxQueueSize: JOB_COUNT });
  await q.register();

  let done = 0;
  const workers: Worker[] = [];
  for (let i = 0; i < WORKER_COUNT; i++) {
    const w = new Worker(q, async () => { done++; }, redis, { concurrency: CONCURRENCY });
    await w.register();
    workers.push(w);
  }

  await Promise.all(Array.from({ length: JOB_COUNT }, (_, i) => q.enqueue({ i })));

  const t0 = Date.now();
  workers.forEach(w => void w.start());
  while (done < JOB_COUNT && Date.now() - t0 < 30000) await sleep(50);
  const dt = Date.now() - t0;
  workers.forEach(w => w.stop());

  console.log(`${done}/${JOB_COUNT} jobs, ${dt}ms, ${Math.round(done / (dt/1000))} jobs/s with ${WORKER_COUNT} workers x ${CONCURRENCY} concurrency`);

  await sleep(1000);
  await redis.quit();
  process.exit(0);
})();
