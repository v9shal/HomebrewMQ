import Redis from "ioredis";
import { Queue } from "./queue";
import { HeartBeat } from "./utils/heartbeat";
import { Job } from "./type";
import { randomUUID } from 'crypto';
import os from 'os';
import { CircuitBreaker } from "./utils/circuitBreaker";
import { JobHandle } from "./utils/jobHandle";
import { RealtimePublisher } from "./dashboard/realtime";


class Worker{

    
    private running:boolean=false;
    private processor:(job:Job)=>Promise<void>
    private queue:Queue
    private redis:Redis
    private heartBeat:HeartBeat;
    readonly workerId:string;
    private cb:CircuitBreaker
    private publisher:RealtimePublisher;
    private concurrency:number;

    constructor(
        queue:Queue,
        processor:(job:Job)=>Promise<void>,
        _redisClient:Redis,
        options:{ workerId?:string; concurrency?:number } = {}
    ){
        // Dedicated connection per worker — ioredis serializes commands on a single
        // socket, so sharing one client across workers makes them queue behind each
        // other and kills horizontal scaling.
        const ownRedis = new Redis();
        this.queue = new Queue(queue.name, ownRedis);
        this.processor=processor;
        this.running=false;
        this.cb=new CircuitBreaker(ownRedis)
        this.redis=ownRedis;
        this.publisher = new RealtimePublisher(ownRedis);
        this.workerId = options.workerId ?? `${os.hostname()}-${process.pid}-${randomUUID().slice(0, 8)}`;
        this.concurrency = options.concurrency ?? 1;
        this.heartBeat = new HeartBeat(this.redis, this.workerId);
    }
    async register(){
        const multi = this.redis.multi();
        multi.sadd('{homebrewmq}:workers', this.workerId);
        multi.hset(`worker:${this.workerId}`,
            'workerId',      this.workerId,
            'queue',         this.queue.name,
            'hostname',      os.hostname(),
            'pid',           String(process.pid),
            'status',        'idle',
            'registeredAt',  String(Date.now()),
        );
        await multi.exec();
    }
    
    async start(){
        this.running = true;
        this.heartBeat.start();
        const active = new Set<Promise<void>>();

        while (this.running) {
            if (active.size >= this.concurrency) {
                await Promise.race(active);
                continue;
            }
            const job = await this.queue.claim(this.workerId);
            if (job === 'circuit_open') {
                await new Promise(r => setTimeout(r, 5000 + Math.random() * 1000));
                continue;
            }
            if (!job || !job.id) {
                await new Promise(r => setTimeout(r, 500));
                continue;
            }
            const p = this.process(job).finally(() => { active.delete(p); });
            active.add(p);
        }
        await Promise.all(active);
    }
    stop(){
        this.running=false;
        this.heartBeat.stop();
        // Defer quit until the claim loop unwinds to avoid racing in-flight
        // commands. Errors after disconnect are swallowed.
        setTimeout(() => {
            void this.redis.quit().catch(() => this.redis.disconnect());
        }, 600);
    }
    async process(job:Job){
        await this.publisher.publish('job:claimed', { jobId: job.id, queue: this.queue.name, attempts: job.attempts });
        const handle=new JobHandle(this.redis,job.id,this.workerId);
        const extendInterval = setInterval(
    () => handle.extend(30000), 15000
    );
        try {
  await this.processor(job);
  const owned = await handle.isOwner();
  if (!owned) return;

  // pipeline all success writes into one round trip
  const pipeline = this.redis.pipeline();
  pipeline.zrem(`{homebrewmq}:processingQueue`, job.id);
  pipeline.del(`job:${job.id}`);
  pipeline.zadd(`{homebrewmq}:cb:success:${this.queue.name}`, Date.now(), job.id);
  pipeline.publish('{homebrewmq}:events', JSON.stringify({
    event: 'job:completed', jobId: job.id, queue: this.queue.name, ts: Date.now()
  }));
  await pipeline.exec();

  const state = await this.cb.getState(this.queue.name);
  if (state === 'half-open') await this.cb.reset(this.queue.name);
} catch (err) {
  await this.queue.fail(job, err as Error);
  await this.redis.zadd(`{homebrewmq}:cb:failure:${this.queue.name}`, Date.now(), job.id);
} finally {
  clearInterval(extendInterval); // ← fixed
}
    }
    
}

export {Worker};
