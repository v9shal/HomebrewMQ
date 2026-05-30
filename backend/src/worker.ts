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

    constructor(queue:Queue,processor:(job:Job)=>Promise<void>,redisClient:Redis,workerId?:string){
        this.queue=queue;
        this.processor=processor;
        this.running=false;
        this.cb=new CircuitBreaker(redisClient)
        this.redis=redisClient;
        this.publisher = new RealtimePublisher(redisClient);
     this.workerId = workerId??`${os.hostname()}-${process.pid}-${randomUUID().slice(0, 8)}`;
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
        this.running =true;
        this.heartBeat.start();
        while(this.running){
            const job=await this.queue.claim(this.workerId);
            
           if (job === 'circuit_open') {
  await new Promise(r => setTimeout(r, 5000 + Math.random() * 1000)); 
  continue;
}
            if(!job || !job.id){
                 await new Promise(r=>setTimeout(r,500));
                 continue;
            }
            await this.process(job);
        }
    }
    stop(){
        this.running=false;
        this.heartBeat.stop();
    }
    async process(job:Job){
        await this.publisher.publish('job:claimed', { jobId: job.id, queue: this.queue.name, attempts: job.attempts });
        const handle=new JobHandle(this.redis,job.id,this.workerId);
        const extendInterval = setInterval(
    () => handle.extend(30000), 15000
    );
        try{
            await this.processor(job);
            const owned = await handle.isOwner();
        if (!owned) return;
            await this.queue.complete(job.id);
            await this.redis.zadd(`{homebrewmq}:cb:success:${this.queue.name}`, Date.now(), job.id);
            // Successful probe while half-open → reset circuit to closed
            const state = await this.cb.getState(this.queue.name);
            if (state === 'half-open') await this.cb.reset(this.queue.name);
        }
        catch(err){
            await this.queue.fail(job,err as Error);
            await this.redis.zadd(`{homebrewmq}:cb:failure:${this.queue.name}`, Date.now(), job.id);
        }
        finally{
            clearTimeout(extendInterval);
        }
    }
    
}

export {Worker};
