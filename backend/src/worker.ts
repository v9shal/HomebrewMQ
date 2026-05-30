import Redis from "ioredis";
import { Queue } from "./queue";
import { HeartBeat } from "./utils/heartbeat";
import { Job } from "./type";
import { randomUUID } from 'crypto';
import os from 'os';


class Worker{

    
    private running:boolean=false;
    private processor:(job:Job)=>Promise<void>
    private queue:Queue
    private redis:Redis
    private heartBeat:HeartBeat;
    readonly workerId:string;


    constructor(queue:Queue,processor:(job:Job)=>Promise<void>,redisClient:Redis,workerId?:string){
        this.queue=queue;
        this.processor=processor;
        this.running=false;
        this.redis=redisClient;
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
        try{
            await this.processor(job);
            await this.queue.complete(job.id);
        }
        catch(err){
            await this.queue.fail(job,err as Error);
        }
    }
    
}

export {Worker};
