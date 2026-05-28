import Redis from "ioredis";
import { randomUUID } from 'crypto';
import fs from 'fs'

const PREFIX = '{homebrewmq}';
interface EnqueueOptions {
  priority?: number;
  delay?: number;
  affinity?: string | null;
  maxRetries?: number;
  idempotencyKey?: string | null;
}

class Queue{
     private name: string;
  private redis: Redis;

  private readyKey: string;
  private processingKey: string;
  private delayedKey: string;
  private queuesKey: string;
  private maxQueueSize: number;
private maxRetries: number;
private jobTTL: number;

  private failedKey: string;

    constructor(name:string,redisClient:Redis,options:{
        maxQueueSize?:number,
        maxRetries?:number,
        jobTTL?:number
    }={}){
        if (!name || !redisClient) {
      throw new Error('Queue requires a name and a Redis client');
    }
    this.name=name;
    this.redis=redisClient
    this.readyKey=`${PREFIX}:readyQueue`;
    this.processingKey=`${PREFIX}:processingQueue`;
    this.delayedKey=`${PREFIX}:delayedQueue`;
    this.queuesKey = `${PREFIX}:queues`;
    this.failedKey = `${PREFIX}:failedQueue`;
    this.maxQueueSize = options.maxQueueSize ?? 10000;
    this.maxRetries = options.maxRetries ?? 3;
    this.jobTTL = options.jobTTL ?? 86400;


    }
    async register(){
        await this.redis.sadd(this.queuesKey,this.name)
    }
    async  enqueue(payload:unknown,options:EnqueueOptions={}){
        const {
      priority = 0,
      delay = 0,
      idempotencyKey = null,
      affinity = null,
    } = options;
        const jobId=randomUUID();
        const now=Date.now();
        const runAt=now+delay;

        const result=await this.redis.zcard(this.readyKey);
        if(result>this.maxQueueSize){
            throw new Error('Queue is full');
        }
        const multi =  this.redis.multi();
        multi.hset(`job:${jobId}`, {
      id: jobId,
      queue: this.name,
      payload: JSON.stringify(payload),
      priority,
      affinity: affinity ?? '',
      status: delay > 0 ? 'delayed' : 'ready',
      createdAt: now,
      attempts: 0,
      maxRetries: options.maxRetries ?? this.maxRetries,
    });
    if(delay>0){
        multi.zadd(this.delayedKey,runAt,jobId)
    }else if(affinity){
         multi.zadd(`${PREFIX}:ready:${affinity}`, priority, jobId);
    } else {
        multi.zadd(this.readyKey, priority, jobId);

    }
    await multi.exec();

    

    return jobId;
    }

    

    async  claim():Promise<Record<string,string>|null>{
    const claimScript = fs.readFileSync('/path/to/claim.lua', 'utf8');
    
    const flat = await this.redis.eval(
        claimScript,
        3,
        this.readyKey,
        this.processingKey,
        this.failedKey,
        '30000'
    ) as string[] | null;
    if(!flat|| flat.length===0)return null;
    const job:Record<string,string>={};
    for(let i =0;i<flat.length;i+=2){
        job[flat[i]]=flat[i+1];
    }
    return job;
    }

}
        


export {Queue};
