import Redis from "ioredis";
import { randomUUID } from 'crypto';
import fs from 'fs'

import * as path from 'path';
import { backoff } from "./utils/backoff";

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

        // Decide which queue and score to use
        let targetKey: string;
        let score: number;

        if(delay>0){
            targetKey = this.delayedKey;
            score = runAt;
        } else if(affinity){
            targetKey = `${PREFIX}:ready:${affinity}`;
            score = priority;
        } else {
            targetKey = this.readyKey;
            score = priority;
        }

        const enqueueScript = fs.readFileSync(path.join(__dirname, 'lua', 'enqueue.lua'), 'utf8');

        const hashFields = [
            'id', jobId,
            'queue', this.name,
            'payload', JSON.stringify(payload),
            'priority', String(priority),
            'affinity', affinity ?? '',
            'status', delay > 0 ? 'delayed' : 'ready',
            'createdAt', String(now),
            'attempts', '0',
            'maxRetries', String(options.maxRetries ?? this.maxRetries),
        ];

        try {
            await this.redis.eval(
                enqueueScript,
                2,
                targetKey,
                `job:${jobId}`,
                String(this.maxQueueSize),
                String(score),
                jobId,
                ...hashFields
            );
        } catch (err: any) {
            if (err.message && err.message.includes('QUEUE_FULL')) {
                throw new Error('Queue is full');
            }
            throw err;
        }

    return jobId;
    }

    

    async  claim():Promise<Record<string,string>|null>{
    const claimScript = fs.readFileSync(path.join(__dirname, 'lua', 'claim.lua'), 'utf8');
    
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

    async complete(jobId: string): Promise<void> {
        const multi = this.redis.multi();
        multi.zrem(this.processingKey, jobId);
        multi.del(`job:${jobId}`);
        const results = await multi.exec();
        if (!results) {
            throw new Error(`Failed to complete job ${jobId}: transaction aborted`);
        }
        for (const [err] of results) {
            if (err) throw err;
        }
    }

    async fail(job: any): Promise<void> {
        const attempts = parseInt(job.attempts || '0', 10) + 1;
        const maxRetries = parseInt(job.maxRetries || String(this.maxRetries), 10);

        const multi = this.redis.multi();
        multi.zrem(this.processingKey, job.id);

        if (attempts >= maxRetries) {
            // Send to DLQ — no more retries
            multi.zadd(this.failedKey, Date.now(), job.id);
            multi.hset(`job:${job.id}`, 'status', 'dead', 'attempts', String(attempts));
        } else {
            // Requeue with backoff delay
            const delay = backoff(attempts);
            multi.zadd(this.delayedKey, Date.now() + delay, job.id);
            multi.hset(`job:${job.id}`, 'status', 'delayed', 'attempts', String(attempts));
        }

        const results = await multi.exec();
        if (!results) {
            throw new Error(`Failed to nack job ${job.id}: transaction aborted`);
        }
        for (const [err] of results) {
            if (err) throw err;
        }
    }

}
        


export {Queue};
