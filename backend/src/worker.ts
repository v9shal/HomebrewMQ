import Redis from "ioredis";
import { Queue } from "./queue";
import { backoff } from "./utils/backoff";

interface Job {
  id: string;
  queue: string;
  payload: string;
  priority: number;
  attempts: number;
  maxRetries: number;
  status: string;
  affinity: string;
  createdAt: number;
  lastError?: string;
}
class Worker{

    
    private running:boolean=false;
    private processor:(job:Job)=>Promise<void>
    private queue:Queue

    constructor(queue:Queue,processor:(job:Job)=>Promise<void>){
        this.queue=queue;
        this.processor=processor;
        this.running=false;
    }
    async start(){
        this.running =true;
        while(this.running){
            const job=await this.queue.claim();
            if(!job || !job.id){
                 await new Promise(r=>setTimeout(r,500));
                 continue;
            }
            await this.process(job);
        }
    }
    stop(){
        this.running=false;
    }
    async process(job:Job){
        try{
            await this.processor(job);
            await this.queue.complete(job.id);
        }
        catch(err){
            await this.queue.fail(job);
        }
    }
    
}

export {Worker};
