import Redis from "ioredis";
import { Queue } from "./queue";

interface Job {
    id: string;
    [key: string]: string;
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
            await this.process(job as Job);
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
            await this.queue.fail(job.id);
        }
    }
}

export {Worker};
