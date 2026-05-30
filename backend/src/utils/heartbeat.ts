import Redis from "ioredis";


class HeartBeat{

    private redis:Redis
    private workerId:string
    private ttl:number
    private working:boolean
    private intervalId:NodeJS.Timeout|null
    constructor(redisClient:Redis,workerId:string,ttl=30){
        this.redis=redisClient;
        this.workerId=workerId;
        this.ttl=ttl
        this.working=false
        this.intervalId=null
    }
    async  pulse() {
        await this.redis.setex(`worker:heartbeat:${this.workerId}`,this.ttl,'alive')
    }
     start(){
        this.working=true;
        this.intervalId=setInterval(()=>{
            void this.pulse()
        },10000)
    }
     stop(){
        this.working=false;
        if(this.intervalId){
            clearInterval(this.intervalId);
            this.intervalId=null;
        }
    }

}

export {HeartBeat};
