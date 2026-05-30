import fs from 'fs'
import Redis from 'ioredis';
const redis=new Redis();
async function main(){
    await testseed();
    const result=fs.readFileSync('/home/vishal/Desktop/HomebrewMQ/backend/src/lua/claim.lua','utf8');
    const ans=await redis.eval(result, 3, 
  '{homebrewmq}:readyQueue',
  '{homebrewmq}:processingQueue', 
  '{homebrewmq}:failedQueue',
  '30000'
);
    console.log(ans);
    redis.disconnect();
}

main();

async function testseed(){
    // seed a test job
await redis.hset('job:job1', {
  attempts: '0',
  maxRetries: '3',
  payload: '{"task":"hello"}',
  status: 'pending'
});
await redis.zadd('{homebrewmq}:readyQueue', 1, 'job1')
}
//testseed();