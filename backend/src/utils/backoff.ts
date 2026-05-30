export function backoff(attempts:number):number{
    const base=1000;
    const cap=30000;
    const delay=Math.min(cap,base*Math.pow(2,attempts));
     const jitter = Math.random() * 1000;
     return Math.floor(delay + jitter);
}
