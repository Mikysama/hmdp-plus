"""Repeatable client-side admission pressure test, isolated socket Redis, no business data.
Not an HTTP/MySQL/Kafka capacity benchmark. Admission limits intentionally lifted
so stock screening is measured independently of deployment throttles.
"""
import concurrent.futures, datetime, json, pathlib, statistics, time
from test_protocol import ProtocolTest, Redis, LUA

def percentile(values, p):
    return sorted(values)[min(len(values)-1,int((len(values)-1)*p))]

def run(workers, attempts=100000):
    case=ProtocolTest(); case.setUp(); case.prepare(stock=100)
    sha=case.r.command('SCRIPT','LOAD',(LUA/'reserve.lua').read_text())
    def task(part):
        r=Redis(case.sock); latencies=[]; accepted=0; rejected={}
        try:
            for i in range(part,attempts,workers):
                user=str(i+1000); k=case.keys(user)+[case.prefix+':fence',case.prefix+':admission-outbox']
                r.command('HSET',k[9],'token','t')
                start=time.perf_counter()
                try:
                    raw=r.command('EVALSHA',sha,len(k),*k,1,9,user,'r'+user,'t',user,1000000,1000000,30000)
                    if json.loads(raw)['state']=='HELD': accepted+=1
                except RuntimeError as e:
                    message=str(e); rejected[message]=rejected.get(message,0)+1
                latencies.append((time.perf_counter()-start)*1000)
        finally: r.close()
        return accepted,rejected,latencies
    start=time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        results=list(pool.map(task,range(workers)))
    elapsed=time.perf_counter()-start
    accepted=sum(x[0] for x in results); latencies=[v for x in results for v in x[2]]; rejected={}
    for _,errors,_ in results:
        for key,value in errors.items(): rejected[key]=rejected.get(key,0)+value
    stock=int(case.r.command('GET',case.keys()[2]))
    assert accepted==100 and stock==0, (accepted,stock)
    assert case.r.command('XLEN',case.prefix+':admission-outbox')==accepted
    assert rejected=={'ERR SOLD_OUT':attempts-100}, rejected
    return dict(concurrency=workers,attempts=attempts,stock_initial=100,accepted=accepted,stock_final=stock,
                rejection_counts=rejected,elapsed_seconds=round(elapsed,3),client_requests_per_second=round(attempts/elapsed,1),
                client_lua_latency_ms={name:round(percentile(latencies,p),3) for name,p in [('p50',.5),('p95',.95),('p99',.99)]},
                redis_cpu=case.r.command('INFO','CPU'),redis_commandstats=case.r.command('INFO','COMMANDSTATS'))

if __name__=='__main__':
    ProtocolTest.setUpClass()
    try:
        output=dict(recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    scope='Isolated UNIX socket Redis only; Python client overhead included; no MySQL/Kafka/HTTP capacity claims',
                    total_attempts=400000,runs=[run(n) for n in (1,8,32,64)])
        path=pathlib.Path(__file__).with_name('benchmark-result.json')
        path.write_text(json.dumps(output,ensure_ascii=False,indent=2)+'\n')
        print(json.dumps({**output,'runs':[{k:v for k,v in r.items() if not k.startswith('redis_')} for r in output['runs']]},indent=2))
    finally: ProtocolTest.tearDownClass()
