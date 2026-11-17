#!/usr/bin/env python3
"""Bounded production walkthroughs using only the public HTTP service."""
import argparse, concurrent.futures, json, os, pathlib, shutil, socket, subprocess, tempfile, time, urllib.request, urllib.error
WORKFLOWS=['catalog-create','txn-write','snapshot-read','index-build','query-plan','sql-execute','txn-conflict','restart','checkpoint','backup-restore','changes','vacuum','transaction-close','statistics','savepoint-rollback']
class Session:
 def __init__(self,binary,root,restore=None):
  self.binary=str(binary);self.root=pathlib.Path(root);self.restore=restore;self.process=None;self.log=None
 def start(self):
  with socket.socket() as sock:
   sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
  self.url=f'http://127.0.0.1:{port}/v1/lamina'
  args=[self.binary,'-data',str(self.root),'-listen',f'127.0.0.1:{port}']
  if self.restore:args+=['-restore',str(self.restore)];self.restore=None
  self.log=tempfile.TemporaryFile();self.process=subprocess.Popen(args,stdout=self.log,stderr=self.log)
  deadline=time.monotonic()+8
  while time.monotonic()<deadline:
   if self.process.poll() is not None:
    self.log.seek(0);raise RuntimeError(self.log.read().decode())
   try:self.call('GET','/health');return self
   except (OSError,urllib.error.URLError):time.sleep(.03)
  raise RuntimeError('service did not become ready')
 def stop(self):
  if self.process and self.process.poll() is None:
   self.process.terminate()
   try:self.process.wait(timeout=12)
   except subprocess.TimeoutExpired:self.process.kill();self.process.wait()
  if self.log:self.log.close();self.log=None
 def call(self,method,path,data=None,status=200,raw=False):
  encoded=None if data is None else json.dumps(data,ensure_ascii=False).encode()
  req=urllib.request.Request(self.url+path,data=encoded,method=method,headers={'Content-Type':'application/json'})
  try:
   with urllib.request.urlopen(req,timeout=12) as response:code=response.status;body=response.read()
  except urllib.error.HTTPError as e:code=e.code;body=e.read()
  if code!=status:raise RuntimeError(f'{method} {path}: expected {status}, got {code}: {body.decode()}')
  return body if raw else json.loads(body)
 def begin(self,readonly=False):return self.call('POST','/transactions',{'read_only':readonly},201)['id']
 def put(self,tx,row,status=200,insert=False):return self.call('POST',f'/transactions/{tx}/put',{'relation':'spectra','row':row,'insert_only':insert},status)
 def commit(self,tx,status=200):return self.call('POST',f'/transactions/{tx}/commit',status=status)
 def savepoint(self,tx,name,status=201):return self.call('POST',f'/transactions/{tx}/savepoints',{'name':name},status)
 def rollback_to(self,tx,name,status=200):return self.call('POST',f'/transactions/{tx}/savepoints/rollback',{'name':name},status)
 def release_sp(self,tx,name,status=200):return self.call('POST',f'/transactions/{tx}/savepoints/release',{'name':name},status)
 def savepoints(self,tx,status=200):return self.call('GET',f'/transactions/{tx}/savepoints',status=status)
 def delete(self,tx,key,status=200):return self.call('POST',f'/transactions/{tx}/delete',{'relation':'spectra','key':key},status)
 def query(self,**kwargs):return self.call('POST','/query',{'relation':'spectra',**kwargs})
 def stats(self):return self.call('GET','/stats')
def require(condition,message):
 if not condition:raise RuntimeError(message)
def tuple_(n,band=None,amplitude=None):return {'sample':n,'band':band or f'b{n}','amplitude':n*10 if amplitude is None else amplitude,'valid':True}
def seed(s):
 schema={'name':'spectra','primary':'sample','columns':[{'name':'sample','type':'int'},{'name':'band','type':'text'},{'name':'amplitude','type':'int','nullable':True},{'name':'valid','type':'bool'}]}
 s.call('POST','/relations',schema,201)
 tx=s.begin()
 for n in range(1,7):s.put(tx,tuple_(n))
 s.put(tx,{'sample':7,'band':'b7','amplitude':None,'valid':False})
 return s.commit(tx)['sequence']
def walkthrough(name,binary):
 with tempfile.TemporaryDirectory(prefix='lamina-flow-') as temp:
  temp=pathlib.Path(temp);s=Session(binary,temp/'source').start();other=None
  try:
   seed_seq=seed(s);observations={}
   if name=='catalog-create':
    before=s.stats()['sequence'];bad={'name':'invalid','primary':'missing','columns':[{'name':'value','type':'int'}]}
    s.call('POST','/relations',bad,400);require(s.stats()['sequence']==before,'invalid schema changed sequence')
    cat=s.call('GET','/catalog');require(len(cat['relations'])==1,'catalog mismatch');observations=cat
   elif name=='txn-write':
    tx=s.begin();s.put(tx,tuple_(8));s.put(tx,tuple_(9));require(s.query()['matched']==7,'uncommitted tuples leaked')
    result=s.commit(tx);require(result['writes']==2 and s.query()['matched']==9,'atomic commit failed')
    bad=s.begin();s.put(bad,{'sample':10,'band':'x','amplitude':'bad','valid':True},400)
    s.call('POST',f'/transactions/{bad}/rollback');observations=result
   elif name=='snapshot-read':
    old=s.begin();writer=s.begin();s.put(writer,tuple_(1,amplitude=555));s.commit(writer)
    q={'where':[{'column':'sample','op':'=','value':1}]}
    require(s.query(transaction=old,**q)['rows'][0]['amplitude']==10,'repeatable read violated')
    require(s.query(**q)['rows'][0]['amplitude']==555,'current snapshot stale')
    s.put(old,tuple_(8));require(s.query(transaction=old)['matched']==8,'read-own-write scan missing')
    require(s.query(at=seed_seq)['rows'][0]['amplitude']==10,'time travel failed')
    s.call('POST',f'/transactions/{old}/rollback');s.call('POST','/query',{'relation':'spectra','at':999},410);observations={'old':10,'current':555}
   elif name=='index-build':
    definition={'name':'by_band','relation':'spectra','column':'band','unique':True}
    s.call('POST','/indexes',definition,201)
    a=s.begin();b=s.begin();s.put(a,tuple_(8,band='shared'));s.put(b,tuple_(9,band='shared'));s.commit(a);s.commit(b,409)
    swap=s.begin();s.put(swap,tuple_(1,band='b2'));s.put(swap,tuple_(2,band='b1'));s.commit(swap)
    duplicate=s.begin();s.put(duplicate,tuple_(10,band='b3'));s.commit(duplicate,409)
    observations=s.call('GET','/catalog')
   elif name=='query-plan':
    s.call('POST','/indexes',{'name':'by_amplitude','relation':'spectra','column':'amplitude'},201)
    at=s.stats()['sequence'];tx=s.begin();s.put(tx,tuple_(1,amplitude=999));s.commit(tx)
    q={'where':[{'column':'amplitude','op':'<=','value':20}]}
    fast=s.query(**q);slow=s.query(force_scan=True,**q)
    require(fast['rows']==slow['rows'] and fast['plan']['access']=='index','index/scan mismatch')
    historical=s.query(at=at,**q);require(len(historical['rows'])==2,'historical index lost prior value')
    observations={'indexed':fast,'historical':historical}
   elif name=='sql-execute':
    sql=s.call('POST','/sql',{'sql':'SELECT sample, amplitude FROM spectra WHERE amplitude >= 30 AND valid = true ORDER BY amplitude DESC LIMIT 2;'})
    structured=s.query(columns=['sample','amplitude'],where=[{'column':'amplitude','op':'>=','value':30},{'column':'valid','op':'=','value':True}],order=[{'column':'amplitude','desc':True}],limit=2)
    require(sql['rows']==structured['rows'],'SQL and structured query diverged')
    count=s.call('POST','/sql',{'sql':'SELECT count(amplitude) FROM spectra;'})
    total=s.call('POST','/sql',{'sql':'SELECT sum(amplitude) FROM spectra;'})
    require(count['rows']==[{'count':6}] and total['rows']==[{'sum':210}],'aggregate semantics wrong')
    s.call('POST','/sql',{'sql':'SELECT * FROM spectra; SELECT * FROM spectra;'},400);observations={'projection':sql['rows'],'count':count['rows'],'sum':total['rows']}
   elif name=='txn-conflict':
    a=s.begin();b=s.begin();s.put(a,tuple_(1,amplitude=101));s.put(b,tuple_(1,amplitude=202));s.put(b,tuple_(8))
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
     jobs=[pool.submit(s.commit,a),pool.submit(s.commit,b)]
     results=[]
     for job in jobs:
      try:results.append({'commit':job.result()})
      except RuntimeError as e:
       require('got 409' in str(e),'unexpected concurrent failure');results.append({'conflict':True})
    require(sum('conflict' in r for r in results)==1,'exactly one writer must lose')
    rows=s.query()['rows'];one=next(r for r in rows if r['sample']==1)
    require((one['amplitude']==202)==any(r['sample']==8 for r in rows),'conflicting commit partially published')
    s.commit(a,404);s.commit(b,404);observations={'results':results,'rows':rows}
   elif name=='restart':
    before=s.query()['rows'];s.stop()
    with (s.root/'journal.lmw').open('ab') as f:f.write(b'\x00\x00\x00')
    s.start();require(s.query()['rows']==before,'tail recovery changed committed data');require(s.stats()['recovered_tail_bytes']==3,'tail recovery not reported')
    s.stop();broken=temp/'broken';shutil.copytree(s.root,broken)
    data=bytearray((broken/'journal.lmw').read_bytes());data[-1]^=1;(broken/'journal.lmw').write_bytes(data)
    result=subprocess.run([str(binary),'-data',str(broken),'-listen','127.0.0.1:0'],capture_output=True,text=True,timeout=5)
    require(result.returncode!=0 and 'checksum' in result.stderr,'full frame corruption accepted')
    observations={'tail_bytes':3,'corruption':'rejected'}
   elif name=='checkpoint':
    at=s.call('POST','/checkpoint')['sequence'];tx=s.begin();s.put(tx,tuple_(8));s.commit(tx)
    before=s.query()['rows'];s.stop();s.start();require(s.query()['rows']==before,'checkpoint plus suffix recovery failed')
    locked=subprocess.run([str(binary),'-data',str(s.root),'-listen','127.0.0.1:0'],capture_output=True,text=True,timeout=5)
    require(locked.returncode!=0 and 'already open' in locked.stderr,'exclusive directory ownership missing');observations={'checkpoint':at,'head':s.stats()['sequence']}
   elif name=='backup-restore':
    backup=s.call('GET','/backup',raw=True);path=temp/'portable.lmi';path.write_bytes(backup)
    frozen=s.query()['rows'];tx=s.begin();s.put(tx,tuple_(8));s.commit(tx)
    other=Session(binary,temp/'restored',path).start();require(other.query()['rows']==frozen,'backup mutated with source')
    tx=other.begin();other.put(tx,tuple_(9));result=other.commit(tx);other.stop();other.start()
    require(other.query()['matched']==8,'restored journal base does not continue')
    other.call('GET','/changes?after=0',status=410)
    broken=json.loads(backup);broken['sha256']='0'*64;bad=temp/'bad.lmi';bad.write_text(json.dumps(broken))
    failed=subprocess.run([str(binary),'-restore',str(bad),'-data',str(temp/'badrestore')],capture_output=True,text=True,timeout=5)
    require(failed.returncode!=0 and not (temp/'badrestore').exists(),'invalid backup published');observations={'restored_commit':result,'original_sequence':seed_seq}
   elif name=='changes':
    tx=s.begin()
    for n in range(8,12):s.put(tx,tuple_(n))
    committed=s.commit(tx);page=s.call('GET',f'/changes?after={seed_seq}&limit=1')
    require(len(page['records'])==1 and len(page['records'][0]['writes'])==4 and page['cursor']==committed['sequence'],'change page split transaction')
    s.call('GET','/changes?after=999',status=400);observations=page
   elif name=='vacuum':
    old=s.begin();tx=s.begin();s.put(tx,tuple_(1,amplitude=99));s.commit(tx);requested=s.stats()['sequence']
    pinned=s.call('POST','/vacuum',{'horizon':requested});require(pinned['pinned'] and pinned['horizon']==seed_seq,'vacuum crossed active snapshot')
    require(s.query(transaction=old)['rows'][0]['amplitude']==10,'active snapshot lost')
    s.call('POST',f'/transactions/{old}/rollback');reclaimed=s.call('POST','/vacuum',{'horizon':requested})
    require(reclaimed['removed']>=1,'obsolete version not reclaimed');s.call('POST','/query',{'relation':'spectra','at':seed_seq},410)
    s.call('POST','/checkpoint');s.stop();s.start();require(s.query()['rows'][0]['amplitude']==99,'vacuum did not survive restart');observations={'pinned':pinned,'reclaimed':reclaimed}
   elif name=='transaction-close':
    tx=s.begin();s.put(tx,tuple_(8));s.call('POST',f'/transactions/{tx}/rollback');s.commit(tx,404)
    require(s.query()['matched']==7 and not s.stats()['active'],'rollback leaked state or pin')
    tx=s.begin(True);s.put(tx,tuple_(9),409);s.call('POST',f'/transactions/{tx}/rollback');observations={'rollback':'discarded','read_only':'enforced'}
   elif name=='statistics':
    s.call('POST','/indexes',{'name':'by_amp','relation':'spectra','column':'amplitude'},201)
    q=s.query(where=[{'column':'amplitude','op':'>=','value':40}],limit=2)
    require(q['matched']==3 and q['scanned']==3 and len(q['rows'])==2 and q['truncated'],'query fact meanings conflated')
    observations={'query':q,'stats':s.stats()}
   elif name=='savepoint-rollback':
    def amp(sample):
     rows=s.query(transaction=tx,where=[{'column':'sample','op':'=','value':sample}])['rows']
     return None if not rows else rows[0]['amplitude']
    tx=s.begin()
    s.put(tx,tuple_(8,amplitude=80))
    first=s.savepoint(tx,'sp1');require(first['depth']==1 and first['staged_keys']==1 and first['memory']['limit_bytes']==16*1024*1024,'savepoint admission facts wrong')
    s.put(tx,tuple_(9,amplitude=90));s.put(tx,tuple_(8,amplitude=81));s.delete(tx,1)
    require(amp(8)==81 and amp(1) is None,'private write overlay did not reflect staging')
    restored=s.rollback_to(tx,'sp1')
    require(restored['restored_keys']==3 and amp(8)==80 and amp(9) is None and amp(1)==10,'rollback to savepoint did not restore the surviving write set')
    require(s.query(transaction=tx)['matched']==8,'committed rows plus one staged row expected after partial rollback')
    require(s.query()['matched']==7,'rolled-back private writes leaked outside the transaction')
    # delete then rewrite inside a savepoint must restore cleanly, including the tombstone path
    s.delete(tx,8);require(amp(8) is None,'staged tombstone not visible inside transaction')
    s.put(tx,tuple_(8,amplitude=82));s.put(tx,tuple_(8,amplitude=83))
    s.rollback_to(tx,'sp1');require(amp(8)==80,'delete-and-rewrite sequence did not restore to savepoint value')
    # same-name savepoints are rejected while active; release frees the name and keeps earlier history
    s.savepoint(tx,'sp2')
    s.savepoint(tx,'sp2',409);s.savepoint(tx,'bad name',400);s.savepoint(tx,'bad-name',400)
    s.release_sp(tx,'sp2');recreated=s.savepoint(tx,'sp2');require(recreated['depth']==3,'released savepoint name reused with a fresh depth')
    s.put(tx,tuple_(9,amplitude=91));s.rollback_to(tx,'sp1')
    require([p['name'] for p in s.savepoints(tx)['savepoints']]==['sp1'],'nested rollback left inner savepoints behind')
    require(amp(9) is None and amp(8)==80,'nested rollback through a released interval failed')
    # unique value swap inside a savepoint, then rollback, then a different swap commits
    s.call('POST','/indexes',{'name':'uniq_band','relation':'spectra','column':'band','unique':True},201)
    s.savepoint(tx,'swap');s.put(tx,tuple_(1,band='b2'));s.put(tx,tuple_(2,band='b1'))
    s.rollback_to(tx,'swap');require(amp(1)==10 and amp(2)==20,'unique-swap rollback did not return prior rows')
    s.put(tx,tuple_(1,band='b2'));s.put(tx,tuple_(2,band='b1'))
    committed=s.commit(tx);require(committed['writes']==3,'commit must publish exactly the surviving write set')
    rows=s.query()['rows'];require(next(r for r in rows if r['sample']==1)['band']=='b2','surviving unique swap not published')
    # depth limit: the 64th active savepoint is admitted, the 65th refused without state change
    guard=s.begin()
    for n in range(1,65):s.savepoint(guard,f'sp{n}')
    s.savepoint(guard,'sp65',429);require(len(s.savepoints(guard)['savepoints'])==64,'depth limit did not hold at 64')
    s.rollback_to(guard,'sp1');require(len(s.savepoints(guard)['savepoints'])==1,'rollback did not release deeper savepoint depth')
    s.call('POST',f'/transactions/{guard}/rollback')
    # memory budget: rejection reports the budget and cannot silently discard retained work
    budget=s.begin();s.savepoint(budget,'hold')
    large_band='x'*30000;rejected=None
    for n in range(20,620):
     try:s.put(budget,tuple_(n,band=large_band))
     except RuntimeError as e:
      require('got 429' in str(e),'memory pressure must surface as capacity 429');rejected=str(e);break
    require(rejected is not None and '16777216' in rejected,'budget rejection must state the byte limit')
    usage=s.savepoints(budget)['memory'];require(usage['used_bytes']>0 and usage['used_bytes']<=usage['limit_bytes'],'usage accounting overran the limit')
    recovered=s.rollback_to(budget,'hold');require(recovered['staged_keys']==0 and recovered['memory']['used_bytes']==recovered['memory']['undo_bytes'],'rollback did not release staged bytes')
    s.put(budget,tuple_(20,amplitude=200));require(s.query(transaction=budget,where=[{'column':'sample','op':'=','value':20}])['rows'][0]['amplitude']==200,'retained savepoint unusable after budget pressure')
    s.call('POST',f'/transactions/{budget}/rollback')
    # read-only transactions cannot hold savepoints; closed transactions reject every savepoint route
    reader=s.begin(True);s.savepoint(reader,'x',409);s.call('POST',f'/transactions/{reader}/rollback')
    closed=s.begin();s.savepoint(closed,'gone');s.call('POST',f'/transactions/{closed}/rollback')
    s.savepoint(closed,'gone',404);s.rollback_to(closed,'gone',404);s.release_sp(closed,'gone',404);s.savepoints(closed,404)
    require(not any(t['id']==closed for t in s.stats()['active']),'closed transaction still pinned in stats')
    # conflict loser with savepoints closes atomically and publishes nothing
    a=s.begin();b=s.begin();s.savepoint(a,'pa');s.savepoint(b,'pb')
    s.put(a,tuple_(3,amplitude=301));s.put(b,tuple_(3,amplitude=302))
    s.commit(a);s.commit(b,409);s.commit(b,404)
    require(s.query(where=[{'column':'sample','op':'=','value':3}])['rows'][0]['amplitude']==301,'conflicting savepoint transaction partially published')
    observations={'first':first,'restored':restored,'recreated':recreated,'committed':committed,'budget_error':rejected,'recovered':recovered}
   else:raise ValueError(name)
   return {'workflow':name,'status':'observed','observations':observations}
  finally:
   s.stop()
   if other:other.stop()
def main():
 parser=argparse.ArgumentParser();parser.add_argument('--workflow',choices=['all',*WORKFLOWS],default='all');args=parser.parse_args()
 project=pathlib.Path(__file__).resolve().parents[1];binary=project/'bin/laminad'
 if not binary.exists():raise SystemExit('build first: go build -o bin/laminad ./cmd/laminad')
 names=WORKFLOWS if args.workflow=='all' else [args.workflow]
 for name in names:print(json.dumps(walkthrough(name,binary),ensure_ascii=False),flush=True)
if __name__=='__main__':main()
