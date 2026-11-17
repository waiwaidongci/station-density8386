"""HTTP 端到端：真实 FastAPI 路由 + SQLite + fakeredis，验证对外行为。"""
import os,sys,tempfile,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
ART=Path(tempfile.mkdtemp(prefix='http-art-'))
os.environ['DATABASE_URL']='sqlite:///'+str(Path(tempfile.mkdtemp())/'http.db')
os.environ['REDIS_URL']='redis://localhost:6379/0'
os.environ['ARTIFACT_DIR']=str(ART)
os.environ['SEED_DEMO']='false'

import fakeredis
from fastapi.testclient import TestClient
from sqlalchemy import select
from src.config.database import Base,engine,Session
from src.config.migrate import run_migrations
from src.models.user import User
from src.models.source import Source
from src.models.record import Record
from src.services import dispatch
import src.services.runs as runs_svc
from src.services.domain_validation import validate_record
from src.utils.security import hash_password

fake=fakeredis.FakeStrictRedis()
dispatch.queue=fake
runs_svc.queue=fake

Base.metadata.create_all(engine);run_migrations()
with Session() as db:
    db.add(User(email='analyst@x',password_hash=hash_password('pw123456'),role='analyst'))
    db.add(User(email='admin@x',password_hash=hash_password('pw123456'),role='admin'))
    db.add(User(email='viewer@x',password_hash=hash_password('pw123456'),role='viewer'))
    s=Source(key='K1',name='来源1',filename='k1.csv',description='d');db.add(s);db.flush()
    import random;rng=random.Random(1)
    for i in range(30):
        db.add(Record(source_id=s.id,reference=f'K1-{i:04}',values=validate_record(
            {'temperature':round(20+rng.gauss(0,4),3),'humidity':round(50+rng.gauss(0,12),3),
             'particles':round(40+rng.gauss(0,18),3)})))
    db.commit();sid=s.id

from src.main import app
PASS=0;FAIL=0
def check(n,c,d=''):
    global PASS,FAIL
    if c:PASS+=1;print('  ✓',n)
    else:FAIL+=1;print('  ✗',n,'-',d)

with TestClient(app) as client:
    print('\n[A] 认证与权限')
    r=client.post('/auth/login',json={'email':'analyst@x','password':'pw123456'})
    check('分析师登录 200',r.status_code==200,str(r.status_code))
    tok=r.json()['token'];H={'Authorization':'Bearer '+tok,'Content-Type':'application/json'}
    rv=client.post('/auth/login',json={'email':'viewer@x','password':'pw123456'}).json()
    HV={'Authorization':'Bearer '+rv['token']}
    check('匿名建任务 401',client.post('/runs',json={}).status_code==401)
    check('viewer 建任务 403',client.post('/runs',headers=HV,json={}).status_code==403)

    print('\n[B] 幂等建任务：重试不新增研究')
    payload={'source_id':sid,'name':'HTTP 研究','parameters':{'eps':0.55,'min_samples':5}}
    r1=client.post('/runs',headers={**H,'Idempotency-Key':'idem-001'},json=payload)
    r2=client.post('/runs',headers={**H,'Idempotency-Key':'idem-001'},json={**payload,'name':'改名重试'})
    check('两次请求均成功',r1.status_code in(200,201) and r2.status_code in(200,201),f'{r1.status_code},{r2.status_code}')
    check('返回同一研究 id',r1.json()['id']==r2.json()['id'])
    rid=r1.json()['id']
    runs=client.get('/runs',headers=H).json()
    check('列表只有 1 个研究',len(runs)==1,str(len(runs)))
    check('列表含可靠性新字段',all(k in runs[0] for k in('takeovers','attempt_id','enqueued_at','state')))
    check('新建任务初始接管次数 0',runs[0]['takeovers']==0)

    print('\n[C] 未核对前不可采用')
    check('未完成导出 409',client.get(f'/runs/{rid}/export',headers=H).status_code==409)
    nr=client.post('/notes',headers=H,json={'run_id':rid,'title':'T'*5,'body':'B'*10})
    check('未核对不能建复访计划 409',nr.status_code==409,str(nr.status_code))

    print('\n[D] worker 真实执行并核对')
    # 取出队列消息执行（模拟独立 worker 进程）
    msg=fake.lpop('analysis:queue')
    check('任务确实进入 Redis 队列',msg is not None and int(msg)==rid)
    dispatch.execute_run(rid,'test-worker')
    detail=client.get(f'/runs/{rid}',headers=H).json()
    check('最终状态 completed',detail['state']=='completed',detail.get('state'))
    check('带核对时间与双校验和',detail['verified_at'] and detail['result_checksum'] and detail['artifact_checksum'])
    check('结果含完整行',len(detail['result']['rows'])==30)

    print('\n[E] 核对一致后开放采用')
    er=client.get(f'/runs/{rid}/export',headers=H)
    check('导出 200',er.status_code==200,str(er.status_code))
    check('导出为 CSV 附件','text/csv' in er.headers.get('content-type',''))
    import csv,io
    rows=list(csv.DictReader(io.StringIO(er.content.decode('utf-8-sig'))))
    check('导出 30 行与结果一致',len(rows)==30)

    print('\n[F] 复访计划批准后快照独立可读')
    n=client.post('/notes',headers=H,json={'run_id':rid,'title':'十一月复核意见','body':'已核对，可用于复访安排。'})
    check('分析师建草案 200',n.status_code==200,str(n.status_code))
    nid=n.json()['id']
    check('分析师批准 403',client.post(f'/notes/{nid}/approve',headers=H).status_code==403)
    check('负责人批准 200',client.post(f'/notes/{nid}/approve',headers={'Authorization':'Bearer '+client.post('/auth/login',json={'email':'admin@x','password':'pw123456'}).json()['token']}).status_code==200)
    snap=client.get(f'/notes/{nid}/export',headers=HV)
    check('viewer 可下载批准快照 200',snap.status_code==200,str(snap.status_code))
    check('快照含原始结果行',b'K1-0000' in snap.content)

    print('\n[G] 队列短断：任务保持 queued 等待补送')
    # 临时让 redis 故障建任务
    import unittest.mock as mock
    dead=fakeredis.FakeStrictRedis();dead.rpush=mock.Mock(side_effect=ConnectionError('down'))
    with mock.patch.object(dispatch,'queue',dead),mock.patch.object(runs_svc,'queue',dead):
        rr=client.post('/runs',headers={**H,'Idempotency-Key':'idem-002'},json=payload)
    check('队列故障下建任务仍 201',rr.status_code in(200,201),str(rr.status_code))
    rid2=rr.json()['id']
    check('故障任务保持 queued',client.get(f'/runs/{rid2}',headers=H).json()['state']=='queued')
    # 恢复后对账补送
    # 制造宽限超时
    from datetime import datetime,timedelta,timezone
    from src.models.run import Run
    with Session() as db:
        r=db.get(Run,rid2);r.enqueued_at=(datetime.now(timezone.utc)-timedelta(seconds=30)).isoformat();db.commit()
    out=dispatch.reconcile_once()
    check('对账补送漏单',any(x[1]==rid2 for x in out),str(out))
    check('补送后队列有消息',int(fake.lpop('analysis:queue'))==rid2)

print(f'\nHTTP 结果：{PASS} 通过 / {FAIL} 失败')
sys.exit(1 if FAIL else 0)
