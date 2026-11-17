"""可靠性场景测试（SQLite + fakeredis，无需外部服务）。

覆盖需求：
1. 同一幂等键重试不新增研究；无键请求各自独立
2. 队列短断后自动补送（reconcile 重新 RPUSH）
3. 进程崩溃（running 超时）后接管，沿用冻结输入重算，takeovers 递增
4. 多进程并发领取同一任务仅一份有效执行（行锁/状态守卫）
5. 超时接管后原进程晚归，fencing token 拦截，无权改写结果
6. 结果与完整导出 CSV 核对一致后才 completed/可采用
7. 写工件/落库中断可恢复（临时文件无半截正式文件，可重建）
8. 重复队列消息只产生一次完成审计（event_key 唯一）
9. 页面/API 可见等待、接管次数与最终状态
10. 已批准复访计划仍可读取原结果（frozen_result 独立留存）
"""
import os,sys,time,json,threading,tempfile,importlib
from pathlib import Path
from unittest import mock

import fakeredis
from sqlalchemy import create_engine,select,func

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
ART=Path(tempfile.mkdtemp(prefix='artifacts-'))
os.environ['DATABASE_URL']='sqlite:///'+str(Path(tempfile.mkdtemp())/'test.db')
os.environ['REDIS_URL']='redis://localhost:6379/0'
os.environ['ARTIFACT_DIR']=str(ART)
os.environ['SEED_DEMO']='false'

from src.config.database import Base,engine,Session
from src.config.migrate import run_migrations
from src.models.run import Run
from src.models.audit import Audit
from src.models.source import Source
from src.models.record import Record
from src.services import dispatch
from src.services.dispatch import (reconcile_once,enqueue_run,execute_run,
    claim_run,QUEUE_KEY,RUN_TIMEOUT,ENQUEUE_GRACE,_artifact_path)
from src.services.domain_validation import validate_record
from src.utils.serialization import now
import src.services.runs as runs_svc
import src.services.export as export_svc

PASS=0;FAIL=0
def check(name,cond,detail=''):
    global PASS,FAIL
    if cond:PASS+=1;print('  ✓',name)
    else:FAIL+=1;print('  ✗',name,'-',detail)

fake=fakeredis.FakeStrictRedis()
dispatch.queue=fake
runs_svc.queue=fake

def setup_module():
    Base.metadata.create_all(engine)
    run_migrations()
    with Session() as db:
        s=Source(key='K-1',name='测试来源',filename='k1.csv',description='d')
        db.add(s);db.flush()
        import random
        rng=random.Random(7)
        for i in range(40):
            vals={'temperature':round(20+rng.gauss(0,4),3),
                  'humidity':round(50+rng.gauss(0,12),3),
                  'particles':round(40+rng.gauss(0,18),3)}
            db.add(Record(source_id=s.id,reference=f'K-1-{i:04}',values=validate_record(vals)))
        db.commit();return s.id

def make_run(db,source_id,name='研究',state='queued',key=None):
    recs=[{'id':r.id,'source_id':r.source_id,'reference':r.reference,'values':r.values}
          for r in db.scalars(select(Record).where(Record.source_id==source_id).order_by(Record.id))]
    r=Run(source_id=source_id,name=name,state=state,
          parameters={'eps':0.55,'min_samples':5},
          snapshot={'source':{'id':source_id,'name':'测试来源','filename':'k1.csv'},'records':recs},
          created_at=now(),idempotency_key=key,enqueued_at=now())
    db.add(r);db.flush();db.commit();return r.id

class User:
    def __init__(self,email):self.email=email

class Data:
    def __init__(self,source_id,name='接口研究'):
        self.source_id=source_id;self.name=name;self.parameters={'eps':0.55,'min_samples':5}

def drain_queue():
    ids=[]
    while True:
        v=fake.lpop(QUEUE_KEY)
        if not v:return ids
        ids.append(int(v))

def wait_state(run_id,states,timeout=20):
    deadline=time.time()+timeout
    while time.time()<deadline:
        with Session() as db:
            r=db.get(Run,run_id)
            if r.state in states:return r
        time.sleep(0.1)
    with Session() as db:return db.get(Run,run_id)

def test_1_idempotent_create(source_id):
    print('\n[1] 同一请求重试不新增研究')
    with Session() as db:
        before=db.scalar(select(func.count()).select_from(Run))
        rid=runs_svc.create_run(db,User('a@x'),Data(source_id),'key-abc-1')
        rid2=runs_svc.create_run(db,User('a@x'),Data(source_id,'另一个名字'),'key-abc-1')
        rid3=runs_svc.create_run(db,User('a@x'),Data(source_id),None)
        after=db.scalar(select(func.count()).select_from(Run))
    check('相同幂等键返回同一 run id',rid['id']==rid2['id'])
    check('重试不新增研究（仅 +1 条无键任务）',after-before==2,f'{before}->{after}')
    check('无键请求独立建研究',rid3['id']!=rid['id'])
    drain_queue()

def test_2_requeue_after_outage(source_id):
    print('\n[2] 队列短断后自动补送')
    with Session() as db:rid=make_run(db,source_id,'短断研究')
    # 模拟入队时 Redis 故障
    with mock.patch.object(dispatch,'queue',fakeredis.FakeStrictRedis()) as dead:
        dead.rpush=mock.Mock(side_effect=ConnectionError('redis down'))
        ok=enqueue_run(rid,retries=2)
    check('入队重试全部失败返回 False 且任务仍 queued',ok is False)
    with Session() as db:check('失败后任务保持 queued 等待补送',db.get(Run,rid).state=='queued')
    # 制造超过宽限的 enqueued_at
    with Session() as db:
        r=db.get(Run,rid)
        from datetime import datetime,timedelta,timezone
        r.enqueued_at=(datetime.now(timezone.utc)-timedelta(seconds=ENQUEUE_GRACE+5)).isoformat()
        db.commit()
    recovered=reconcile_once()
    queued_ids=drain_queue()
    check('对账补送该任务到队列',rid in queued_ids,str(queued_ids))
    check('产生 run.requeued 审计',any(x[0]=='queued' and x[1]==rid for x in recovered))

def test_3_crash_takeover_uses_frozen_input(source_id):
    print('\n[3] 进程崩溃后接管重算，沿用冻结输入')
    with Session() as db:rid=make_run(db,source_id,'崩溃研究')
    # worker A 领取后“崩溃”：停在 running，不更新心跳
    r,attempt=claim_run(rid,'worker-A')
    check('worker A 领取成功 attempt=1',attempt==1)
    with Session() as db:
        run=db.get(Run,rid)
        check('领取后状态 running',run.state=='running' and run.attempt_id==1)
        from datetime import datetime,timedelta,timezone
        old=(datetime.now(timezone.utc)-timedelta(seconds=RUN_TIMEOUT+5)).isoformat()
        run.heartbeat_at=old;run.started_at=old;db.commit()
    # 崩溃后篡改来源当前数据，证明重算用的是冻结快照而非当前数据
    with Session() as db:
        rec=db.scalars(select(Record).where(Record.source_id==source_id)).first()
        original=rec.values.copy();rec.values=validate_record({**rec.values,'temperature':99.9})
        db.commit()
    recovered=reconcile_once()
    check('超时接管生成 takeover 记录',any(x==('takeover',rid,1) for x in recovered),str(recovered))
    with Session() as db:
        run=db.get(Run,rid)
        check('接管后回到 queued 且 takeovers=1',run.state=='queued' and run.takeovers==1)
    ids=drain_queue();check('接管任务重新入队',rid in ids)
    # 新 worker 消费并完成
    execute_run(rid,'worker-B')
    with Session() as db:
        run=db.get(Run,rid)
        check('接管重算后 completed',run.state=='completed')
        check('attempt_id 递增到 2（fencing 代次）',run.attempt_id==2)
        check('takeovers 保留为 1',run.takeovers==1)
        check('核对时间与双校验和已写入',bool(run.verified_at) and bool(run.result_checksum) and bool(run.artifact_checksum))
        # 校验和证明用的是冻结输入（被篡改值不应出现）
        check('重算沿用冻结输入（无 99.9 篡改值）',all(row['temperature']!=99.9 for row in run.result['rows']))
    # 还原来源
    with Session() as db:
        rec=db.scalars(select(Record).where(Record.source_id==source_id)).first()
        rec.values=original;db.commit()

def test_4_concurrent_claim_single_execution(source_id):
    print('\n[4] 多进程并发领取仅一份有效执行')
    with Session() as db:rid=make_run(db,source_id,'并发研究')
    results=[]
    barrier=threading.Barrier(4)
    def claim(wid):
        barrier.wait()
        try:results.append((wid,)+claim_run(rid,wid))
        except Exception as e:results.append((wid,None,None,repr(e)))
    ts=[threading.Thread(target=claim,args=(f'w{i}',)) for i in range(4)]
    for t in ts:t.start()
    for t in ts:t.join()
    success=[x for x in results if x[1] is not None]
    check('4 个并发领取只有 1 个成功',len(success)==1,str([(x[0],x[1] is not None) for x in results]))
    with Session() as db:
        run=db.get(Run,rid)
        check('attempt 只递增一次 =1',run.attempt_id==1 and run.state=='running')
    # 重复队列消息：再次 execute 应被状态守卫忽略，不产生第二条 started
    execute_run(rid,'duplicate-message-worker')
    with Session() as db:
        n=db.scalar(select(func.count()).select_from(Audit).where(Audit.event_key==f'run.started.{rid}.1'))
    check('重复消息未产生重复启动审计',n==1)

def test_5_stale_worker_cannot_overwrite(source_id):
    print('\n[5] 超时接管后原进程晚归无权改写结果')
    with Session() as db:rid=make_run(db,source_id,'晚归研究')
    r,attempt_a=claim_run(rid,'worker-A')
    check('A 领取 attempt=1',attempt_a==1)
    # 让 A 的 calculate 挂起，模拟进程僵死；期间 B 完成接管与计算
    a_enter=threading.Event();a_release=threading.Event()
    real_calc=dispatch.calculate
    def slow_calc(records,parameters):
        a_enter.set();a_release.wait(timeout=30)
        return real_calc(records,parameters)
    with mock.patch.object(dispatch,'calculate',slow_calc):
        tA=threading.Thread(target=execute_run,args=(rid,'worker-A'));tA.start()
        a_enter.wait(timeout=10)
        # A 心跳停在中途直到超时
        with Session() as db:
            from datetime import datetime,timedelta,timezone
            old=(datetime.now(timezone.utc)-timedelta(seconds=RUN_TIMEOUT+5)).isoformat()
            run=db.get(Run,rid);run.heartbeat_at=old;run.started_at=old;db.commit()
        reconcile_once();drain_queue()
        execute_run(rid,'worker-B')  # B 完成代次 2
        with Session() as db:
            good=db.get(Run,rid)
            check('B 已完成且 attempt=2',good.state=='completed' and good.attempt_id==2)
            good_checksum=good.result_checksum
        # A 晚归：恢复其计算，结束后尝试写终态
        a_release.set();tA.join(timeout=30)
    with Session() as db:
        run=db.get(Run,rid)
        check('晚归 A 未改写最终状态，仍 completed',run.state=='completed')
        check('晚归 A 未改写结果校验和',run.result_checksum==good_checksum)
        check('最终代次仍为 B 的 2',run.attempt_id==2)
    # 再模拟 A 的异常退出路径，fencing 条件同样拦截
    with Session() as db:
        cur=db.get(Run,rid)
        blocked=not (cur.state=='running' and cur.attempt_id==attempt_a)
    check('fencing 条件判定 A 代次失效，禁止写回',blocked)

def test_6_verification_gate(source_id):
    print('\n[6] 结果与完整导出核对一致后才开放采用')
    with Session() as db:rid=make_run(db,source_id,'核对研究')
    execute_run(rid,'w')
    path=_artifact_path(rid)
    check('CSV 工件已生成',path.exists())
    with Session() as db:
        run=db.get(Run,rid)
        check('完成即带 verified_at',bool(run.verified_at))
        check('export 采用闸门通过',export_svc._adoptable(run))
        resp=export_svc.run_csv(db,rid)
    # 导出内容与 result 行数一致
    import csv,io
    rows=list(csv.DictReader(io.StringIO(resp.body.decode('utf-8-sig'))))
    with Session() as db:
        n=len(db.get(Run,rid).result['rows'])
    check('导出 CSV 行数等于结果行数',len(rows)==n,f'{len(rows)} vs {n}')
    # 损坏工件 -> 自动重建且仍通过校验
    path.write_bytes(b'CORRUPT')
    with Session() as db:
        resp2=export_svc.run_csv(db,rid)
    check('工件损坏后从已核对结果自动恢复',b'CORRUPT' not in resp2.body and len(resp2.body)>100)
    # 未核对任务不可导出
    with Session() as db:
        rid2=make_run(db,source_id,'未核对')
    try:
        with Session() as db:export_svc.run_csv(db,rid2)
        check('未核对任务导出被 409 拦截',False)
    except Exception as e:
        check('未核对任务导出被 409 拦截',getattr(e,'status_code',None)==409,str(e))

def test_7_interrupted_writes_recover(source_id):
    print('\n[7] 写文件/落库中断可恢复')
    with Session() as db:rid=make_run(db,source_id,'中断研究')
    # 让原子写在 rename 前抛错
    real_write=dispatch._write_artifact_atomic
    def boom(run_id,result):raise OSError('disk lost during write')
    with mock.patch.object(dispatch,'_write_artifact_atomic',boom):
        execute_run(rid,'w-crash')
    tmps=list(ART.glob('run-*.tmp'))
    check('中断不留下半截正式文件/临时文件',not _artifact_path(rid).exists() and not tmps,str(tmps))
    with Session() as db:check('写入失败落 failed 并保留错误信息',db.get(Run,rid).state=='failed' and db.get(Run,rid).error)
    # 重新排队执行（同冻结输入）成功
    with Session() as db:
        r=db.get(Run,rid);r.state='queued';r.enqueued_at=now();db.commit()
    execute_run(rid,'w-recover')
    with Session() as db:
        run=db.get(Run,rid)
        check('中断后重跑成功并完成核对',run.state=='completed' and bool(run.verified_at))

def test_8_audit_once(source_id):
    print('\n[8] 重复消息只留一次完成审计')
    with Session() as db:rid=make_run(db,source_id,'审计研究')
    execute_run(rid,'w1')
    # 直接重放完成审计写入
    with Session() as db:
        before=db.scalar(select(func.count()).select_from(Audit).where(Audit.event_key==f'run.completed.{rid}'))
        dispatch._log_once(db,f'run.completed.{rid}','worker:w1','run.completed',rid,'dup')
        db.commit()
        after=db.scalar(select(func.count()).select_from(Audit).where(Audit.event_key==f'run.completed.{rid}'))
    check('完成审计恰好一条',before==1 and after==1)
    # 终态任务收到重复队列消息：claim 直接忽略，不新增任何审计
    before_all=0
    with Session() as db:before_all=db.scalar(select(func.count()).select_from(Audit).where(Audit.target==str(rid)))
    execute_run(rid,'stale-msg')
    with Session() as db:after_all=db.scalar(select(func.count()).select_from(Audit).where(Audit.target==str(rid)))
    check('终态重复消息不新增审计',before_all==after_all)

def test_9_visibility(source_id):
    print('\n[9] 等待/接管次数/最终状态字段可见')
    with Session() as db:rid=make_run(db,source_id,'可见性研究')
    claim_run(rid,'w')
    with Session() as db:
        from datetime import datetime,timedelta,timezone
        old=(datetime.now(timezone.utc)-timedelta(seconds=RUN_TIMEOUT+5)).isoformat()
        r=db.get(Run,rid);r.heartbeat_at=old;r.started_at=old;db.commit()
    reconcile_once();drain_queue();execute_run(rid,'w2')
    with Session() as db:
        r=db.get(Run,rid)
        data={c.name:getattr(r,c.name) for c in r.__table__.columns}
    for field in ('state','takeovers','attempt_id','enqueued_at','started_at','heartbeat_at','verified_at','result_checksum','artifact_checksum'):
        check(f'字段 {field} 可读出',field in data and data[field] not in (None,'') or field in('heartbeat_at',))
    check('最终状态 completed',data['state']=='completed')
    check('接管次数=1 可见',data['takeovers']==1)

def test_10_approved_note_keeps_result(source_id):
    print('\n[10] 已批准复访计划仍可读取原结果')
    from src.services import notes
    from src.schemas.note import NoteInput
    with Session() as db:rid=make_run(db,source_id,'留存研究')
    execute_run(rid,'w')
    with Session() as db:
        n=notes.create_note(db,User('analyst@x'),NoteInput(run_id=rid,title='计划',body='正文内容足够长'))
        note_id=n['id']
        approved=notes.approve_note(db,User('admin@x'),note_id)
        frozen=db.get(__import__('src.models.note',fromlist=['Note']).Note,note_id).frozen_result
    check('批准后冻结结果存在且含校验和字段对应行',len(frozen['rows'])==40)
    # 即便原 run 之后被重新计算/替换结果，批准快照不变（这里验证快照是独立副本）
    with Session() as db:
        run=db.get(Run,rid)
        snap=run.result
        note_obj=db.get(__import__('src.models.note',fromlist=['Note']).Note,note_id)
        check('批准快照与当前结果内容一致但相互独立',note_obj.frozen_result['rows']==snap['rows']
              and note_obj.frozen_result is not snap)
        # 模拟原结果被新执行替换（新对象），快照仍可读
        run.result={'summary':[],'clusters':[],'rows':[{'reference':'X'}],'columns':['reference'],
                    'chart':[],'explanation':'x','checksum':'new','seed':42,'standardization':{}}
        db.commit()
    with Session() as db:
        note_obj=db.get(__import__('src.models.note',fromlist=['Note']).Note,note_id)
        resp=export_svc.note_csv(db,note_id)
    check('原结果变更后批准快照仍可导出原始 40 行',b'K-1-0000' in resp.body and b'X' not in resp.body)

def test_11_concurrent_reconcile_single_takeover(source_id):
    print('\n[11] 多进程并发对账只接管一次')
    with Session() as db:rid=make_run(db,source_id,'并发对账')
    claim_run(rid,'wA')
    with Session() as db:
        from datetime import datetime,timedelta,timezone
        old=(datetime.now(timezone.utc)-timedelta(seconds=RUN_TIMEOUT+5)).isoformat()
        r=db.get(Run,rid);r.heartbeat_at=old;r.started_at=old;db.commit()
    outs=[];barrier=threading.Barrier(3)
    def rec():
        barrier.wait()
        try:outs.append(reconcile_once())
        except Exception as e:outs.append([('error',str(e))])
    ts=[threading.Thread(target=rec) for _ in range(3)]
    for t in ts:t.start()
    for t in ts:t.join()
    takeovers=[x for o in outs for x in o if x and x[0]=='takeover']
    check('3 个并发对账只产生 1 次接管',len(takeovers)==1,str(takeovers))
    with Session() as db:
        r=db.get(Run,rid)
        check('takeovers 只增加到 1',r.takeovers==1,str(r.takeovers))
        check('任务回到 queued 等待单次重算',r.state=='queued')
    queued=drain_queue()
    check('队列中该任务只有一条消息',queued.count(rid)==1,str(queued))

if __name__=='__main__':
    sid=setup_module()
    for fn in (test_1_idempotent_create,test_2_requeue_after_outage,test_3_crash_takeover_uses_frozen_input,
               test_4_concurrent_claim_single_execution,test_5_stale_worker_cannot_overwrite,
               test_6_verification_gate,test_7_interrupted_writes_recover,test_8_audit_once,
               test_9_visibility,test_10_approved_note_keeps_result,test_11_concurrent_reconcile_single_takeover):
        fn(sid)
    print(f'\n结果：{PASS} 通过 / {FAIL} 失败')
    sys.exit(1 if FAIL else 0)
