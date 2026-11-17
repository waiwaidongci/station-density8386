"""可靠调度：入队重试、租约接管、fencing token、工件核对与幂等审计。

设计要点（对应可靠性需求）：
- enqueue 带重试，队列短断后由 backend 守护线程 reconcile 补送，不丢研究。
- 多进程 worker 同时领取时，条件 UPDATE（compare-and-set）做状态守卫：
  PostgreSQL 由行锁串行、SQLite 由写锁串行，保证只有一份有效执行；
  每次领取递增 attempt_id（fencing token），被接管的原进程晚归时
  attempt 已不匹配，无权改写结果。
- 写结果与 CSV 工件走“临时文件 + fsync + 原子 rename + SHA256 双向核对”，
  任一步中断都可沿冻结输入重新计算，核对一致后才置 completed。
- 终态审计带唯一 event_key，重复队列消息只产生一次完成审计。
"""
import os,json,time,logging,threading,hashlib,tempfile
from pathlib import Path
from redis import Redis
from sqlalchemy import select,update
from ..config.settings import REDIS_URL,ARTIFACT_DIR
from ..config.database import Session
from ..models.run import Run
from ..models.audit import Audit
from ..utils.serialization import now
from ..utils.csv_export import csv_response
from .algorithm import calculate

QUEUE_KEY='analysis:queue'
# 时间阈值（秒）：排队超过 ENQUEUE_GRACE 仍未被领取则补送；
# 运行中超过 RUN_TIMEOUT 未心跳则判定崩溃/僵死并接管重算。
ENQUEUE_GRACE=20
RUN_TIMEOUT=60
HEARTBEAT_INTERVAL=2
RECONCILE_INTERVAL=10
ENQUEUE_RETRIES=5
queue=Redis.from_url(REDIS_URL)
ARTIFACT_DIR.mkdir(parents=True,exist_ok=True)


def enqueue_run(run_id,retries=ENQUEUE_RETRIES):
    """RPUSH 带重试；全部失败时调用方保留 queued 态，由对账线程补送。"""
    last=None
    for attempt in range(retries):
        try:
            queue.rpush(QUEUE_KEY,run_id)
            return True
        except Exception as e:  # 队列短断：退避后重试，不新增研究
            last=e
            time.sleep(min(0.2*(2**attempt),2.0))
    logging.warning('enqueue run %s failed after %s tries: %s',run_id,retries,last)
    return False


def _log_once(db,event_key,actor,action,target,detail=''):
    """幂等审计：event_key 唯一，重复消息/重复提交只留一条。"""
    if db.scalar(select(Audit.id).where(Audit.event_key==event_key)):
        return False
    db.add(Audit(actor=actor,action=action,target=str(target),detail=detail,
                 created_at=now(),event_key=event_key))
    return True


def claim_run(run_id,worker_id):
    """领取任务：条件 UPDATE 做状态守卫，多进程/多线程并发只有一个能成功。

    返回 (Run, attempt_id)；未抢到或任务已终态返回 (None, None)。
    单条 UPDATE ... WHERE state='queued' 在 PG 上持行锁、SQLite 上由写锁串行，
    rowcount 保证只有一个执行者把状态推进到 running。
    """
    ts=now()
    with Session() as db:
        r=db.get(Run,run_id)
        if not r or r.state!='queued':
            return None,None
        next_attempt=(r.attempt_id or 0)+1
        result=db.execute(
            update(Run).where(Run.id==run_id,Run.state=='queued')
            .values(state='running',attempt_id=next_attempt,
                    started_at=ts,heartbeat_at=ts,error=''))
        if result.rowcount!=1:
            db.rollback()
            return None,None
        snapshot=r.snapshot
        _log_once(db,f'run.started.{run_id}.{next_attempt}','worker:'+worker_id,
                  'run.started',run_id,f'第 {next_attempt} 次执行')
        db.commit()
    r.state='running';r.attempt_id=next_attempt
    return r,next_attempt


def _artifact_path(run_id):
    return ARTIFACT_DIR/f'run-{run_id}.csv'


def _write_artifact_atomic(run_id,result):
    """临时文件 + fsync + 原子 rename；中途崩溃只可能留下 .tmp，不会出现半截正式文件。"""
    body=csv_response(result,'run.csv').body
    path=_artifact_path(run_id)
    fd,tmp=tempfile.mkstemp(prefix=path.name+'.',suffix='.tmp',dir=str(ARTIFACT_DIR))
    try:
        with open(fd,'wb') as f:
            f.write(body);f.flush();os.fsync(f.fileno())
        Path(tmp).replace(path)
    except BaseException:
        try:Path(tmp).unlink(missing_ok=True)
        except Exception:logging.exception('cleanup temp artifact failed')
        raise
    return body,hashlib.sha256(body).hexdigest()


def _verify_artifact(run_id,result,expected_checksum):
    """结果与完整导出文件核对一致：文件可完整解析、列/行数/逐行内容与 result 相等，校验和匹配。"""
    import csv,io
    path=_artifact_path(run_id)
    raw=path.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=expected_checksum:
        raise RuntimeError('导出文件校验和不一致')
    text=raw.decode('utf-8-sig')
    reader=csv.DictReader(io.StringIO(text))
    if reader.fieldnames!=result['columns']:
        raise RuntimeError('导出文件列结构与结果不一致')
    rows=list(reader)
    if len(rows)!=len(result['rows']):
        raise RuntimeError('导出文件行数与结果不一致')
    for exported,original in zip(rows,result['rows']):
        for col in result['columns']:
            if str(exported.get(col,''))!=str(original.get(col,'')):
                raise RuntimeError(f'导出文件内容与结果不一致：{col}')
    return True


def execute_run(run_id,worker_id):
    """一次完整执行；任何异常都安全落 failed 或被 fencing 拦截。"""
    r,attempt=claim_run(run_id,worker_id)
    if r is None:
        return  # 已被其他进程领取或已终态：重复消息直接忽略
    stop=threading.Event()
    def heartbeat():
        while not stop.wait(HEARTBEAT_INTERVAL):
            try:
                with Session() as db:
                    cur=db.get(Run,run_id)
                    # 只允许当前 attempt 续租；被接管后旧进程立刻停跳
                    if cur and cur.state=='running' and cur.attempt_id==attempt:
                        cur.heartbeat_at=now();db.commit()
                    else:
                        stop.set()
            except Exception:
                logging.exception('heartbeat update failed')
    hb=threading.Thread(target=heartbeat,name=f'hb-{run_id}-{attempt}',daemon=True)
    hb.start()
    try:
        # 始终使用创建时冻结的输入；接管重算不重新读取来源当前数据
        result=calculate(r.snapshot['records'],r.parameters)
        json.dumps(result,allow_nan=False)  # 拒绝 NaN/Infinity 等不可 JSON 化结果
        body,artifact_checksum=_write_artifact_atomic(r.id,result)
        _verify_artifact(r.id,result,artifact_checksum)
        result_checksum=hashlib.sha256(
            json.dumps(result,sort_keys=True,ensure_ascii=False,allow_nan=False).encode()
        ).hexdigest()
        ts=now()
        with Session() as db:
            # fencing：只有仍持有当前 attempt 的运行代次才能写终态；晚归者无权改写
            outcome=db.execute(
                update(Run).where(Run.id==r.id,Run.state=='running',Run.attempt_id==attempt)
                .values(state='completed',result=result,result_checksum=result_checksum,
                        artifact_checksum=artifact_checksum,verified_at=ts,error=''))
            if outcome.rowcount!=1:
                db.rollback()
                logging.warning('stale success attempt %s for run %s suppressed',attempt,r.id)
                return
            # 完成审计幂等：同一 run 只可能有一条 run.completed
            _log_once(db,f'run.completed.{r.id}','worker:'+worker_id,
                      'run.completed',r.id,f'attempt={attempt}, sha256={result_checksum[:12]}')
            db.commit()
    except Exception as e:
        logging.exception('analysis failed for run %s attempt %s',run_id,attempt)
        with Session() as db:
            # 晚归失败同样不能覆盖接管者或已完成结果
            outcome=db.execute(
                update(Run).where(Run.id==run_id,Run.state=='running',Run.attempt_id==attempt)
                .values(state='failed',error=str(e)[:2000]))
            if outcome.rowcount==1:
                _log_once(db,f'run.failed.{run_id}.{attempt}','worker:'+worker_id,
                          'run.failed',run_id,str(e)[:1800])
            db.commit()
    finally:
        stop.set()


def _queued_ids():
    """当前队列中已有的 run id（演示级队列深度，LRANGE 全量扫描成本可忽略）。"""
    try:
        return {int(v) for v in queue.lrange(QUEUE_KEY,0,-1)}
    except Exception:
        logging.exception('queue scan failed; treat as empty for reconcile')
        return set()


def reconcile_once():
    """一轮对账：补送排队漏单 + 接管超时运行（崩溃恢复）。

    多 worker 并发对账时，接管走条件 UPDATE（state='running' 且心跳/代次未变），
    只有一个进程能把僵死任务推进回 queued，避免重复接管与重复入队。
    补送前先确认队列里没有该消息，正常积压不重复推送。
    """
    recovered=[]
    ts=now()
    in_queue=_queued_ids()
    with Session() as db:
        for r in db.scalars(select(Run).where(Run.state=='queued')).all():
            # 刚入队的给一个宽限，避免与正常 BLPOP 投递重复推送（重复消息本身也安全）
            age=time.time()-_parse(r.enqueued_at) if r.enqueued_at else 999
            if age>=ENQUEUE_GRACE and r.id not in in_queue:
                # 条件 UPDATE 抢占补送权：enqueued_at 未被其他对账者改动才成功
                old_enq=r.enqueued_at
                won=db.execute(
                    update(Run).where(Run.id==r.id,Run.state=='queued',Run.enqueued_at==old_enq)
                    .values(enqueued_at=ts)).rowcount==1
                if won:recovered.append(('queued',r.id))
        for r in db.scalars(select(Run).where(Run.state=='running')).all():
            age=time.time()-_parse(r.heartbeat_at or r.started_at)
            if age>=RUN_TIMEOUT:
                next_take=(r.takeovers or 0)+1
                # 接管 fencing：仅当代次/心跳未被其他进程改动时才接管成功
                won=db.execute(
                    update(Run).where(Run.id==r.id,Run.state=='running',
                                      Run.attempt_id==r.attempt_id,
                                      Run.heartbeat_at==r.heartbeat_at)
                    .values(state='queued',takeovers=next_take,enqueued_at=ts)).rowcount==1
                if won:recovered.append(('takeover',r.id,next_take))
        db.commit()
    for item in recovered:
        kind,run_id=item[0],item[1]
        if enqueue_run(run_id):
            with Session() as db:
                if kind=='takeover':
                    _log_once(db,f'run.takeover.{run_id}.{item[2]}','reconciler',
                              'run.takeover',run_id,f'超时接管第 {item[2]} 次，沿用冻结输入重算')
                else:
                    _log_once(db,f'run.requeued.{run_id}.{int(time.time())}',
                              'reconciler','run.requeued',run_id,'队列短断补送')
                db.commit()
        logging.info('reconcile %s run %s',kind,run_id)
    return recovered


def _parse(iso):
    from datetime import datetime
    try:return datetime.fromisoformat(iso).timestamp()
    except Exception:return 0.0


def reconcile_loop(stop_event):
    while not stop_event.wait(RECONCILE_INTERVAL):
        try:reconcile_once()
        except Exception:logging.exception('reconcile loop error')


def start_reconciler():
    stop=threading.Event()
    t=threading.Thread(target=reconcile_loop,args=(stop,),name='run-reconciler',daemon=True)
    t.start()
    return stop
