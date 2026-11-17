from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException
from ..models.run import Run
from ..utils.serialization import row,now
from .sources import source_or_404,list_records
from .domain_validation import validate_parameters
from .audit import log
from .dispatch import queue,enqueue_run

def get_run(db,id):
    r=db.get(Run,id)
    if not r:raise HTTPException(404,'未找到分析任务')
    return r

def create_run(db,user,data,idempotency_key=None):
    # 同一请求重试不新增研究：命中幂等键直接返回既有任务（含进行中/已完成）
    key=(idempotency_key or '').strip() or None
    if key:
        existing=db.scalar(select(Run).where(Run.idempotency_key==key))
        if existing:return row(existing)
    s=source_or_404(db,data.source_id);records=list_records(db,s.id)
    if len(records)<3:raise HTTPException(422,'至少录入3条样本后才能执行')
    try: params=validate_parameters(data.parameters)
    except ValueError as e:raise HTTPException(422,str(e))
    r=Run(source_id=s.id,name=data.name,parameters=params,
          snapshot={'source':row(s),'records':records},
          created_at=now(),state='queued',idempotency_key=key,
          enqueued_at=now(),takeovers=0,attempt_id=0)
    db.add(r)
    try:
        db.flush()
    except IntegrityError:
        # 并发重试同一幂等键：回退到既有任务，仍然不新增研究
        db.rollback()
        if key:
            existing=db.scalar(select(Run).where(Run.idempotency_key==key))
            if existing:return row(existing)
        raise HTTPException(409,'重复提交，且未找到对应既有任务')
    log(db,user,'run.queued',r.id,data.name)
    db.commit()
    # 队列短断：带退避重试；仍失败也保留 queued，backend 对账线程会自动补送
    enqueue_run(r.id)
    return row(r)

def list_runs(db):
    return [{k:v for k,v in row(r).items() if k not in ('snapshot','result')} for r in db.scalars(select(Run).order_by(Run.id.desc()))]
