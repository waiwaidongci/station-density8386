from sqlalchemy import Column,Integer,String,JSON,Text,ForeignKey,Index
from ..config.database import Base
from ..config.settings import CONFIG
class Run(Base):
    __tablename__=CONFIG['run_table']
    id=Column(Integer,primary_key=True)
    source_id=Column(Integer,ForeignKey(CONFIG['source_table']+'.id'),nullable=False)
    name=Column(String(160),nullable=False)
    state=Column(String(20),default='queued',nullable=False)
    parameters=Column(JSON,nullable=False)
    snapshot=Column(JSON,nullable=False)
    result=Column(JSON,nullable=True)
    error=Column(Text,default='')
    created_at=Column(String(40),nullable=False)
    # 同一请求重试不新增研究：幂等键唯一；无键任务存 NULL，NULL 在唯一索引中互不冲突
    idempotency_key=Column(String(80),nullable=True)
    # 执行租约与接管：attempt_id 是 fencing token，每次领取递增
    attempt_id=Column(Integer,default=0,nullable=False)
    enqueued_at=Column(String(40),default='')
    started_at=Column(String(40),default='')
    heartbeat_at=Column(String(40),default='')
    # 崩溃恢复后沿用冻结输入重新计算；接管次数对复访人员可见
    takeovers=Column(Integer,default=0,nullable=False)
    # 结果与完整导出文件核对一致后才开放采用
    result_checksum=Column(String(64),default='')
    artifact_checksum=Column(String(64),default='')
    verified_at=Column(String(40),default='')
    __table_args__=(Index('ix_runs_idempotency','idempotency_key',unique=True),
                    Index('ix_runs_state','state'))
