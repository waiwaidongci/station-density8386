from sqlalchemy import Column,Integer,String,Text,Index
from ..config.database import Base
class Audit(Base):
    __tablename__='audit_events'
    id=Column(Integer,primary_key=True)
    actor=Column(String(120),nullable=False)
    action=Column(String(80),nullable=False)
    target=Column(String(160),nullable=False)
    detail=Column(Text,default='')
    created_at=Column(String(40),nullable=False)
    # 幂等事件键：同一任务的同一终态事件只允许一条，重复消息不重复审计；NULL 互不冲突
    event_key=Column(String(120),nullable=True)
    __table_args__=(Index('ix_audit_event_key','event_key',unique=True),)
