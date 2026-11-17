from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from .settings import DATABASE_URL
class Base(DeclarativeBase): pass
# SQLite 本地开发时，worker 心跳线程与主线程都会开 Session，需要关闭同线程校验；
# 写并发仍由 SQLite 文件锁串行化，生产 PostgreSQL 不受影响。
_connect_args={'check_same_thread':False} if DATABASE_URL.startswith('sqlite') else {}
engine=create_engine(DATABASE_URL,pool_pre_ping=True,connect_args=_connect_args)
Session=sessionmaker(bind=engine,expire_on_commit=False)
def db_session():
    with Session() as db: yield db
