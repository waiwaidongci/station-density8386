"""独立计算进程：BLPOP 消费队列，执行前后均有数据库状态守卫。

- 多进程部署时同一 run_id 即便被重复投递，claim_run 的行锁+状态守卫也只放行一份执行。
- 启动即跑一轮 reconcile：上次进程崩溃留下的 running 租约超时后自动接管，沿用冻结输入。
- 心跳文件供容器健康检查；心跳写库供超时接管判断。
"""
import time,logging,uuid
from .config.database import Base,engine
from .config.migrate import run_migrations
from .config.settings import ARTIFACT_DIR
from .services.dispatch import queue,QUEUE_KEY,execute_run,reconcile_once

WORKER_ID=uuid.uuid4().hex[:12]
ARTIFACT_DIR.mkdir(parents=True,exist_ok=True)

def main():
    logging.basicConfig(level=logging.INFO,
                        format='%%(asctime)s worker %s %%(levelname)s %%(message)s'%WORKER_ID)
    Base.metadata.create_all(engine)
    run_migrations()
    reconcile_once()  # 崩溃恢复：接管僵死租约、补送漏单
    logging.info('worker %s ready',WORKER_ID)
    while True:
        (ARTIFACT_DIR/'heartbeat').write_text(str(time.time()))
        try:
            value=queue.blpop(QUEUE_KEY,timeout=2)
        except Exception:
            logging.exception('queue read error; retry after backoff')
            time.sleep(1)
            continue
        if value:
            try:execute_run(int(value[1]),WORKER_ID)
            except Exception:logging.exception('execute crashed for message %r',value[1])

if __name__=='__main__':main()
