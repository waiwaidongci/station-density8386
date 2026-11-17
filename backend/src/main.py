import os,logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy import text
from .config.database import Base,engine
from .config.settings import CONFIG
from .config.migrate import run_migrations
from .models import user,source,record,run,note,audit
from .middlewares.errors import error_boundary
from .middlewares.rate_limit import rate_limit
from .routes import auth,sources,runs,notes,audit as audit_routes
from .services.dispatch import queue,start_reconciler,reconcile_once
@asynccontextmanager
async def lifespan(app):
    Base.metadata.create_all(engine)
    run_migrations()
    # 队列短断补送 / 超时接管：backend 进程内守护线程，worker 多副本也只有它做对账决策
    try:reconcile_once()
    except Exception:logging.exception('startup reconcile failed')
    stop=start_reconciler()
    if os.getenv('SEED_DEMO','true')=='true':
        try:
            from scripts.seed_demo import seed
            seed()
        except Exception:logging.exception('seed failed; service remains available')
    yield
    stop.set()
app=FastAPI(title=CONFIG['title'],lifespan=lifespan)
app.middleware('http')(error_boundary)
app.middleware('http')(rate_limit)
for router in (auth.router,sources.router,runs.router,notes.router,audit_routes.router):app.include_router(router)
@app.get('/health')
def health():
    with engine.connect() as c:c.execute(text('SELECT 1'))
    queue.ping();return {'status':'ok'}
@app.get('/catalog')
def catalog():return CONFIG
