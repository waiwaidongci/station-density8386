"""幂等结构迁移：演示环境的数据库卷可能在升级前已存在，需要补列而不丢数据。

SQLAlchemy 的 create_all 只建新表、不改已有表，因此这里用 inspector 检查列后
执行 ADD COLUMN；唯一索引使用 IF NOT EXISTS（PG9.5+ / SQLite 均支持）。
"""
import logging
from sqlalchemy import text,inspect
from .database import engine
from .settings import CONFIG
_RUN_COLUMNS={
    'idempotency_key':"VARCHAR(80)",
    'attempt_id':"INTEGER NOT NULL DEFAULT 0",
    'enqueued_at':"VARCHAR(40) NOT NULL DEFAULT ''",
    'started_at':"VARCHAR(40) NOT NULL DEFAULT ''",
    'heartbeat_at':"VARCHAR(40) NOT NULL DEFAULT ''",
    'takeovers':"INTEGER NOT NULL DEFAULT 0",
    'result_checksum':"VARCHAR(64) NOT NULL DEFAULT ''",
    'artifact_checksum':"VARCHAR(64) NOT NULL DEFAULT ''",
    'verified_at':"VARCHAR(40) NOT NULL DEFAULT ''",
}
def _add_columns(db,inspector,table,columns):
    existing={c['name'] for c in inspector.get_columns(table)}
    for name,ddl in columns.items():
        if name not in existing:
            db.execute(text(f'ALTER TABLE {table} ADD COLUMN {name} {ddl}'))
            logging.info('migration: %s.%s added',table,name)
def run_migrations():
    run_table=CONFIG['run_table']
    inspector=inspect(engine)
    with engine.begin() as db:
        _add_columns(db,inspector,run_table,_RUN_COLUMNS)
        if 'event_key' not in {c['name'] for c in inspector.get_columns('audit_events')}:
            db.execute(text("ALTER TABLE audit_events ADD COLUMN event_key VARCHAR(120)"))
            logging.info('migration: audit_events.event_key added')
        db.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS ix_runs_idempotency '
                        f'ON {run_table} (idempotency_key)'))
        db.execute(text('CREATE INDEX IF NOT EXISTS ix_runs_state ON '
                        f'{run_table} (state)'))
        db.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS ix_audit_event_key '
                        'ON audit_events (event_key)'))
