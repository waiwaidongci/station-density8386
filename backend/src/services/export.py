import csv,io,hashlib,logging
from fastapi import HTTPException
from fastapi.responses import Response
from ..models.note import Note
from ..utils.csv_export import csv_response
from .runs import get_run
from .dispatch import _artifact_path,_write_artifact_atomic,_verify_artifact

def _adoptable(r):
    """采用闸门：状态 completed 且结果/工件已完成双向核对。"""
    return r.state=='completed' and r.verified_at and r.result and r.artifact_checksum

def _ensure_artifact(r):
    """工件卷故障或写入中断后恢复：按已核对结果重建并重新验证，不放宽一致性要求。"""
    path=_artifact_path(r.id)
    need=not path.exists()
    if not need:
        try:
            if hashlib.sha256(path.read_bytes()).hexdigest()!=r.artifact_checksum:need=True
        except OSError:need=True
    if need:
        logging.warning('artifact missing/corrupt for run %s; rebuilding from verified result',r.id)
        _,checksum=_write_artifact_atomic(r.id,r.result)
        if checksum!=r.artifact_checksum:
            raise HTTPException(500,'导出文件重建后校验仍不一致，结果暂不可采用')
    _verify_artifact(r.id,r.result,r.artifact_checksum)

def run_csv(db,id):
    r=get_run(db,id)
    if not _adoptable(r):raise HTTPException(409,'结果尚未完成核对，暂不可导出采用')
    _ensure_artifact(r)
    return csv_response(r.result,'run-'+str(id)+'.csv')

def note_csv(db,id):
    n=db.get(Note,id)
    if not n:raise HTTPException(404,'未找到说明')
    if n.state!='approved':raise HTTPException(403,'仅批准后的业务说明可导出')
    return csv_response(n.frozen_result,'approved-'+str(id)+'.csv')
