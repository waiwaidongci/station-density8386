from ..services import runs
from ..utils.serialization import row
class RunsController:
    list=staticmethod(runs.list_runs)
    @staticmethod
    def create(db,user,data,idempotency_key):
        return runs.create_run(db,user,data,idempotency_key)
    @staticmethod
    def get(db,id):return row(runs.get_run(db,id))
