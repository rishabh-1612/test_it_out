from typing import Literal
from pydantic import BaseModel



class SyncEventFormat(BaseModel):
    status: Literal["success", "failed"] = "success"
    message: str = "success"
    event: Literal["sync_log", "sync_completed", "sync_failed", "error"] = "sync_completed"
    status_code: Literal[200, 500, 400] = 200


