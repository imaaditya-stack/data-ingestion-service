from pydantic import BaseModel

from typing import Optional


class Input(BaseModel):
    """
    Input json format.
    """
    
    document_id: int
    reaction: int
    action_by: int
    tenant_id: int
    comment: Optional[str] = None