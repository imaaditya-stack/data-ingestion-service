from typing import Optional
from pydantic import BaseModel, Field

from src.database.models import Entity


class StoreFeedbackRequest(BaseModel):
    """
    StoreFeedbackRequest json format.
    """

    query: str = Field(
        ..., min_length=2, max_length=512, description="User query, max 512 characters"
    )

    document_id: int = Field(
        ..., ge=0, description="Unique identifier of the document being reacted to"
    )

    entity: Entity = Field(
        ..., description="The unique entity of the document {company, product}"
    )

    reaction: int = Field(
        ..., ge=0, le=1, description="Reaction value (e.g., rating or sentiment score)"
    )

    action_by: int = Field(
        ..., ge=0, description="User ID or actor performing the action"
    )

    tenant_id: int = Field(
        ..., ge=0, description="Tenant identifier for multi-tenant architecture"
    )

    comment: Optional[str] = Field(
        None,
        max_length=1024,
        description="Optional user comment (up to 1024 characters)",
    )
