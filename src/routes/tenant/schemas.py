"""
Pydantic schemas for tenant routes
"""

import enum
from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class OnboardTenantRequest(BaseModel):
    """Request schema for tenant onboarding"""

    tenant_id: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="External tenant ID from Django source app",
    )
    tenant_name: str = Field(
        ..., min_length=1, max_length=255, description="Human-readable tenant name"
    )


class OnboardTenantResponse(BaseModel):
    """Response schema for tenant onboarding"""

    message: str
    data: dict = Field(
        ...,
        description="Tenant information with secret keys",
        example={
            "id": 1,
            "tenant_id": "tenant_abc",
            "tenant_name": "ABC Company",
            "onboarding_status": "pending",
            "secret_key_primary": "abc123...",
            "secret_key_secondary": "def456...",
            "config": {
                "vector_store": {
                    "collection_name": "",
                    "use_remote": True,
                    "remote_host": "https://example.com",
                    "remote_port": 8000,
                }
            },
            "created_at": "2021-01-01T00:00:00Z",
            "updated_at": "2021-01-01T00:00:00Z",
        },
    )


class TenantResponse(BaseModel):
    """Response schema for tenant information (without secret keys)"""

    id: int
    tenant_id: str
    tenant_name: str
    onboarding_status: str
    config: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class OnboardingStatus(str, enum.Enum):
    PENDING = "pending"
    ACTIVE = "active"
    FAILED = "failed"
    SUSPENDED = "suspended"
