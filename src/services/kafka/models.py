"""
Pydantic models for Kafka event schema
"""

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class EventMetadata(BaseModel):
    operation: str = Field(..., description="CRUD operation e.g., CREATE/UPDATE/DELETE")
    source: str = Field(..., description="Originating system e.g., django-backend")


class KafkaEvent(BaseModel):
    event_id: str
    tenant_id: str
    timestamp: datetime
    data: Dict[str, Any]
    metadata: EventMetadata
    schema_version: int = 1

    @property
    def entity_id(self) -> Optional[str]:
        value = self.data.get("id") if isinstance(self.data, dict) else None
        return str(value) if value is not None else None

    def build_document_id(self, entity_type: Optional[str] = None) -> str:
        if self.entity_id:
            if entity_type:
                return f"{self.tenant_id}_{entity_type}_{self.entity_id}"
            return f"{self.tenant_id}_{self.entity_id}"
        return f"{self.tenant_id}_{self.event_id}"


class ProductData(BaseModel):
    id: str | int
    product_name: str
    product_description: str
    category: str
    category_id: str | int
    created_at: str
    updated_at: str
    seller_id: str | int
    seller_name: str

    class Config:
        extra = "ignore"


class CompanyData(BaseModel):
    id: str | int
    company_name: str
    company_description: str
    company_address: str
    created_at: str
    updated_at: str
    seller_id: str | int
    seller_name: str
    business_type_name: str
    business_type_id: str | int

    class Config:
        extra = "ignore"
