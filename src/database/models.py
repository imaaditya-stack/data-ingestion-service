# pylint: disable=E1102

import enum

from sqlalchemy.sql import func
from sqlalchemy import (
    Enum as SqlEnum,
    Column,
    Integer,
    String,
    DateTime,
    UniqueConstraint,
    CheckConstraint,
)
from src.database.db import Base


# Custom Entity Class
class Entity(str, enum.Enum):
    COMPANY = "company"
    PRODUCT = "product"


# Custom Entity Validation
def get_enum_values(enum_class):
    return [member.value for member in enum_class]


class Feedback(Base):
    """
    Feedback table storing user reactions to documents.

    Attributes:
        id (int): Primary key.
        query (str): The original query or text related to the feedback.
        document_id (int): ID of the related document.
        entity(enum): The unique entity of the document {company, product}.
        reaction (int): Numeric reaction (e.g., like/dislike, rating, etc.).
        created_at (datetime): Timestamp when the feedback was created.
        updated_at (datetime): Timestamp when the feedback was last updated.
        action_by (int): ID of the user who performed the action.
        tenant_id (int): ID of the tenant (for multi-tenant setups).
        comment (str): Optional comment explaining the feedback.
    """

    __tablename__ = "feedback"

    id = Column(Integer, primary_key=True)
    query = Column(String(512), nullable=False)
    document_id = Column(Integer, index=True, nullable=False)
    entity = Column(
        SqlEnum(Entity, values_callable=get_enum_values),
        nullable=False,
        doc="Type of entity the feedback is related to ('company' or 'product').",
    )

    reaction = Column(Integer, nullable=False)

    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    action_by = Column(Integer, nullable=False)
    tenant_id = Column(Integer, nullable=False)
    comment = Column(String(1024), nullable=True)

    # Unique Table Constraints
    __table_args__ = (
        UniqueConstraint(
            "query",
            "document_id",
            "entity",
            "tenant_id",
            name="uix_query_document_entity_tenant",
        ),
        CheckConstraint("reaction IN (0, 1)", name="check_reaction_binary"),
    )
