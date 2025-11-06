# pylint: disable=E1102

import enum

from sqlalchemy import JSON, Boolean, CheckConstraint, Column, DateTime
from sqlalchemy import Enum as SqlEnum
from sqlalchemy import ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from src.database.db import Base


# Custom Entity Class
class Entity(str, enum.Enum):
    COMPANY = "company"
    PRODUCT = "product"


# Onboarding Status Enum
class OnboardingStatus(str, enum.Enum):
    PENDING = "pending"
    ACTIVE = "active"
    FAILED = "failed"
    SUSPENDED = "suspended"


# Plan Type Enum
class PlanType(str, enum.Enum):
    PREDEFINED = "predefined"  # Standard plans like "basic", "premium", "enterprise"
    CUSTOM = "custom"  # Custom plans for enterprise customers


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


class Tenant(Base):
    """
    Tenants table storing tenant metadata and default configuration.

    Attributes:
        id (int): Primary key (internal database ID).
        tenant_id (str): External tenant ID from Django source app (unique, indexed).
        tenant_name (str): Human-readable tenant name (unique).
        secret_key_primary (str): Primary encrypted secret key for HMAC signature verification.
        secret_key_secondary (str): Secondary encrypted secret key for key rotation.
        config (dict): Default/fallback JSON configuration storing:
            - ChromaDB settings (remote_host, remote_port, collection_name)
            - Embedding model configuration
            - LLM configuration
            - Any other tenant-specific default settings
            Note: This serves as fallback when no active plans exist or for settings not
            overridden by active plans. Active plans in tenant_plans table take precedence.
        onboarding_status (OnboardingStatus): Status of tenant onboarding.
        created_at (datetime): Timestamp when the tenant was created.
        updated_at (datetime): Timestamp when the tenant was last updated.
        plans (relationship): One-to-many relationship to TenantPlan subscriptions.
    """

    __tablename__ = "tenants"

    id = Column(Integer, primary_key=True)
    tenant_id = Column(String(255), unique=True, nullable=False)
    tenant_name = Column(String(255), unique=True, nullable=False)
    secret_key_primary = Column(String(512), nullable=False)
    secret_key_secondary = Column(String(512), nullable=False)
    config = Column(JSON, nullable=True)
    onboarding_status = Column(
        SqlEnum(OnboardingStatus, values_callable=get_enum_values),
        nullable=False,
        default=OnboardingStatus.PENDING,
        server_default="pending",
        doc="Status of tenant onboarding: pending, active, failed, suspended",
    )
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # Relationship to tenant plans
    plans = relationship(
        "TenantPlan", back_populates="tenant", cascade="all, delete-orphan"
    )

    # Table Constraints and Indexes
    # Note: tenant_id already has an index from unique constraint
    __table_args__ = (Index("ix_tenants_onboarding_status", "onboarding_status"),)


class TenantPlan(Base):
    """
    Tenant subscription plans table supporting multiple active plans per tenant.

    This table allows tenants to have multiple active subscription plans simultaneously
    (e.g., base plan + add-ons). Plans are prioritized by the priority field, with
    higher priority plans taking precedence for conflicting configuration settings.

    Attributes:
        id (int): Primary key.
        tenant_id (int): Foreign key to tenants table.
        plan_name (str): Plan identifier (e.g., "basic", "premium", "enterprise", or custom name).
        plan_type (PlanType): Whether this is a predefined or custom plan.
        config (dict): Plan-specific JSON configuration storing:
            - Storage limits
            - Rate limits
            - Feature flags
            - ChromaDB overrides
            - Embedding/LLM model overrides
            - Any other plan-specific settings
        is_active (bool): Whether this plan is currently active.
        priority (int): Priority level (higher = higher priority). Used to resolve
            conflicts when multiple active plans have overlapping settings.
        description (str): Optional description of the plan.
        start_date (datetime): When the plan becomes active.
        end_date (datetime): When the plan expires (null = no expiration).
        created_at (datetime): Timestamp when the plan was created.
        updated_at (datetime): Timestamp when the plan was last updated.
        tenant (relationship): Many-to-one relationship to Tenant.
    """

    __tablename__ = "tenant_plans"

    id = Column(Integer, primary_key=True)
    tenant_id = Column(
        Integer,
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    plan_name = Column(
        String(255),
        nullable=False,
        default="default_plan",
        server_default="default_plan",
    )
    plan_type = Column(
        SqlEnum(PlanType, values_callable=get_enum_values),
        nullable=False,
        default=PlanType.PREDEFINED,
        server_default="predefined",
        doc="Type of plan: predefined or custom",
    )
    config = Column(JSON, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True, server_default="1")
    priority = Column(Integer, nullable=False, default=0, server_default="0")
    description = Column(Text, nullable=True)
    start_date = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    end_date = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # Relationship to tenant
    tenant = relationship("Tenant", back_populates="plans")

    # Table Constraints and Indexes
    __table_args__ = (
        Index("ix_tenant_plans_tenant_active", "tenant_id", "is_active"),
        Index("ix_tenant_plans_priority", "priority"),
        Index("ix_tenant_plans_dates", "start_date", "end_date"),
    )
