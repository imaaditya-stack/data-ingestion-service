"""
Tenant management routes
"""

from typing import List

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select

from src.core.dependencies import DBSessionDep, TenantOnboardingServiceDep
from src.database.models import Tenant
from src.routes.tenant.schemas import (
    OnboardTenantRequest,
    OnboardTenantResponse,
    TenantResponse,
)

router = APIRouter(prefix="/tenant", tags=["Tenant"])


@router.post(
    "/onboard",
    response_model=OnboardTenantResponse,
    status_code=status.HTTP_201_CREATED,
)
async def onboard_tenant(
    request: OnboardTenantRequest,
    db: DBSessionDep,
    onboarding_service: TenantOnboardingServiceDep,
):
    """
    Onboard a new tenant.

    Creates a tenant record with:
    - Generated and encrypted secret keys (primary and secondary)
    - Default configuration (ChromaDB settings)
    - Initial onboarding status: PENDING
    """

    try:
        tenant, primary_secret, secondary_secret = (
            await onboarding_service.onboard_tenant(
                db=db,
                tenant_id=request.tenant_id,
                tenant_name=request.tenant_name,
            )
        )

        return OnboardTenantResponse(
            message="Tenant onboarded successfully",
            data={
                "id": tenant.id,
                "tenant_id": tenant.tenant_id,
                "tenant_name": tenant.tenant_name,
                "onboarding_status": tenant.onboarding_status.value,
                "secret_key_primary": primary_secret,
                "secret_key_secondary": secondary_secret,
                "config": tenant.config,
            },
        )
    except ValueError as e:
        # Session context manager will handle rollback automatically
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e),
        )
    except Exception as e:
        # Session context manager will handle rollback automatically
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to onboard tenant: {str(e)}",
        )


@router.get("/", response_model=List[TenantResponse])
async def get_all_tenants(
    db: DBSessionDep,
):
    """
    Get all tenants.

    Returns list of all tenants (without secret keys for security).
    """
    try:
        result = await db.execute(select(Tenant))
        tenants = result.scalars().all()

        return [
            TenantResponse(
                id=tenant.id,
                tenant_id=tenant.tenant_id,
                tenant_name=tenant.tenant_name,
                onboarding_status=tenant.onboarding_status.value,
                config=tenant.config,
                created_at=tenant.created_at,
                updated_at=tenant.updated_at,
            )
            for tenant in tenants
        ]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve tenants: {str(e)}",
        )
