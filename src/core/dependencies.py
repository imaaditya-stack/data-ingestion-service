from typing import Annotated, AsyncGenerator

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.db import (
    DatabaseConnectionError,
    DatabaseSessionError,
    get_session_manager,
)
from src.services.feedback.feedback_service import FeedbackService
from src.services.tenant.onboarding_service import TenantOnboardingService


# Create FastAPI DB Dependency
async def get_safe_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Provides a safe AsyncSession dependency for FastAPI routes.
    Translates internal DB errors into HTTP exceptions.
    """
    manager = get_session_manager()
    try:
        async with manager.session() as db:
            yield db
    except DatabaseConnectionError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database connection error: {str(e)}",
        )
    except DatabaseSessionError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database session error: {str(e)}",
        )


# Create FastAPI Feedback Service Dependency
async def get_feedback_service(request: Request) -> FeedbackService:
    try:
        return request.app.state.feedback_service
    except AttributeError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="FeedbackService not initialized",
        )


# Create FastAPI Tenant Onboarding Service Dependency
def get_tenant_onboarding_service() -> TenantOnboardingService:
    """Provides TenantOnboardingService dependency"""
    return TenantOnboardingService()


# FastAPI Dependency Injection
DBSessionDep = Annotated[AsyncSession, Depends(get_safe_db_session)]
FeedbackServiceDep = Annotated[FeedbackService, Depends(get_feedback_service)]
TenantOnboardingServiceDep = Annotated[
    TenantOnboardingService, Depends(get_tenant_onboarding_service)
]
