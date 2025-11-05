from typing import Annotated, AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends, HTTPException, status, Request

from src.database.db import (
    get_db_session,
    DatabaseConnectionError,
    DatabaseSessionError,
)
from src.services.feedback.feedback_service import FeedbackService


# Create FastAPI DB Dependency
async def get_safe_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Provides a safe AsyncSession dependency for FastAPI routes.
    Wraps get_db_session() to translate internal DB errors into HTTP exceptions.
    """
    try:
        async for db in get_db_session():
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
    except Exception as e:
        raise e


# Create FastAPI Feedback Service Dependency
async def get_feedback_service(request: Request) -> FeedbackService:
    try:
        return request.app.state.feedback_service
    except AttributeError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="FeedbackService not initialized",
        )


# FastAPI Dependency Injection
DBSessionDep = Annotated[AsyncSession, Depends(get_safe_db_session)]
FeedbackServiceDep = Annotated[FeedbackService, Depends(get_feedback_service)]
