from datetime import datetime, timezone
from typing import Optional
import asyncio
import logging

from fastapi import HTTPException, status
from fastapi.responses import JSONResponse

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError, OperationalError

from tenacity import (
    retry,
    stop_after_attempt,
    retry_if_exception_type,
    wait_exponential,
    before_sleep_log,
)

from src.services.feedback.config import FeedbackServiceConfig
from src.config.settings import VectorStoreConfig
from src.providers.vector_stores import VectorStoreFactory
from src.database.db import DatabaseConnectionError
from src.database.models import Feedback
from src.routes.feedback.schemas import StoreFeedbackRequest
from src.utils.logger import get_logger

logger = get_logger("feedback_service")


# Custom Exceptions
class FeedbackServiceError(Exception):
    """Base exception for FeedbackService errors."""

    pass


class VectorStoreError(FeedbackServiceError):
    """Vector store related errors."""

    pass


class DocumentNotFoundError(FeedbackServiceError):
    """Document not found in vector store."""

    pass


# Helper Function
def _is_transient_operational_error(exc: OperationalError) -> bool:
    """
    Heuristic to determine whether an OperationalError is transient
    and is worth retrying (deadlock, timeout, connection issues).
    """
    err = str(exc).lower()
    checks = [
        "deadlock",
        "timeout",
        "could not connect",
        "connection reset",
        "server closed the connection",
    ]
    return any(ch in err for ch in checks)


class FeedbackService:
    """Handles creation and update of feedback entries."""

    def __init__(self) -> None:
        logger.info(
            "Initializing FeedbackService",
            extra={"max_retries": FeedbackServiceConfig().max_retries},
        )
        self._initialize_vector_store()

    def _initialize_vector_store(self) -> None:
        """Initialize vector store with proper error handling."""
        try:
            _, self.collection, _ = VectorStoreFactory.create(VectorStoreConfig())

            if not self.collection:
                logger.error("Vector store collection initialization returned None")
                raise VectorStoreError(
                    "Vector store collection could not be initialized"
                )

            logger.info("FeedbackService successfully initialized")

        except Exception as e:
            logger.exception(
                "Failed to initialize vector store",
                extra={"error_type": type(e).__name__},
            )
            raise VectorStoreError("FeedbackService initialization failed")

    async def validate_document_exists(self, document_id: int) -> bool:
        try:
            result = await asyncio.to_thread(
                self.collection.get, where={"id": str(document_id)}, include=[]
            )
            ids = result.get("ids") if isinstance(result, dict) else None
            return bool(ids)
        except Exception:
            logger.exception(
                "Vector store query failed", extra={"document_id": document_id}
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Vector store query failed",
            )

    async def _get_existing_feedback(
        self, db: AsyncSession, input_data: StoreFeedbackRequest
    ) -> Optional[Feedback]:
        """Retrieve existing feedback record if it exists."""

        result = await db.execute(
            select(Feedback).where(
                Feedback.query == input_data.query,
                Feedback.document_id == input_data.document_id,
                Feedback.entity == input_data.entity,
                Feedback.action_by == input_data.action_by,
                Feedback.tenant_id == input_data.tenant_id,
            )
        )
        existing_feedback = result.scalars().first()
        return existing_feedback

    def _create_feedback_object(self, input_data: StoreFeedbackRequest) -> Feedback:
        """Create a new Feedback object from input data."""
        now = datetime.now(timezone.utc)
        return Feedback(
            query=input_data.query,
            document_id=input_data.document_id,
            entity=input_data.entity,
            reaction=input_data.reaction,
            created_at=now,
            updated_at=now,
            action_by=input_data.action_by,
            tenant_id=input_data.tenant_id,
            comment=input_data.comment,
        )

    def _update_feedback_object(
        self, feedback: Feedback, input_data: StoreFeedbackRequest
    ) -> None:
        """Update existing feedback object with new data."""
        feedback.query = input_data.query
        feedback.reaction = input_data.reaction
        feedback.comment = input_data.comment
        feedback.updated_at = datetime.now(timezone.utc)

    async def _handle_integrity_error(
        self, error: IntegrityError, db: AsyncSession
    ) -> None:
        """Handle database integrity errors and raise HTTPException with appropriate status."""
        try:
            await db.rollback()
        except Exception:
            logger.exception("Failed rollback after IntegrityError")
        logger.warning(
            "IntegrityError while saving feedback", extra={"error": str(error)}
        )

        error_str = str(error).lower()
        if "unique constraint" in error_str or "duplicate" in error_str:
            detail = "Duplicate feedback entry detected"
            status_code = status.HTTP_409_CONFLICT
        elif "foreign key" in error_str:
            detail = "Invalid reference in feedback data"
            status_code = status.HTTP_400_BAD_REQUEST
        else:
            detail = "Data integrity violation"
            status_code = status.HTTP_400_BAD_REQUEST

        raise HTTPException(status_code=status_code, detail=detail)

    async def _handle_operational_error(
        self, error: OperationalError, db: AsyncSession
    ) -> None:
        """Handle non-transient OperationalError: rollback and raise HTTPException."""
        try:
            await db.rollback()
        except Exception:
            logger.exception("Failed rollback after OperationalError")

        if _is_transient_operational_error(error):
            logger.warning(
                "Transient OperationalError encountered and will be retried",
                extra={"error": str(error)},
            )
            # Re-raise to allow tenacity to retry the decorated function
            raise error

        logger.error("Non-transient OperationalError", extra={"error": str(error)})
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database temporarily unavailable",
        )

    @retry(
        stop=stop_after_attempt(FeedbackServiceConfig().max_retries),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((OperationalError, DatabaseConnectionError)),
        reraise=True,
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    async def add_or_update_feedback(
        self, input_data: StoreFeedbackRequest, db: AsyncSession
    ) -> JSONResponse:
        """
        Add or update feedback for a given document.
        Retries transient database errors with exponential backoff.
        """
        logger.info(
            "Processing feedback request",
            extra={
                "document_id": input_data.document_id,
                "action_by": input_data.action_by,
                "tenant_id": input_data.tenant_id,
            },
        )

        # Step 1: Validate document existence (vector store)
        if not await self.validate_document_exists(input_data.document_id):
            logger.warning(
                "Document not found in vector store",
                extra={"document_id": input_data.document_id},
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Document ID not found in vector database",
            )

        try:
            # Use explicit transaction scope
            async with db.begin():
                existing_feedback = await self._get_existing_feedback(db, input_data)

                if existing_feedback:
                    # Update existing feedback
                    logger.debug(
                        "Existing feedback found; updating",
                        extra={"feedback_id": existing_feedback.id},
                    )
                    self._update_feedback_object(existing_feedback, input_data)
                    # flush to persist changes and get primary key if needed
                    await db.flush()
                    await db.refresh(existing_feedback)
                    feedback_id = existing_feedback.id
                    status_code = status.HTTP_200_OK
                    message = "Feedback updated successfully"

                else:
                    # Create new feedback (may raise IntegrityError if concurrent insert)
                    new_feedback = self._create_feedback_object(input_data)
                    db.add(new_feedback)
                    await db.flush()
                    await db.refresh(new_feedback)
                    feedback_id = new_feedback.id
                    status_code = status.HTTP_201_CREATED
                    message = "Feedback created successfully"

            # Transaction successful
            logger.info(
                message,
                extra={
                    "feedback_id": feedback_id,
                    "document_id": input_data.document_id,
                    "action_by": input_data.action_by,
                },
            )
            return JSONResponse(
                status_code=status_code,
                content={
                    "message": message,
                    "data": {"feedback_id": feedback_id},
                },
            )

        except IntegrityError as e:
            # Could be a race condition (concurrent insert). Attempt a safe retry: select again and update.
            logger.warning(
                "IntegrityError on create/update; attempting select+update fallback",
                extra={"error": str(e)},
            )
            # Attempt fallback: re-select the row and update it
            try:
                await db.rollback()
            except Exception:
                logger.exception(
                    "Failed rollback after IntegrityError fallback attempt"
                )

            # Re-run select outside transaction to fetch the existing row
            existing_feedback = await self._get_existing_feedback(db, input_data)
            if existing_feedback:
                # Update existing and commit
                try:
                    async with db.begin():
                        self._update_feedback_object(existing_feedback, input_data)
                        await db.flush()
                        await db.refresh(existing_feedback)
                        feedback_id = existing_feedback.id
                    logger.info(
                        "Feedback updated after integrity fallback",
                        extra={"feedback_id": feedback_id},
                    )
                    return JSONResponse(
                        status_code=status.HTTP_200_OK,
                        content={
                            "message": "Feedback updated successfully",
                            "data": {"feedback_id": feedback_id},
                        },
                    )

                except Exception as sub_e:
                    logger.exception(
                        "Failed to update after integrity fallback",
                        extra={"error": str(sub_e)},
                    )
            # If no existing row found or fallback fails, translate integrity error
            await self._handle_integrity_error(e, db)

        except OperationalError as e:
            # Determine transient v/s non-transient
            await self._handle_operational_error(e, db)

        except HTTPException:
            # Re-raise HTTP exceptions
            raise

        except Exception as e:
            try:
                await db.rollback()
            except Exception:
                logger.exception("Failed rollback in generic exception handler")
            logger.exception(
                "Unexpected error while processing feedback",
                extra={
                    "document_id": input_data.document_id,
                    "action_by": input_data.action_by,
                    "error_type": type(e).__name__,
                },
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal server error",
            )
