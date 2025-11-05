from fastapi import APIRouter

from src.core.dependencies import DBSessionDep, FeedbackServiceDep
from .schemas import StoreFeedbackRequest

router = APIRouter(prefix="/feedback", tags=["Feedback"])


@router.post("/")
async def store_feedback(
    request: StoreFeedbackRequest,
    db: DBSessionDep,
    feedback_service: FeedbackServiceDep,
):
    """
    Store feedback into the database if the document id exists in the collection.
    """
    response = await feedback_service.add_or_update_feedback(request, db)
    return response
