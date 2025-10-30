from fastapi import APIRouter, Depends, status

from sqlalchemy.orm import Session

from src.services.feedback.feedback_service import FeedbackService
from src.database.db import Base, engine
from src.database.deps import get_db
from .schemas import Input

Base.metadata.create_all(bind=engine)

service = FeedbackService()

router = APIRouter(prefix="/feedback", tags=["Feedback"])

@router.post("/", status_code=status.HTTP_201_CREATED)
def store_feedback(request: Input, db: Session = Depends(get_db)):
    """
    Store feedback into the database if the document id exists in the collection.
    """
    feedback_id = service.add_to_db(request, db)

    return {"message": "Feedback stored successfully", "data": feedback_id}
