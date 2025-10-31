from fastapi import HTTPException, Depends, status 

from sqlalchemy.orm import Session

from chromadb import PersistentClient

from .configs import ChromadbConfig

from src.database.models import Feedback
from src.routes.feedback.schemas import Input
from src.database.deps import get_db

from src.utils.logger import get_logger

from typing import Annotated

from datetime import datetime

logger = get_logger('feedback_service')

class FeedbackService:
    def __init__(self):
        
        logger.info('Initializing feedback service')

        # Configs
        self.chromadb_config = ChromadbConfig()

        # Client & Collection
        self.client = PersistentClient(
            path = self.chromadb_config.path
        )
        self.collection = self.client.get_collection(
            self.chromadb_config.collection
        )  
        
        logger.info('Feedback service successfully initiated')

    def validate_document_exists(self, document_id: int):
        """
        Check whether a particular document id
        is present in the vector database.
        """
        result = self.collection.get(where={'id': str(document_id)}, include=['documents', 'metadatas'])
        for metadata in result['metadatas']:
            if str(metadata.get('id')) == str(document_id):
                return True
        else:
            return False
        
    def add_to_db(self, input_data: Input, db: Annotated[Session, Depends(get_db)]):
        """
        Add or update feedback for a document by a user.
        """
        # Step 1: Validate document existence in ChromaDB
        if not self.validate_document_exists(input_data.document_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Document ID not found in the collection')

        # Step 2: Check for existing feedback
        existing_feedback = db.query(Feedback).filter(
            Feedback.document_id == input_data.document_id,
            Feedback.action_by == input_data.action_by
        ).first()

        if existing_feedback:
            # Step 3A: Update existing feedback
            existing_feedback.reaction = input_data.reaction
            existing_feedback.updated_at = datetime.now()
            existing_feedback.comment = input_data.comment

            db.commit()
            db.refresh(existing_feedback)
            logger.info(f'Updated feedback for document {input_data.document_id} by user {input_data.action_by}')
            return {"message": "Feedback updated successfully", "data": existing_feedback.id}
        
        else:
            # Step 3B: Create new feedback entry
            new_feedback = Feedback(
                document_id = input_data.document_id,
                reaction = input_data.reaction,
                created_at = datetime.now(),
                updated_at = datetime.now(),
                action_by = input_data.action_by,
                tenant_id = input_data.tenant_id,
                comment = input_data.comment,
            )

            db.add(new_feedback)
            db.commit()
            db.refresh(new_feedback)
            logger.info(f'Feedback created for document {input_data.document_id} by user {input_data.action_by}')
            return {"message": "Feedback created successfully", "data": new_feedback.id}
