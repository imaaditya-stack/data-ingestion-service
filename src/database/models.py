from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    UniqueConstraint
)

from src.database.db import Base

# Feedback Table
class Feedback(Base):
    __tablename__ = 'feedback'

    id = Column(Integer, primary_key=True, index=True)
    document_id = Column(Integer, index = True)

    reaction = Column(Integer)

    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    
    action_by = Column(Integer)
    tenant_id = Column(Integer)
    
    comment = Column(String)

    __table_args__ = (
        UniqueConstraint('document_id', 'action_by', name='uix_document_action'),
    )