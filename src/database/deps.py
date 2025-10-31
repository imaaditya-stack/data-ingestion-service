from .db import local_session


# Dependency
def get_db():
    """Get the database session."""
    db = local_session()
    try:
        yield db
    finally:
        db.close()
