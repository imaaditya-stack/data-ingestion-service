from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import create_engine
import os

database_directory = os.path.join(os.getcwd(), 'database')
os.makedirs(database_directory, exist_ok=True)

SQLALCHEMY_DATABASE_URL = f"sqlite:///{database_directory}/feedback.db"

engine = create_engine(
    url = SQLALCHEMY_DATABASE_URL,
    connect_args = {
        "check_same_thread": False
    }
)
local_session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()