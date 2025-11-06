import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI

load_dotenv()

from src.database import db
from src.routes.feedback import feedback
from src.routes.tenant import tenant
from src.services.feedback.feedback_service import FeedbackService
from src.utils.logger import get_logger

logger = get_logger("api.main")


class AppContext:
    """Manages application lifecycle and service initialization"""

    def __init__(self):
        self.feedback_service = FeedbackService()

    async def startup(self):
        """Startup application services"""
        logger.info("Starting FastAPI Application")

        # Create manager and optionally auto-create tables in non-prod envs
        await db.startup(
            db_url=os.getenv("DATABASE_URL"),
            auto_create_tables=(os.getenv("AUTO_CREATE_TABLES") == "true"),
            create_table_retries=3,
        )

    async def shutdown(self):
        """Shutdown application services"""
        logger.info("Shutting down FastAPI Application")

        logger.info("Shutdown Complete")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan manager for FastAPI app"""
    app_context = AppContext()
    await app_context.startup()

    # Add feedback service to the FastAPI App State
    app.state.feedback_service = app_context.feedback_service

    try:
        yield
    finally:
        await app_context.shutdown()


app = FastAPI(
    title="Networking Platform - AI",
    description="Kafka consumer for data ingestion and ai backend",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(feedback.router)
app.include_router(tenant.router)


@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "API RUNNING"}


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "NETWORKING PLATFORM AI"}
