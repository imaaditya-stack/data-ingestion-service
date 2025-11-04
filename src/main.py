from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI

load_dotenv()

from src.utils.logger import get_logger

logger = get_logger("api.main")


class AppContext:
    """Manages application lifecycle and service initialization"""

    def __init__(self):
        pass

    async def startup(self):
        """Startup application services"""
        logger.info("Starting FastAPI Application")

    async def shutdown(self):
        """Shutdown application services"""
        logger.info("Shutting down FastAPI Application")

        logger.info("Shutdown Complete")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan manager for FastAPI app"""
    app_context = AppContext()
    await app_context.startup()
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


@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "API RUNNING"}


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "NETWORKING PLATFORM AI"}
