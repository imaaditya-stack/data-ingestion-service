import asyncio
import os

from llama_index.core import VectorStoreIndex
from llama_index.llms.ollama import Ollama
from src.pipelines.ingestion import IngestionPipeline


async def run_ingestion():

    pipeline = IngestionPipeline()
    nodes = await pipeline.ingest_from_csv(
        os.path.join("__data__", "sample_data(Experts).csv")
    )

    print(f"✅ Ingested {len(nodes)} nodes\n")


async def run_query():
    pipeline = IngestionPipeline()
    index = VectorStoreIndex.from_vector_store(
        pipeline.vector_store, pipeline.embed_model
    )
    llm = Ollama(model="gemma3:latest", context_window=8000)
    query_engine = index.as_query_engine(llm=llm)
    response = query_engine.query("Find experts in Bangalore")
    print(response)


if __name__ == "__main__":
    # asyncio.run(run_ingestion())

    asyncio.run(run_query())
