"""
ChromaDB Inspector - View documents and embeddings stored in the vector database
Exports detailed analysis to CSV files
"""

import asyncio
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust depth if needed
sys.path.insert(0, str(PROJECT_ROOT))


from dotenv import load_dotenv

from src.providers.vector_stores import VectorStoreFactory
from src.config.settings import VectorStoreConfig

load_dotenv()


async def cleanup_chromadb():
    """Clean up the contents of ChromaDB"""

    _, collection, client = VectorStoreFactory.create(VectorStoreConfig())

    client.delete_collection(name="networking-platform")


async def main():
    try:
        # Inspect ChromaDB contents
        await cleanup_chromadb()

    except Exception as e:
        print(f"❌ Error during cleanup: {e}")
        import traceback

        traceback.print_exc()
        print("Make sure ChromaDB is initialized and contains data.")


if __name__ == "__main__":
    asyncio.run(main())
