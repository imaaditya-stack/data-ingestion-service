"""
ChromaDB Inspector - View documents and embeddings stored in the vector database
Exports detailed analysis to CSV files
"""

import asyncio
import sys
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust depth if needed
sys.path.insert(0, str(PROJECT_ROOT))


from dotenv import load_dotenv

from src.config.settings import VectorStoreConfig
from src.providers.vector_stores import VectorStoreFactory

load_dotenv()

import chromadb
import pandas as pd
from chromadb.config import Settings


async def inspect_chromadb():
    """Inspect the contents of ChromaDB and export to CSV"""

    _, collection, _ = VectorStoreFactory.create(VectorStoreConfig())

    # Get all documents
    results = collection.get(include=["documents", "metadatas", "embeddings"])

    total_docs = len(results["documents"])

    # Analyze metadata to count entities based on "entity" field only
    entity_counter = Counter()

    for metadata in results["metadatas"]:
        if metadata and "entity" in metadata:
            entity_counter[metadata["entity"]] += 1

    # Create entity summary DataFrame
    summary_rows = []
    for entity_type, count in entity_counter.most_common():
        summary_rows.append({"entity_type": entity_type, "count": count})

    summary_df = pd.DataFrame(summary_rows)

    # Create detailed document export DataFrame - only 3 documents
    num_docs_to_export = min(3, total_docs)
    document_rows = []

    # print(results["ids"])
    for i in range(num_docs_to_export):
        doc_text = results["documents"][i] if i < len(results["documents"]) else ""
        metadata = results["metadatas"][i] if i < len(results["metadatas"]) else {}

        row = {
            "document_id": results["ids"][i] if i < len(results["ids"]) else f"doc_{i}",
            "content_preview": (
                doc_text[:200] + "..." if len(doc_text) > 200 else doc_text
            ),
            "content_length": len(doc_text),
            "embedding_dim": (
                len(results["embeddings"][i]) if i < len(results["embeddings"]) else 0
            ),
        }

        # Add all metadata fields as columns
        for key, value in (metadata if metadata else {}).items():
            row[key] = value

        document_rows.append(row)

    doc_df = pd.DataFrame(document_rows)

    # Export to CSV
    output_dir = Path("__data__")
    output_dir.mkdir(exist_ok=True)

    summary_path = output_dir / "chromadb_metadata_summary.csv"
    details_path = output_dir / "chromadb_documents.csv"

    summary_df.to_csv(summary_path, index=False)
    doc_df.to_csv(details_path, index=False)


async def main():
    """Main inspection function"""
    try:
        # Inspect ChromaDB contents
        await inspect_chromadb()

    except Exception as e:
        print(f"❌ Error during inspection: {e}")
        import traceback

        traceback.print_exc()
        print("Make sure ChromaDB is initialized and contains data.")


if __name__ == "__main__":
    asyncio.run(main())
