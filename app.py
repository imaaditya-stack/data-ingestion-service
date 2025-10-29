"""
Streamlit App for Networking Platform Search
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# pylint: disable=wrong-import-position
import streamlit as st  # pylint: disable=wrong-import-position
from llama_index.core.vector_stores import (  # pylint: disable=wrong-import-position
    ExactMatchFilter,
    MetadataFilters,
)

from src.services.query_engine import (
    QueryEngineService,
)  # pylint: disable=wrong-import-position

# -----------------------------
# Configuration
# -----------------------------
st.set_page_config(
    page_title="🚀 Networking Platform Search",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Apply custom CSS for better UI
st.markdown(
    """
    <style>
        .main {
            background-color: #fafafa;
        }
        h1 {
            color: #2b4162;
            text-align: center;
            margin-bottom: 0.5rem;
        }
        .subtitle {
            text-align: center;
            color: #555;
            font-size: 1.1rem;
            margin-bottom: 2rem;
        }
        .stButton>button {
            background-color: #2b4162;
            color: white;
            border-radius: 8px;
            padding: 0.6rem 1.2rem;
            font-size: 1rem;
            font-weight: 600;
        }
        .stButton>button:hover {
            background-color: #3a5380;
            color: white;
        }
        .result-card {
            padding: 1rem 1.5rem;
            border-radius: 10px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            margin-bottom: 1rem;
            /* background-color: white; */
        }
        .similarity {
            color: #888;
            font-size: 0.9rem;
        }
        .entity-badge {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 12px;
            font-size: 0.85rem;
            font-weight: 600;
            margin-right: 0.5rem;
        }
        .entity-company {
            background-color: #e3f2fd;
            color: #1976d2;
        }
        .entity-product {
            background-color: #f3e5f5;
            color: #7b1fa2;
        }
        .entity-seller {
            background-color: #fff3e0;
            color: #f57c00;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# Header
# -----------------------------
st.markdown("<h1>🚀 Networking Platform Search</h1>", unsafe_allow_html=True)
st.markdown(
    "<p class='subtitle'>Search for companies, products, and sellers across the network</p>",
    unsafe_allow_html=True,
)

# -----------------------------
# Load Pipeline
# -----------------------------


@st.cache_resource
def load_query_service():
    return QueryEngineService()


def run_async(coro):
    """Run async coroutine in a new event loop"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("Event loop is closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    try:
        return loop.run_until_complete(coro)
    finally:
        # Don't close the loop, just leave it open
        pass


query_service = load_query_service()

# -----------------------------
# User Inputs
# -----------------------------
with st.container():
    query_text = st.text_input(
        "🔎 Enter your search query:",
        placeholder="e.g. Laser cutting systems or companies in Bangalore",
        label_visibility="collapsed",
    )

    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        entity_types = ["All", "Company", "Product", "Seller"]
        selected_entity = st.selectbox("🏷️ Filter by entity type:", entity_types)

    with col2:
        n_results = st.slider("Number of results:", 1, 20, 10)

    with col3:
        score_threshold = st.slider(
            "Score threshold:", 0.0, 1.0, 0.5, step=0.1, format="%.1f"
        )

    search_button = st.button("🔍 Search", use_container_width=True)

# -----------------------------
# Search Execution
# -----------------------------
if search_button:
    if not query_text.strip():
        st.warning("⚠️ Please enter a valid query.")
    else:
        with st.spinner("Searching the network..."):
            try:
                # Build metadata filters
                metadata_filters = None
                if selected_entity != "All":
                    metadata_filters = MetadataFilters(
                        filters=[
                            ExactMatchFilter(
                                key="entity", value=selected_entity.lower()
                            )
                        ]
                    )

                # Perform retrieval
                retrieval_result = run_async(
                    query_service.retrieve(
                        query=query_text,
                        top_k=n_results,
                        metadata_filters=metadata_filters,
                    )
                )

                nodes = retrieval_result.nodes

                if not nodes:
                    st.info("No results found. Try adjusting your query or filters.")
                else:
                    # Display results
                    st.success(
                        f"Found {len(nodes)} results for '{query_text}'"
                        + (
                            f" (filtered by: {selected_entity})"
                            if selected_entity != "All"
                            else ""
                        )
                    )

                    # Info about relevance scores in semantic search
                    st.info(
                        "💡 **Understanding Relevance Scores:** In semantic search: "
                        "40-50% = Moderate relevance, "
                        "50-70% = Good relevance, "
                        "70%+ = High relevance. "
                        "Even moderate scores can be relevant due to semantic understanding."
                    )

                    for idx, node in enumerate(nodes, start=1):
                        # Get entity type
                        entity = node.metadata.get("entity", "unknown")
                        raw_score = node.score if hasattr(node, "score") else 0.0

                        # Convert similarity score (0-1) to percentage
                        similarity_percentage = raw_score * 100

                        # Determine relevance level for better UX
                        if similarity_percentage >= 70:
                            relevance = "🔥 High"
                            score_color = "#4caf50"
                        elif similarity_percentage >= 50:
                            relevance = "✓ Medium"
                            score_color = "#ff9800"
                        else:
                            relevance = "- Low"
                            score_color = "#999"

                        # Determine entity badge style
                        badge_class = f"entity-{entity}"
                        entity_label = entity.capitalize()

                        with st.container():
                            st.markdown(
                                f"""
                                <div class='result-card'>
                                    <span class='entity-badge {badge_class}'>{idx}. {entity_label}</span>
                                    <span class='similarity'>Relevance: {similarity_percentage:.1f}% ({relevance})</span>
                                </div>
                                """,
                                unsafe_allow_html=True,
                            )

                            # Display text content
                            with st.expander(f"View details", expanded=False):
                                st.markdown(f"**Content:**\n\n{node.text}")

                                # Display metadata
                                if node.metadata:
                                    st.markdown("**Metadata:**")
                                    metadata_display = {
                                        k: v
                                        for k, v in node.metadata.items()
                                        if k
                                        not in [
                                            "_node_type",
                                            "_node_content",
                                            "id_",
                                            "ref_doc_id",
                                            "doc_id",
                                            "excluded_embed_metadata_keys",
                                            "excluded_llm_metadata_keys",
                                        ]
                                    }
                                    st.json(metadata_display)

            except Exception as e:
                st.error(f"❌ Error while querying: {e}")
                import traceback

                st.code(traceback.format_exc())
