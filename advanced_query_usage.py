"""
Advanced Query Service Usage Examples
Demonstrates granular control over retrieval pipeline
"""

import asyncio

from llama_index.core.vector_stores import ExactMatchFilter, MetadataFilters

from src.pipelines.advanced_query import AdvancedQueryPipeline


async def example_1_simple_query():
    """Example 1: Simple one-call query (easiest)"""
    print("\n" + "=" * 70)
    print("EXAMPLE 1: Simple Query (One Call)")
    print("=" * 70)

    pipeline = AdvancedQueryPipeline()

    # Single method call - everything automated
    result = await pipeline.query(
        query="Find experts in Bangalore with Engineering skills and provide the details of the person and summary of the person",
        top_k=10,
        score_threshold=0.5,
        rerank=False,
    )

    print(f"\n📊 Results:")
    print(f"   Retrieved: {result.retrieval_result.total_retrieved} nodes")
    print(f"   After filtering: {len(result.filter_result.filtered_nodes)} nodes")
    print(f"   Filter stats: {result.filter_result.filter_stats}")
    print(f"   Total time: {result.total_time:.2f}s")
    print(f"\n💬 Answer:\n{result.get_answer()}\n")


async def example_2_granular_control():
    """Example 2: Step-by-step control (maximum flexibility)"""
    print("\n" + "=" * 70)
    print("EXAMPLE 2: Granular Control (Step by Step)")
    print("=" * 70)

    pipeline = AdvancedQueryPipeline()
    query = "Find experts skilled in Human Resources"

    # Step 1: Retrieve
    print("\n📥 STEP 1: Retrieval")
    retrieval = await pipeline.retrieve(query=query, top_k=15)
    print(f"   Retrieved: {retrieval.total_retrieved} nodes")
    print(f"   Time: {retrieval.retrieval_time:.2f}s")

    # Step 2: Filter
    print("\n🔍 STEP 2: Filtering")
    filtered = pipeline.filter_nodes(
        nodes=retrieval.nodes,
        score_threshold=0.6,
        custom_filter=lambda n: "Human Resources"
        in str(n.metadata.get("category1", "")),
    )
    print(f"   Filtered: {len(filtered.filtered_nodes)} nodes")
    print(f"   Removed: {len(retrieval.nodes) - len(filtered.filtered_nodes)} nodes")
    print(f"   Filter stats: {filtered.filter_stats}")

    # Step 3: Re-rank (optional)
    print("\n🔄 STEP 3: Re-ranking")
    reranked = await pipeline.rerank_nodes(
        query=query, nodes=filtered.filtered_nodes, strategy="llm", top_n=5
    )
    print(f"   Re-ranked to: {len(reranked.reranked_nodes)} nodes")
    print(f"   Strategy: {reranked.strategy_used}")
    print(f"   Time: {reranked.rerank_time:.2f}s")

    # Step 4: Generate response
    print("\n✨ STEP 4: Response Generation")
    synthesis = await pipeline.synthesize_response(
        query=query, nodes=reranked.reranked_nodes, response_mode="compact"
    )
    print(f"   Tokens used: {synthesis.llm_usage.total_tokens}")
    print(f"   Time: {synthesis.synthesis_time:.2f}s")
    print(f"\n💬 Answer:\n{synthesis.response}\n")


async def example_3_compare_strategies():
    """Example 3: Compare different retrieval strategies"""
    print("\n" + "=" * 70)
    print("EXAMPLE 3: Compare Score Thresholds & Re-ranking")
    print("=" * 70)

    pipeline = AdvancedQueryPipeline()
    query = "Find experts in Banking"

    # Strategy A: Low threshold, no rerank
    print("\n🔵 Strategy A: Low threshold (0.3), No re-ranking")
    result_a = await pipeline.query(query, top_k=10, score_threshold=0.3, rerank=False)
    print(f"   Final nodes: {len(result_a.synthesis_result.source_nodes)}")
    print(f"   Tokens: {result_a.synthesis_result.llm_usage.total_tokens}")

    # Strategy B: High threshold, no rerank
    print("\n🟢 Strategy B: High threshold (0.7), No re-ranking")
    result_b = await pipeline.query(query, top_k=10, score_threshold=0.7, rerank=False)
    print(f"   Final nodes: {len(result_b.synthesis_result.source_nodes)}")
    print(f"   Tokens: {result_b.synthesis_result.llm_usage.total_tokens}")

    # Strategy C: Medium threshold + LLM rerank
    print("\n🟡 Strategy C: Medium threshold (0.5), LLM re-ranking")
    result_c = await pipeline.query(
        query, top_k=15, score_threshold=0.5, rerank=True, rerank_strategy="llm"
    )
    print(f"   Final nodes: {len(result_c.synthesis_result.source_nodes)}")
    print(f"   Tokens: {result_c.synthesis_result.llm_usage.total_tokens}")
    if result_c.rerank_result:
        print(f"   Re-rank time: {result_c.rerank_result.rerank_time:.2f}s")

    print("\n📊 Comparison:")
    print(
        f"   Strategy A: {len(result_a.synthesis_result.source_nodes)} nodes, "
        f"{result_a.total_time:.2f}s"
    )
    print(
        f"   Strategy B: {len(result_b.synthesis_result.source_nodes)} nodes, "
        f"{result_b.total_time:.2f}s"
    )
    print(
        f"   Strategy C: {len(result_c.synthesis_result.source_nodes)} nodes, "
        f"{result_c.total_time:.2f}s"
    )


async def example_4_metadata_filters():
    """Example 4: Using metadata filters"""
    print("\n" + "=" * 70)
    print("EXAMPLE 4: Metadata Filtering")
    print("=" * 70)

    pipeline = AdvancedQueryPipeline()

    # Create metadata filters
    filters = MetadataFilters(
        filters=[
            ExactMatchFilter(key="state", value="Karnataka"),
            # Can add more filters here
        ]
    )

    result = await pipeline.query(
        query="Find experts with Engineering experience",
        top_k=10,
        metadata_filters=filters,
        score_threshold=0.5,
    )

    print(f"\n📍 Filtered by: state = Karnataka")
    print(f"   Retrieved: {result.retrieval_result.total_retrieved} nodes")
    print(f"   After filtering: {len(result.filter_result.filtered_nodes)} nodes")
    print(f"\n💬 Answer:\n{result.get_answer()}\n")


async def example_5_custom_postprocessor():
    """Example 5: Custom post-processor"""
    print("\n" + "=" * 70)
    print("EXAMPLE 5: Custom Post-Processor")
    print("=" * 70)

    pipeline = AdvancedQueryPipeline()

    # Define custom post-processor to boost senior experts
    def boost_senior_experts(nodes):
        """Boost experts with >5 years experience"""
        for node in nodes:
            try:
                exp = node.metadata.get("Experience_Years", "0")
                # Handle both string and numeric
                exp_years = int(float(str(exp)))
                if exp_years > 5:
                    node.score *= 1.2  # 20% boost
            except (ValueError, TypeError):
                pass
        return sorted(nodes, key=lambda x: x.score, reverse=True)

    # Add custom post-processor after filtering
    pipeline.add_postprocessor(boost_senior_experts, stage="post_filter")

    result = await pipeline.query(
        query="Find experienced consultants", top_k=10, score_threshold=0.5
    )

    print(f"\n⚡ Custom processor applied: Boosted senior experts (>5 years)")
    print(f"   Final nodes: {len(result.synthesis_result.source_nodes)}")

    # Show source nodes with experience
    print(f"\n📋 Top sources:")
    for i, node in enumerate(result.synthesis_result.source_nodes[:5], 1):
        name = node.metadata.get("full_name", "Unknown")
        exp = node.metadata.get("Experience_Years", "N/A")
        print(f"   {i}. {name} - {exp} years (score: {node.score:.3f})")


async def main():
    """Run all examples"""
    print("\n🚀 Advanced Query Service - Usage Examples\n")

    # Run examples (comment out those you don't want)
    await example_1_simple_query()
    # await example_2_granular_control()
    # await example_3_compare_strategies()
    # await example_4_metadata_filters()
    # await example_5_custom_postprocessor()

    print("\n✅ All examples completed!\n")


if __name__ == "__main__":
    asyncio.run(main())
