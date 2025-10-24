"""
Asset definitions for database tables in the tourism data pipeline.

These assets represent the actual tables in Supabase PostgreSQL and provide
observability, lineage tracking, and metadata about the data artifacts.
"""

from typing import Dict, Any
from dagster import asset, Output, AssetExecutionContext
from config import get_db_connection


@asset(
    key_prefix=["supabase"],
    description="Reviews table containing tourism review data with content, ratings, and metadata",
    compute_kind="postgres",
    group_name="core_tables",
)
def reviews_table(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Asset representing the reviews table.
    
    This is a "source" asset that tracks the state of the reviews table.
    The table is populated by the ingestion pipeline, updated by parsing and translation pipelines.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Get table stats
            cur.execute("""
                SELECT 
                    COUNT(*) as total_reviews,
                    COUNT(CASE WHEN translated_content IS NOT NULL THEN 1 END) as translated_count,
                    COUNT(CASE WHEN destination_id IS NOT NULL THEN 1 END) as parsed_count,
                    COUNT(DISTINCT language) as language_count,
                    MIN(published_date) as oldest_review,
                    MAX(published_date) as newest_review,
                    AVG(rating)::numeric(3,2) as avg_rating
                FROM reviews
            """)
            row = cur.fetchone()
            
            stats = {
                "total_reviews": row[0],
                "translated_count": row[1],
                "parsed_count": row[2],
                "language_count": row[3],
                "oldest_review": str(row[4]) if row[4] else None,
                "newest_review": str(row[5]) if row[5] else None,
                "avg_rating": float(row[6]) if row[6] else None,
            }
            
            context.log.info(f"Reviews table stats: {stats}")
            
            return Output(
                value=stats,
                metadata={
                    "total_reviews": stats["total_reviews"],
                    "translated_count": stats["translated_count"],
                    "parsed_count": stats["parsed_count"],
                    "languages": stats["language_count"],
                    "avg_rating": stats["avg_rating"],
                }
            )
    finally:
        conn.close()


@asset(
    key_prefix=["supabase"],
    description="Destinations reference table containing parsed destination entities",
    compute_kind="postgres",
    group_name="core_tables",
    deps=[reviews_table],
)
def destinations_table(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Asset representing the destinations table.
    
    Populated by the parsing pipeline from review tags.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Get table stats
            cur.execute("""
                SELECT 
                    COUNT(*) as total_destinations,
                    COUNT(CASE WHEN name IS NOT NULL THEN 1 END) as named_destinations,
                    (SELECT COUNT(*) FROM reviews WHERE destination_id IS NOT NULL) as referenced_in_reviews
                FROM destinations
            """)
            row = cur.fetchone()
            
            stats = {
                "total_destinations": row[0],
                "named_destinations": row[1],
                "referenced_in_reviews": row[2],
            }
            
            context.log.info(f"Destinations table stats: {stats}")
            
            return Output(
                value=stats,
                metadata=stats
            )
    finally:
        conn.close()


@asset(
    key_prefix=["supabase"],
    description="Offerings reference table containing parsed offering/activity entities",
    compute_kind="postgres",
    group_name="core_tables",
    deps=[reviews_table],
)
def offerings_table(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Asset representing the offerings table.
    
    Populated by the parsing pipeline from review tags.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Get table stats
            cur.execute("""
                SELECT 
                    COUNT(*) as total_offerings,
                    COUNT(CASE WHEN name IS NOT NULL THEN 1 END) as named_offerings,
                    (SELECT COUNT(*) FROM reviews WHERE offering_id IS NOT NULL) as referenced_in_reviews
                FROM offerings
            """)
            row = cur.fetchone()
            
            stats = {
                "total_offerings": row[0],
                "named_offerings": row[1],
                "referenced_in_reviews": row[2],
            }
            
            context.log.info(f"Offerings table stats: {stats}")
            
            return Output(
                value=stats,
                metadata=stats
            )
    finally:
        conn.close()


@asset(
    key_prefix=["supabase"],
    description="Review aspects table containing extracted aspects with evidence spans and metadata",
    compute_kind="postgres",
    group_name="core_tables",
    deps=[reviews_table],
)
def review_aspects_table(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Asset representing the review_aspect_extractions table.
    
    Populated by the aspect extraction pipeline (LLM or PyABSA approach).
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Get table stats
            cur.execute("""
                SELECT 
                    COUNT(*) as total_aspects,
                    COUNT(DISTINCT review_id) as reviews_with_aspects,
                    COUNT(DISTINCT aspect) as unique_aspects,
                    COUNT(CASE WHEN approach = 'llm' THEN 1 END) as llm_aspects,
                    COUNT(CASE WHEN approach = 'pyabsa' THEN 1 END) as pyabsa_aspects,
                    AVG(confidence) as avg_confidence,
                    COUNT(CASE WHEN polarity = 'positive' THEN 1 END) as positive_aspects,
                    COUNT(CASE WHEN polarity = 'negative' THEN 1 END) as negative_aspects,
                    COUNT(CASE WHEN polarity = 'neutral' THEN 1 END) as neutral_aspects,
                    (SELECT COUNT(*) FROM reviews) as total_reviews
                FROM review_aspect_extractions
            """)
            row = cur.fetchone()
            
            total_reviews = row[9] or 0
            reviews_with_aspects = row[1] or 0
            coverage = (reviews_with_aspects / total_reviews * 100) if total_reviews > 0 else 0
            
            stats = {
                "total_aspects": row[0],
                "reviews_with_aspects": reviews_with_aspects,
                "unique_aspects": row[2],
                "llm_aspects": row[3],
                "pyabsa_aspects": row[4],
                "avg_confidence": float(row[5]) if row[5] else None,
                "positive_aspects": row[6],
                "negative_aspects": row[7],
                "neutral_aspects": row[8],
                "coverage_percent": round(coverage, 2),
            }
            
            context.log.info(f"Review aspects table stats: {stats}")
            
            return Output(
                value=stats,
                metadata={
                    **stats,
                    "avg_aspects_per_review": round(stats["total_aspects"] / reviews_with_aspects, 2) if reviews_with_aspects > 0 else 0,
                }
            )
    finally:
        conn.close()
