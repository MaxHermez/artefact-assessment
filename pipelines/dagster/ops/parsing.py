"""
Parsing operations for extracting destinations and offerings from review tags.
"""

import json
from typing import Dict, List

from psycopg2.extras import execute_batch

from dagster import (
    op,
    In,
    Out,
    OpExecutionContext,
    get_dagster_logger,
    AssetMaterialization,
)

from config import get_db_connection


@op(
    description="Load reviews from database for parsing",
    out=Out(List[Dict], description="Reviews that need parsing"),
)
def load_reviews_for_parsing(context: OpExecutionContext) -> List[Dict]:
    """Load reviews that haven't been parsed yet (destination_id or offering_id is NULL)."""
    logger = get_dagster_logger()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT id, raw_tags
            FROM reviews
            WHERE (destination_id IS NULL OR offering_id IS NULL)
            AND raw_tags IS NOT NULL
        """)
        
        reviews = []
        for row in cursor.fetchall():
            reviews.append({
                'id': row[0],
                'raw_tags': row[1]
            })
        
        logger.info(f"Loaded {len(reviews)} reviews for parsing")
        
        context.log_event(
            AssetMaterialization(
                asset_key="unparsed_reviews",
                description="Loaded reviews requiring tag parsing",
                metadata={
                    "reviews_to_parse": len(reviews),
                }
            )
        )
        
        return reviews
        
    except Exception as e:
        logger.error(f"Failed to load reviews for parsing: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


@op(
    description="Parse tags and extract destination/offering mappings",
    ins={
        "reviews": In(List[Dict]),
        "mappings": In(Dict),
        "destination_map": In(Dict[str, int]),
        "offering_map": In(Dict[str, int]),
    },
    out=Out(List[Dict], description="Reviews with parsed destination and offering IDs"),
)
def parse_review_tags(
    context: OpExecutionContext,
    reviews: List[Dict],
    mappings: Dict,
    destination_map: Dict[str, int],
    offering_map: Dict[str, int],
) -> List[Dict]:
    """Parse tags and extract destination/offering for reviews."""
    logger = get_dagster_logger()
    
    tags_mapping = mappings.get('tags_mapping', {})
    parsed_reviews = []
    
    for review in reviews:
        try:
            tags_list = json.loads(review['raw_tags']) if isinstance(review['raw_tags'], str) else review['raw_tags']
            
            destination_id = None
            offering_id = None
            
            if tags_list and len(tags_list) > 0:
                first_tag = tags_list[0]
                tag_value = first_tag.get('value') if isinstance(first_tag, dict) else None
                
                if tag_value and tag_value in tags_mapping:
                    mapping_values = tags_mapping[tag_value]
                    if len(mapping_values) >= 2:
                        offering_name = mapping_values[0]
                        destination_name = mapping_values[1]
                        
                        offering_id = offering_map.get(offering_name)
                        destination_id = destination_map.get(destination_name)
            
            parsed_reviews.append({
                'id': review['id'],
                'destination_id': destination_id,
                'offering_id': offering_id,
            })
            
        except Exception as e:
            logger.warning(f"Error parsing review {review.get('id')}: {str(e)}")
            continue
    
    logger.info(f"Successfully parsed {len(parsed_reviews)} reviews")
    
    # Calculate parsing statistics
    reviews_with_destination = sum(1 for r in parsed_reviews if r['destination_id'] is not None)
    reviews_with_offering = sum(1 for r in parsed_reviews if r['offering_id'] is not None)
    reviews_fully_parsed = sum(1 for r in parsed_reviews if r['destination_id'] is not None and r['offering_id'] is not None)
    
    context.log_event(
        AssetMaterialization(
            asset_key="parsed_reviews",
            description="Extracted destinations and offerings from review tags",
            metadata={
                "reviews_parsed": len(parsed_reviews),
                "with_destination": reviews_with_destination,
                "with_offering": reviews_with_offering,
                "fully_parsed": reviews_fully_parsed,
                "parse_success_rate": f"{(reviews_fully_parsed / len(parsed_reviews) * 100):.2f}%" if len(parsed_reviews) > 0 else "N/A",
            }
        )
    )
    
    return parsed_reviews


@op(
    description="Update reviews with parsed destination and offering IDs",
    ins={"parsed_reviews": In(List[Dict])},
    out=Out(int, description="Number of reviews updated"),
)
def update_parsed_reviews(context: OpExecutionContext, parsed_reviews: List[Dict]) -> int:
    """Update reviews in the database with parsed destination and offering IDs."""
    logger = get_dagster_logger()
    
    if not parsed_reviews:
        logger.warning("No parsed reviews to update")
        return 0
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        update_query = """
            UPDATE reviews
            SET destination_id = %(destination_id)s,
                offering_id = %(offering_id)s,
                updated_at = NOW()
            WHERE id = %(id)s
        """
        
        execute_batch(cursor, update_query, parsed_reviews, page_size=1000)
        conn.commit()
        
        updated_count = len(parsed_reviews)
        logger.info(f"Successfully updated {updated_count} reviews")
        
        context.log_event(
            AssetMaterialization(
                asset_key="reviews_parsed",
                description="Updated reviews with parsed destination and offering IDs",
                metadata={
                    "reviews_updated": updated_count,
                    "batch_size": 1000,
                }
            )
        )
        
        # Materialize the reviews_table asset
        context.log_event(
            AssetMaterialization(
                asset_key=["supabase", "reviews_table"],
                description="Reviews table updated by parsing pipeline",
                metadata={
                    "rows_updated": updated_count,
                }
            )
        )
        
        return updated_count
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to update parsed reviews: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


@op(
    description="Refresh materialized views for analytics",
    out=Out(bool, description="Success status"),
)
def refresh_materialized_views(context: OpExecutionContext) -> bool:
    """Refresh materialized views to update aggregated statistics."""
    logger = get_dagster_logger()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT refresh_review_summary_stats()")
        conn.commit()
        logger.info("Successfully refreshed materialized views")
        
        context.log_event(
            AssetMaterialization(
                asset_key="materialized_views",
                description="Refreshed materialized views for analytics",
                metadata={
                    "status": "success",
                }
            )
        )
        
        return True
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to refresh materialized views: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
