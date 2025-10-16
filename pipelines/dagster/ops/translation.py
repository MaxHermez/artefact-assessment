"""
Translation operations for converting non-English reviews to English using OpenAI.
"""

import os
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from psycopg2.extras import execute_batch
from openai import OpenAI

from dagster import (
    op,
    In,
    Out,
    DynamicOut,
    DynamicOutput,
    OpExecutionContext,
    get_dagster_logger,
    Field,
    String,
    Int,
    AssetMaterialization,
)

from config import get_db_connection


@op(
    description="Load all non-English reviews from database in batches",
    out=DynamicOut(List[Dict], description="Batches of non-English reviews to translate"),
    config_schema={
        "batch_size": Field(Int, description="Number of reviews per batch", default_value=100),
    }
)
def load_reviews_for_translation(context: OpExecutionContext):
    """Load all reviews that are not in English and haven't been translated yet, yielding batches."""
    logger = get_dagster_logger()
    
    batch_size = context.op_config["batch_size"]
    offset = 0
    batch_num = 0
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # First, get the total count
        cursor.execute("""
            SELECT COUNT(*)
            FROM reviews
            WHERE language IS NOT NULL
            AND content IS NOT NULL
            AND LOWER(language) NOT IN ('en', 'eng', 'en-us', 'en-gb', 'english')
            AND (translated_content IS NULL OR translated_title IS NULL)
        """)
        total_count = cursor.fetchone()[0]
        logger.info(f"Found {total_count} non-English reviews to translate")
        
        # Load and yield reviews in batches
        while True:
            cursor.execute("""
                SELECT id, content, title, language
                FROM reviews
                WHERE language IS NOT NULL
                AND content IS NOT NULL
                AND LOWER(language) NOT IN ('en', 'eng', 'en-us', 'en-gb', 'english')
                AND (translated_content IS NULL OR translated_title IS NULL)
                ORDER BY id
                LIMIT %s OFFSET %s
            """, (batch_size, offset))
            
            reviews = []
            for row in cursor.fetchall():
                reviews.append({
                    'id': row[0],
                    'content': row[1],
                    'title': row[2],
                    'language': row[3]
                })
            
            if not reviews:
                break
            
            logger.info(f"Loaded batch {batch_num + 1} with {len(reviews)} reviews (offset: {offset})")
            yield DynamicOutput(reviews, mapping_key=f"batch_{batch_num}")
            
            offset += batch_size
            batch_num += 1
        
        logger.info(f"Loaded {batch_num} batches totaling {total_count} reviews for translation")
        
        context.log_event(
            AssetMaterialization(
                asset_key="reviews_for_translation",
                description="Loaded non-English reviews for translation",
                metadata={
                    "total_reviews": total_count,
                    "num_batches": batch_num,
                    "batch_size": batch_size,
                }
            )
        )
        
    except Exception as e:
        logger.error(f"Failed to load reviews for translation: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


@op(
    description="Translate reviews to English using OpenAI",
    ins={"reviews": In(List[Dict])},
    out=Out(List[Dict], description="Reviews with translations"),
    config_schema={
        "model": Field(String, description="OpenAI model to use", default_value="gpt-4o-mini"),
        "api_key": Field(String, description="OpenAI API key (or set OPENAI_API_KEY env var)", is_required=False),
        "max_workers": Field(Int, description="Maximum number of parallel translation workers", default_value=10),
    }
)
def translate_reviews(context: OpExecutionContext, reviews: List[Dict]) -> List[Dict]:
    """Translate review content and titles to English using OpenAI in parallel."""
    logger = get_dagster_logger()
    
    logger.info(f"=== STARTING TRANSLATION OP - Received {len(reviews)} reviews to translate ===")
    
    if not reviews:
        logger.warning("No reviews to translate")
        return []

    model = context.op_config.get("model") or os.getenv('OPENAI_TRANSLATION_MODEL') or "gpt-4o-mini"
    api_key = context.op_config.get("api_key") or os.getenv('OPENAI_API_KEY')
    max_workers = context.op_config.get("max_workers", 10)
    
    logger.info(f"Using model: {model}, max_workers: {max_workers}")
    
    if not api_key:
        raise ValueError("OpenAI API key must be provided via config or OPENAI_API_KEY environment variable")
    
    client = OpenAI(api_key=api_key)
    
    def translate_single_review(review: Dict) -> Optional[Dict]:
        """Translate a single review's content and title."""
        try:
            # Translate content
            translated_content = None
            if review.get('content'):
                content_response = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "You are a translator. Translate the following text to English. Only return the translation, nothing else."},
                        {"role": "user", "content": review['content']}
                    ],
                )
                translated_content = content_response.choices[0].message.content.strip()
            
            # Translate title
            translated_title = None
            if review.get('title'):
                title_response = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "You are a translator. Translate the following text to English. Only return the translation, nothing else."},
                        {"role": "user", "content": review['title']}
                    ],
                )
                translated_title = title_response.choices[0].message.content.strip()
            
            logger.info(f"Translated review {review['id']} from {review.get('language', 'unknown')}")
            
            return {
                'id': review['id'],
                'translated_content': translated_content,
                'translated_title': translated_title,
            }
            
        except Exception as e:
            logger.error(f"Failed to translate review {review.get('id')}: {str(e)}")
            return None
    
    # Translate reviews in parallel
    logger.info(f"Starting parallel translation with ThreadPoolExecutor (max_workers={max_workers})")
    translated_reviews = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        logger.info(f"Submitting {len(reviews)} translation tasks to executor")
        # Submit all translation tasks
        future_to_review = {executor.submit(translate_single_review, review): review for review in reviews}
        
        logger.info(f"Waiting for translation tasks to complete...")
        # Collect results as they complete
        completed = 0
        for future in as_completed(future_to_review):
            result = future.result()
            if result is not None:
                translated_reviews.append(result)
            completed += 1
            if completed % 10 == 0:
                logger.info(f"Progress: {completed}/{len(reviews)} translations completed")
    
    logger.info(f"=== TRANSLATION COMPLETE - Successfully translated {len(translated_reviews)} out of {len(reviews)} reviews using {model} with {max_workers} parallel workers ===")
    
    context.log_event(
        AssetMaterialization(
            asset_key="translated_reviews_batch",
            description="Translated batch of reviews to English using OpenAI",
            metadata={
                "reviews_translated": len(translated_reviews),
                "reviews_attempted": len(reviews),
                "success_rate": f"{(len(translated_reviews) / len(reviews) * 100):.2f}%" if len(reviews) > 0 else "N/A",
                "model": model,
                "max_workers": max_workers,
            }
        )
    )
    
    return translated_reviews


@op(
    description="Update reviews with translations",
    ins={"translated_reviews": In(List[Dict])},
    out=Out(int, description="Number of reviews updated"),
)
def update_translated_reviews(context: OpExecutionContext, translated_reviews: List[Dict]) -> int:
    """Update reviews in the database with translated content."""
    logger = get_dagster_logger()
    
    if not translated_reviews:
        logger.warning("No translated reviews to update")
        return 0
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        update_query = """
            UPDATE reviews
            SET translated_content = %(translated_content)s,
                translated_title = %(translated_title)s,
                updated_at = NOW()
            WHERE id = %(id)s
        """
        
        execute_batch(cursor, update_query, translated_reviews, page_size=100)
        conn.commit()
        
        updated_count = len(translated_reviews)
        logger.info(f"Successfully updated {updated_count} reviews with translations")
        
        context.log_event(
            AssetMaterialization(
                asset_key="reviews_translated",
                description="Updated reviews with English translations",
                metadata={
                    "reviews_updated": updated_count,
                    "batch_size": 100,
                }
            )
        )
        
        return updated_count
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to update translated reviews: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
