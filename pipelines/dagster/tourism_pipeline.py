"""
Tourism Data Pipeline - Modular pipelines for data ingestion, parsing, and translation

This module contains three separate pipelines:
1. Ingestion Pipeline: Loads raw data from GCS to Supabase
2. Parsing Pipeline: Extracts destinations and offerings from tags
3. Translation Pipeline: Translates non-English reviews to English using OpenAI
"""

import os
import json
import ast
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from google.cloud import storage
import psycopg2
from psycopg2.extras import execute_batch
from openai import OpenAI

from dagster import (
    op,
    job,
    In,
    Out,
    Output,
    OpExecutionContext,
    get_dagster_logger,
    DynamicOut,
    DynamicOutput,
    Field,
    String,
    Int,
)


# ============================================================================
# Configuration and Utilities
# ============================================================================

def get_gcs_client():
    """Initialize GCS client with credentials from environment."""
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '/opt/dagster/app/gcs-credentials.json')
    if credentials_path and os.path.exists(credentials_path):
        return storage.Client.from_service_account_json(credentials_path)
    return storage.Client()


def get_db_connection():
    """Create a connection to the Supabase PostgreSQL database."""
    return psycopg2.connect(
        host=os.getenv('SUPABASE_DB_HOST'),
        port=int(os.getenv('SUPABASE_DB_PORT', 6543)),
        database=os.getenv('SUPABASE_DB_NAME', 'postgres'),
        user=os.getenv('SUPABASE_DB_USER'),
        password=os.getenv('SUPABASE_DB_PASSWORD')
    )


# ============================================================================
# Dagster Ops
# ============================================================================

@op(
    description="Load CSV dataset from Google Cloud Storage",
    out=Out(pd.DataFrame, description="Raw tourism reviews dataframe"),
    config_schema={
        "bucket_name": Field(String, description="GCS bucket name"),
        "file_path": Field(String, description="Path to CSV file in bucket", default_value="dataset.csv_(DS_Senior).csv"),
    }
)
def load_dataset_from_gcs(context: OpExecutionContext) -> pd.DataFrame:
    """Load the tourism reviews CSV file from GCS."""
    logger = get_dagster_logger()
    
    bucket_name = context.op_config["bucket_name"]
    file_path = context.op_config["file_path"]
    
    logger.info(f"Loading dataset from gs://{bucket_name}/{file_path}")
    
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        
        # Download to temporary location
        temp_path = f"/tmp/{os.path.basename(file_path)}"
        blob.download_to_filename(temp_path)
        
        # Load CSV
        df = pd.read_csv(temp_path)
        logger.info(f"Successfully loaded {len(df)} rows from GCS")
        
        # Clean up
        os.remove(temp_path)
        
        context.log.info(f"Dataset shape: {df.shape}")
        context.log.info(f"Columns: {df.columns.tolist()}")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to load dataset from GCS: {str(e)}")
        raise


@op(
    description="Load mappings JSON from Google Cloud Storage",
    out=Out(Dict, description="Tags to destination/offering mappings"),
    config_schema={
        "bucket_name": Field(String, description="GCS bucket name"),
        "file_path": Field(String, description="Path to mappings JSON file in bucket", default_value="mappings.json_(DS_Senior).json"),
    }
)
def load_mappings_from_gcs(context: OpExecutionContext) -> Dict:
    """Load the tags mappings JSON file from GCS."""
    logger = get_dagster_logger()
    
    bucket_name = context.op_config["bucket_name"]
    file_path = context.op_config["file_path"]
    
    logger.info(f"Loading mappings from gs://{bucket_name}/{file_path}")
    
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        
        # Download and parse JSON
        mappings_json = blob.download_as_text()
        mappings = json.loads(mappings_json)
        
        logger.info(f"Successfully loaded mappings with {len(mappings.get('tags_mapping', {}))} tag entries")
        
        return mappings
        
    except Exception as e:
        logger.error(f"Failed to load mappings from GCS: {str(e)}")
        raise


@op(
    description="Extract and insert unique destinations into database",
    ins={"mappings": In(Dict)},
    out=Out(Dict[str, int], description="Mapping of destination names to IDs"),
)
def upsert_destinations(context: OpExecutionContext, mappings: Dict) -> Dict[str, int]:
    """Extract unique destinations and insert them into the database."""
    logger = get_dagster_logger()
    
    # Extract unique destinations from mappings
    destinations = set()
    tags_mapping = mappings.get('tags_mapping', {})
    
    for tag_id, values in tags_mapping.items():
        if len(values) >= 2:
            destination = values[1]  # Second element is the destination
            destinations.add(destination)
    
    logger.info(f"Found {len(destinations)} unique destinations")
    
    # Insert into database
    conn = get_db_connection()
    cursor = conn.cursor()
    
    destination_map = {}
    
    try:
        for dest in sorted(destinations):
            cursor.execute(
                """
                INSERT INTO destinations (name)
                VALUES (%s)
                ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id, name
                """,
                (dest,)
            )
            dest_id, dest_name = cursor.fetchone()
            destination_map[dest_name] = dest_id
        
        conn.commit()
        logger.info(f"Successfully upserted {len(destination_map)} destinations")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to upsert destinations: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
    
    return destination_map


@op(
    description="Extract and insert unique offerings into database",
    ins={"mappings": In(Dict)},
    out=Out(Dict[str, int], description="Mapping of offering names to IDs"),
)
def upsert_offerings(context: OpExecutionContext, mappings: Dict) -> Dict[str, int]:
    """Extract unique offerings (categories) and insert them into the database."""
    logger = get_dagster_logger()
    
    # Extract unique offerings from mappings
    offerings = set()
    tags_mapping = mappings.get('tags_mapping', {})
    
    for tag_id, values in tags_mapping.items():
        if len(values) >= 1:
            offering = values[0]  # First element is the offering/category
            offerings.add(offering)
    
    logger.info(f"Found {len(offerings)} unique offerings")
    
    # Insert into database
    conn = get_db_connection()
    cursor = conn.cursor()
    
    offering_map = {}
    
    try:
        for offering in sorted(offerings):
            cursor.execute(
                """
                INSERT INTO offerings (name)
                VALUES (%s)
                ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id, name
                """,
                (offering,)
            )
            offering_id, offering_name = cursor.fetchone()
            offering_map[offering_name] = offering_id
        
        conn.commit()
        logger.info(f"Successfully upserted {len(offering_map)} offerings")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to upsert offerings: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
    
    return offering_map


@op(
    description="Transform raw data into basic review records (without parsing tags)",
    ins={"df": In(pd.DataFrame)},
    out=Out(List[Dict], description="Basic review records for ingestion"),
)
def transform_reviews_basic(
    context: OpExecutionContext,
    df: pd.DataFrame,
) -> List[Dict]:
    """Transform raw CSV data into basic review records without extracting destinations and offerings."""
    logger = get_dagster_logger()
    
    transformed_reviews = []
    errors = 0
    
    for idx, row in df.iterrows():
        try:
            # Parse tags (stored as string representation of list)
            tags_str = row['tags']
            tags_list = ast.literal_eval(tags_str) if isinstance(tags_str, str) else tags_str
            
            # Parse ratings (stored as string representation of dict)
            ratings_str = row['ratings']
            ratings_dict = ast.literal_eval(ratings_str) if isinstance(ratings_str, str) else ratings_str
            
            rating_normalized = ratings_dict.get('normalized') if isinstance(ratings_dict, dict) else None
            rating_raw = ratings_dict.get('raw') if isinstance(ratings_dict, dict) else None
            
            # Parse date
            date_str = row['date']
            date_obj = pd.to_datetime(date_str) if pd.notna(date_str) else None
            
            # Create review record without destination/offering mapping
            review = {
                'id': int(row['id'].split('-')[1]) if '-' in str(row['id']) else hash(str(row['id'])) % (10 ** 15),
                'content': row['content'] if pd.notna(row['content']) else None,
                'date': date_obj,
                'language': row['language'] if pd.notna(row['language']) else None,
                'title': row['title'] if pd.notna(row['title']) else None,
                'rating_normalized': int(rating_normalized) if rating_normalized is not None else None,
                'rating_raw': int(rating_raw) if rating_raw is not None else None,
                'destination_id': None,
                'offering_id': None,
                'raw_tags': json.dumps(tags_list) if tags_list else None,
            }
            
            transformed_reviews.append(review)
            
        except Exception as e:
            errors += 1
            logger.warning(f"Error transforming row {idx}: {str(e)}")
            continue
    
    logger.info(f"Successfully transformed {len(transformed_reviews)} reviews ({errors} errors)")
    
    return transformed_reviews


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
        
        return updated_count
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to update parsed reviews: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


@op(
    description="Load transformed reviews into Supabase database",
    ins={"reviews": In(List[Dict])},
    out=Out(int, description="Number of reviews inserted"),
)
def load_reviews_to_supabase(context: OpExecutionContext, reviews: List[Dict]) -> int:
    """Bulk insert reviews into Supabase PostgreSQL database."""
    logger = get_dagster_logger()
    
    if not reviews:
        logger.warning("No reviews to insert")
        return 0
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Prepare batch insert
        insert_query = """
            INSERT INTO reviews (
                id, content, date, language, title,
                rating_normalized, rating_raw, destination_id,
                offering_id, raw_tags
            )
            VALUES (
                %(id)s, %(content)s, %(date)s, %(language)s, %(title)s,
                %(rating_normalized)s, %(rating_raw)s, %(destination_id)s,
                %(offering_id)s, %(raw_tags)s
            )
            ON CONFLICT (id) DO UPDATE SET
                content = EXCLUDED.content,
                date = EXCLUDED.date,
                language = EXCLUDED.language,
                title = EXCLUDED.title,
                rating_normalized = EXCLUDED.rating_normalized,
                rating_raw = EXCLUDED.rating_raw,
                destination_id = EXCLUDED.destination_id,
                offering_id = EXCLUDED.offering_id,
                raw_tags = EXCLUDED.raw_tags,
                updated_at = NOW()
        """
        
        # Execute batch insert
        execute_batch(cursor, insert_query, reviews, page_size=1000)
        conn.commit()
        
        inserted_count = len(reviews)
        logger.info(f"Successfully loaded {inserted_count} reviews to Supabase")
        
        return inserted_count
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to load reviews to Supabase: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


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
        "model": Field(String, description="OpenAI model to use", default_value="gpt-5-mini"),
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

    model = context.op_config.get("model") or os.getenv('OPENAI_TRANSLATION_MODEL') or "gpt-5-mini"
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
        
        return updated_count
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to update translated reviews: {str(e)}")
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
        return True
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to refresh materialized views: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# Dagster Jobs
# ============================================================================

@job(
    description="Ingestion pipeline: Load raw tourism reviews from GCS to Supabase",
    config={
        "ops": {
            "load_dataset_from_gcs": {
                "config": {
                    "bucket_name": os.getenv('GCS_BUCKET_NAME', 'your-bucket-name'),
                    "file_path": os.getenv('GCS_DATASET_PATH', 'dataset.csv_(DS_Senior).csv'),
                }
            },
        }
    }
)
def ingestion_pipeline():
    """
    Simple ingestion pipeline that loads raw data from GCS to Supabase:
    1. Load CSV dataset from GCS
    2. Transform to basic review records
    3. Load reviews into Supabase (without destination/offering mapping)
    """
    dataset = load_dataset_from_gcs()
    transformed_reviews = transform_reviews_basic(dataset)
    reviews_count = load_reviews_to_supabase(transformed_reviews)


@job(
    description="Parsing pipeline: Extract destinations and offerings from review tags",
    config={
        "ops": {
            "load_mappings_from_gcs": {
                "config": {
                    "bucket_name": os.getenv('GCS_BUCKET_NAME', 'your-bucket-name'),
                    "file_path": os.getenv('GCS_MAPPINGS_PATH', 'mappings.json_(DS_Senior).json'),
                }
            },
        }
    }
)
def parsing_pipeline():
    """
    Parsing pipeline that extracts structured data from tags:
    1. Load mappings JSON from GCS
    2. Upsert destinations and offerings reference tables
    3. Load reviews that need parsing
    4. Parse tags and extract destination/offering mappings
    5. Update reviews with parsed data
    6. Refresh materialized views
    """
    mappings = load_mappings_from_gcs()
    
    # Upsert reference data (can run in parallel)
    destination_map = upsert_destinations(mappings)
    offering_map = upsert_offerings(mappings)
    
    # Load and parse reviews
    reviews = load_reviews_for_parsing()
    parsed_reviews = parse_review_tags(
        reviews=reviews,
        mappings=mappings,
        destination_map=destination_map,
        offering_map=offering_map,
    )
    
    updated_count = update_parsed_reviews(parsed_reviews)
    refresh_materialized_views()


@job(
    description="Translation pipeline: Translate non-English reviews to English using OpenAI",
    config={
        "ops": {
            "load_reviews_for_translation": {
                "config": {
                    "batch_size": int(os.getenv('TRANSLATION_BATCH_SIZE', '100')),
                }
            },
            "translate_reviews": {
                "config": {
                    "model": os.getenv('OPENAI_TRANSLATION_MODEL', 'gpt-5-mini'),
                }
            },
        }
    }
)
def translation_pipeline():
    """
    Translation pipeline that translates non-English reviews in batches:
    1. Load all non-English reviews from database (paginated)
    2. Translate each batch of reviews using OpenAI (in parallel)
    3. Update reviews with translations
    
    This pipeline processes ALL non-English reviews by paginating through them in batches.
    """
    review_batches = load_reviews_for_translation()
    translated_batches = review_batches.map(translate_reviews)
    translated_batches.map(update_translated_reviews)


# Legacy pipeline for backwards compatibility
@job(
    description="[LEGACY] Full ETL pipeline - use separate pipelines instead",
    config={
        "ops": {
            "load_dataset_from_gcs": {
                "config": {
                    "bucket_name": os.getenv('GCS_BUCKET_NAME', 'your-bucket-name'),
                    "file_path": os.getenv('GCS_DATASET_PATH', 'dataset.csv_(DS_Senior).csv'),
                }
            },
            "load_mappings_from_gcs": {
                "config": {
                    "bucket_name": os.getenv('GCS_BUCKET_NAME', 'your-bucket-name'),
                    "file_path": os.getenv('GCS_MAPPINGS_PATH', 'mappings.json_(DS_Senior).json'),
                }
            },
        }
    }
)
def tourism_etl_pipeline():
    """
    Legacy full ETL pipeline (kept for backwards compatibility).
    Consider using the separate ingestion_pipeline and parsing_pipeline instead.
    """
    dataset = load_dataset_from_gcs()
    transformed_reviews = transform_reviews_basic(dataset)
    reviews_count = load_reviews_to_supabase(transformed_reviews)
    
    # Run parsing immediately after ingestion
    mappings = load_mappings_from_gcs()
    destination_map = upsert_destinations(mappings)
    offering_map = upsert_offerings(mappings)
    
    reviews = load_reviews_for_parsing()
    parsed_reviews = parse_review_tags(
        reviews=reviews,
        mappings=mappings,
        destination_map=destination_map,
        offering_map=offering_map,
    )
    
    updated_count = update_parsed_reviews(parsed_reviews)
    refresh_materialized_views()
