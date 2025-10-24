"""
Ingestion operations for loading data from GCS to Supabase.
"""

import os
import json
import ast
from typing import Dict, List

import pandas as pd
from psycopg2.extras import execute_batch

from dagster import (
    op,
    In,
    Out,
    OpExecutionContext,
    get_dagster_logger,
    Field,
    String,
    AssetMaterialization,
)

from config import get_gcs_client, get_db_connection


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
        
        context.log_event(
            AssetMaterialization(
                asset_key="raw_dataset",
                description="Loaded raw tourism reviews dataset from GCS",
                metadata={
                    "rows": len(df),
                    "columns": len(df.columns),
                    "shape": str(df.shape),
                    "source": f"gs://{bucket_name}/{file_path}",
                }
            )
        )
        
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
        
        context.log_event(
            AssetMaterialization(
                asset_key="tags_mappings",
                description="Loaded tags to destination/offering mappings from GCS",
                metadata={
                    "tag_entries": len(mappings.get('tags_mapping', {})),
                    "source": f"gs://{bucket_name}/{file_path}",
                }
            )
        )
        
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
        
        context.log_event(
            AssetMaterialization(
                asset_key="destinations",
                description="Upserted destination reference data",
                metadata={
                    "unique_destinations": len(destination_map),
                    "destinations_list": sorted(destination_map.keys())[:10],  # First 10 for preview
                }
            )
        )
        
        # Materialize the destinations_table asset
        context.log_event(
            AssetMaterialization(
                asset_key=["supabase", "destinations_table"],
                description="Destinations table updated by parsing pipeline",
                metadata={
                    "rows_upserted": len(destination_map),
                }
            )
        )
        
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
        
        context.log_event(
            AssetMaterialization(
                asset_key="offerings",
                description="Upserted offering/category reference data",
                metadata={
                    "unique_offerings": len(offering_map),
                    "offerings_list": sorted(offering_map.keys())[:10],  # First 10 for preview
                }
            )
        )
        
        # Materialize the offerings_table asset
        context.log_event(
            AssetMaterialization(
                asset_key=["supabase", "offerings_table"],
                description="Offerings table updated by parsing pipeline",
                metadata={
                    "rows_upserted": len(offering_map),
                }
            )
        )
        
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
    
    context.log_event(
        AssetMaterialization(
            asset_key="transformed_reviews",
            description="Transformed raw CSV data to review records",
            metadata={
                "records_transformed": len(transformed_reviews),
                "transformation_errors": errors,
                "success_rate": f"{(len(transformed_reviews) / (len(transformed_reviews) + errors) * 100):.2f}%" if (len(transformed_reviews) + errors) > 0 else "N/A",
            }
        )
    )
    
    return transformed_reviews


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
        
        context.log_event(
            AssetMaterialization(
                asset_key="reviews_ingested",
                description="Loaded reviews into Supabase database",
                metadata={
                    "reviews_loaded": inserted_count,
                    "batch_size": 1000,
                }
            )
        )
        
        # Materialize the reviews_table asset
        context.log_event(
            AssetMaterialization(
                asset_key=["supabase", "reviews_table"],
                description="Reviews table updated by ingestion pipeline",
                metadata={
                    "rows_inserted": inserted_count,
                }
            )
        )
        
        return inserted_count
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to load reviews to Supabase: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
