import os

from dagster import job

from ops import (
    # Ingestion
    load_dataset_from_gcs,
    load_mappings_from_gcs,
    transform_reviews_basic,
    load_reviews_to_supabase,
    upsert_destinations,
    upsert_offerings,
    # Parsing
    load_reviews_for_parsing,
    parse_review_tags,
    update_parsed_reviews,
    refresh_materialized_views,
    # Translation
    load_reviews_for_translation,
    translate_reviews,
    update_translated_reviews,
    # Aspect Extraction
    load_reviews_for_aspects,
    extract_aspects_batch_via_api,
    extract_aspects_batch_via_pyabsa,
    upsert_review_aspects,
    generate_batch_numbers,
    process_batch_end_to_end,
    process_batch_end_to_end_pyabsa,
)


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
    Ingestion pipeline that loads raw data from GCS to Supabase:
    1. Load CSV dataset from GCS
    2. Transform to basic review records
    3. Load reviews into Supabase (without destination/offering mapping)
    """
    dataset = load_dataset_from_gcs()
    transformed_reviews = transform_reviews_basic(dataset)
    load_reviews_to_supabase(transformed_reviews)


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
    
    update_parsed_reviews(parsed_reviews)
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


@job(
    description="Aspect extraction pipeline: call external API and persist aspects",
    config={
        "ops": {
            "generate_batch_numbers": {
                "config": {
                    "batch_size": int(os.getenv('ASPECT_BATCH_SIZE', '200')),
                }
            },
            "process_batch_end_to_end": {
                "config": {
                    "batch_size": int(os.getenv('ASPECT_BATCH_SIZE', '200')),
                }
            },
        }
    }
)
def aspect_extraction_pipeline():
    """
    Aspect extraction pipeline using dynamic mapping for end-to-end batch processing.
    
    Each batch is processed as a complete, independent, retriable unit:
    1. Generate batch numbers based on reviews needing extraction
    2. For each batch (dynamically mapped):
       - Select reviews from database (with offset)
       - Call aspect extraction API
       - Upsert extracted aspects
    
    Benefits:
    - Each batch is independently retriable if it fails
    - Full observability per batch with materialized assets
    - No global state dependencies between select/extract/upsert
    """
    batch_numbers = generate_batch_numbers()
    batch_numbers.map(process_batch_end_to_end)


@job(
    description="Aspect extraction pipeline (PyABSA): call PyABSA batch API and persist aspects",
    config={
        "ops": {
            "generate_batch_numbers": {
                "config": {
                    "batch_size": int(os.getenv('PYABSA_BATCH_SIZE', '200')),
                    "approach": "pyabsa",
                }
            },
            "process_batch_end_to_end_pyabsa": {
                "config": {
                    "batch_size": int(os.getenv('PYABSA_BATCH_SIZE', '200')),
                }
            },
        }
    }
)
def pyabsa_aspect_extraction_pipeline():
    """
    Aspect extraction pipeline (PyABSA) using dynamic mapping for end-to-end batch processing.
    
    Each batch is processed as a complete, independent, retriable unit:
    1. Generate batch numbers based on reviews needing extraction
    2. For each batch (dynamically mapped):
       - Select reviews from database (with offset)
       - Call PyABSA aspect extraction API
       - Upsert extracted aspects
    
    Benefits:
    - Each batch is independently retriable if it fails
    - Full observability per batch with materialized assets
    - No global state dependencies between select/extract/upsert
    """
    batch_numbers = generate_batch_numbers()
    batch_numbers.map(process_batch_end_to_end_pyabsa)


@job(
    description="Retry aspect extraction for unprocessed reviews (smaller batches, rate-limited)",
    config={
        "ops": {
            "generate_batch_numbers": {
                "config": {
                    "batch_size": 100,
                }
            },
            "process_batch_end_to_end": {
                "config": {
                    "batch_size": 100,
                }
            },
        }
    }
)
def aspect_extraction_retry_pipeline():
    """
    Retry pipeline for aspect extraction with smaller batches and built-in rate limiting.
    Use this when the main pipeline fails partway through due to Supabase rate limits.
    
    Automatically identifies and processes only reviews that don't have aspects yet.
    Uses smaller batch size (100) and 1-second delays to respect free tier limits.
    Each batch is independently retriable.
    """
    batch_numbers = generate_batch_numbers()
    batch_numbers.map(process_batch_end_to_end)

