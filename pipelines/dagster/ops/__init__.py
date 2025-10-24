"""
Dagster ops for tourism data pipeline.
"""

from ops.ingestion import (
    load_dataset_from_gcs,
    load_mappings_from_gcs,
    transform_reviews_basic,
    load_reviews_to_supabase,
    upsert_destinations,
    upsert_offerings,
)
from ops.parsing import (
    load_reviews_for_parsing,
    parse_review_tags,
    update_parsed_reviews,
    refresh_materialized_views,
)
from ops.translation import (
    load_reviews_for_translation,
    translate_reviews,
    update_translated_reviews,
)
from ops.aspect_extraction import (
    load_reviews_for_aspects,
    extract_aspects_batch_via_api,
    extract_aspects_batch_via_pyabsa,
    upsert_review_aspects,
    generate_batch_numbers,
    process_batch_end_to_end,
    process_batch_end_to_end_pyabsa,
)

__all__ = [
    # Ingestion ops
    "load_dataset_from_gcs",
    "load_mappings_from_gcs",
    "transform_reviews_basic",
    "load_reviews_to_supabase",
    "upsert_destinations",
    "upsert_offerings",
    # Parsing ops
    "load_reviews_for_parsing",
    "parse_review_tags",
    "update_parsed_reviews",
    "refresh_materialized_views",
    # Translation ops
    "load_reviews_for_translation",
    "translate_reviews",
    "update_translated_reviews",
    # Aspect ops
    "load_reviews_for_aspects",
    "extract_aspects_batch_via_api",
    "extract_aspects_batch_via_pyabsa",
    "upsert_review_aspects",
    "generate_batch_numbers",
    "process_batch_end_to_end",
    "process_batch_end_to_end_pyabsa",
]
