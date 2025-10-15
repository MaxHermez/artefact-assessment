"""
Dagster Repository for Tourism Data Pipeline
"""

from dagster import repository

from tourism_pipeline import (
    ingestion_pipeline,
    parsing_pipeline,
    translation_pipeline,
    tourism_etl_pipeline,  # Legacy pipeline
)


@repository
def tourism_data_repository():
    """
    Repository containing all jobs for the tourism data pipeline:
    - ingestion_pipeline: Load raw data from GCS to Supabase
    - parsing_pipeline: Extract destinations and offerings from tags
    - translation_pipeline: Translate non-English reviews using OpenAI
    - tourism_etl_pipeline: Legacy full pipeline (for backwards compatibility)
    """
    return [
        ingestion_pipeline,
        parsing_pipeline,
        translation_pipeline,
        tourism_etl_pipeline,
    ]
