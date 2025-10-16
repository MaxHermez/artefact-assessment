"""
Dagster Repository for Tourism Data Pipeline

Automatically sets up GCS Pub/Sub notifications on load.
"""

from dagster import repository

from jobs import (
    ingestion_pipeline,
    parsing_pipeline,
    translation_pipeline,
)
from sensors import gcs_ingestion_sensor, gcs_parsing_sensor
from config import ensure_gcs_notifications_setup

# Auto-setup GCS notifications when repository loads (idempotent)
ensure_gcs_notifications_setup()


@repository
def tourism_data_repository():
    """
    Repository containing all jobs and sensors for the tourism data pipeline:
    
    Jobs:
    - ingestion_pipeline: Load raw data from GCS to Supabase
    - parsing_pipeline: Extract destinations and offerings from tags
    - translation_pipeline: Translate non-English reviews using OpenAI
    
    Sensors:
    - gcs_ingestion_sensor: Monitors GCS for new dataset files
    - gcs_parsing_sensor: Monitors GCS for new mapping files
    """
    return [
        ingestion_pipeline,
        parsing_pipeline,
        translation_pipeline,
        gcs_ingestion_sensor,
        gcs_parsing_sensor,
    ]
