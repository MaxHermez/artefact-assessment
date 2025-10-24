"""
Dagster Repository for Tourism Data Pipeline

Automatically sets up GCS Pub/Sub notifications on load.
"""

from dagster import Definitions, load_assets_from_modules
import assets

from jobs import (
    ingestion_pipeline,
    parsing_pipeline,
    translation_pipeline,
    aspect_extraction_pipeline,
    pyabsa_aspect_extraction_pipeline,
    aspect_extraction_retry_pipeline,
)
from sensors import (
    gcs_ingestion_sensor,
    gcs_parsing_sensor,
    ingestion_to_parsing,
    parsing_to_translation,
    translation_to_aspects,
)
from resources import (
    DatabaseResource,
    GCSResource,
    get_database_resource_config,
    get_gcs_resource_config,
)
from config import ensure_gcs_notifications_setup

# Auto-setup GCS notifications when repository loads (idempotent)
ensure_gcs_notifications_setup()


# Define resources
resources_defs = {
    "database": DatabaseResource(**get_database_resource_config()),
    "gcs": GCSResource(**get_gcs_resource_config()),
}

# Definitions object (modern Dagster approach)
defs = Definitions(
    assets=load_assets_from_modules([assets]),
    jobs=[
        ingestion_pipeline,
        parsing_pipeline,
        translation_pipeline,
        aspect_extraction_pipeline,
        pyabsa_aspect_extraction_pipeline,
        aspect_extraction_retry_pipeline,
    ],
    sensors=[
        gcs_ingestion_sensor,
        gcs_parsing_sensor,
        ingestion_to_parsing,
        parsing_to_translation,
        translation_to_aspects,
    ],
    resources=resources_defs,
)
