"""
Dagster sensors for triggering pipelines based on GCS Pub/Sub notifications.
"""

import os
import json
from typing import Optional

from google.cloud import pubsub_v1

from dagster import (
    sensor,
    run_status_sensor,
    DagsterRunStatus,
    RunRequest,
    SkipReason,
    SensorEvaluationContext,
    get_dagster_logger,
)

from jobs import (
    ingestion_pipeline,
    parsing_pipeline,
    translation_pipeline,
    aspect_extraction_pipeline,
    pyabsa_aspect_extraction_pipeline,
)


def get_env_specific_subscription() -> str:
    """Get environment-specific subscription name."""
    env = os.getenv('DEPLOYMENT_ENV', 'dev')
    return f"gcs-events-sub-{env}"


def get_pipeline_for_file(file_path: str) -> Optional[str]:
    """
    Determine which Dagster pipeline to trigger based on the file path.
    
    Args:
        file_path: The GCS object path
        
    Returns:
        Pipeline name or None if no pipeline should be triggered
    """
    file_path = file_path.lower()
    
    if file_path.startswith('raw-data/') or 'dataset' in file_path:
        return 'ingestion_pipeline'
    elif file_path.startswith('mappings/') or 'mappings.json' in file_path:
        return 'parsing_pipeline'
    else:
        return None


@sensor(
    job=ingestion_pipeline,
    name="gcs_ingestion_sensor",
    description="Monitors GCS Pub/Sub for new dataset files and triggers ingestion pipeline",
    minimum_interval_seconds=30,
)
def gcs_ingestion_sensor(context: SensorEvaluationContext):
    """
    Sensor that polls GCS Pub/Sub subscription for new dataset files.
    Triggers ingestion_pipeline when files are uploaded to raw-data/* prefix.
    """
    logger = get_dagster_logger()
    
    # Check if GCS monitoring is enabled
    project_id = os.getenv('GCP_PROJECT_ID')
    if not project_id:
        yield SkipReason("GCP_PROJECT_ID not configured - GCS monitoring disabled")
        return
    
    try:
        # Get subscription
        subscriber = pubsub_v1.SubscriberClient()
        subscription_name = get_env_specific_subscription()
        subscription_path = subscriber.subscription_path(project_id, subscription_name)
        
        # Pull messages (non-blocking)
        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": 10,  # Process up to 10 files per sensor tick
            },
            timeout=5.0,
        )
        
        if not response.received_messages:
            yield SkipReason("No new GCS events")
            return
        
        # Process messages
        run_requests = []
        ack_ids = []
        
        for received_message in response.received_messages:
            attributes = received_message.message.attributes
            event_type = attributes.get('eventType', '')
            object_name = attributes.get('objectId', '')
            bucket_name = attributes.get('bucketId', '')
            
            # Only process OBJECT_FINALIZE events (file created/updated)
            if event_type != 'OBJECT_FINALIZE':
                ack_ids.append(received_message.ack_id)
                continue
            
            # Check if this file should trigger ingestion
            pipeline_name = get_pipeline_for_file(object_name)
            
            if pipeline_name == 'ingestion_pipeline':
                logger.info(f"GCS event: {object_name} in bucket {bucket_name}")
                
                run_requests.append(
                    RunRequest(
                        run_key=f"gcs_{bucket_name}_{object_name}_{received_message.message.message_id}",
                        tags={
                            "source": "gcs_pubsub_sensor",
                            "bucket": bucket_name,
                            "file": object_name,
                            "event_type": event_type,
                        }
                    )
                )
                ack_ids.append(received_message.ack_id)
            else:
                # Not for ingestion, acknowledge but don't process
                ack_ids.append(received_message.ack_id)
        
        # Acknowledge all processed messages
        if ack_ids:
            subscriber.acknowledge(
                request={
                    "subscription": subscription_path,
                    "ack_ids": ack_ids,
                }
            )
        
        if run_requests:
            logger.info(f"Triggering {len(run_requests)} ingestion pipeline runs")
            for run_request in run_requests:
                yield run_request
        else:
            yield SkipReason(f"Processed {len(ack_ids)} messages, none triggered ingestion")
            
    except Exception as e:
        logger.error(f"Error in GCS sensor: {str(e)}")
        yield SkipReason(f"Error polling GCS events: {str(e)}")


@sensor(
    job=parsing_pipeline,
    name="gcs_parsing_sensor",
    description="Monitors GCS Pub/Sub for mapping files and triggers parsing pipeline",
    minimum_interval_seconds=30,
)
def gcs_parsing_sensor(context: SensorEvaluationContext):
    """
    Sensor that polls GCS Pub/Sub subscription for new mapping files.
    Triggers parsing_pipeline when files are uploaded to mappings/* prefix.
    """
    logger = get_dagster_logger()
    
    # Check if GCS monitoring is enabled
    project_id = os.getenv('GCP_PROJECT_ID')
    if not project_id:
        yield SkipReason("GCP_PROJECT_ID not configured - GCS monitoring disabled")
        return
    
    try:
        # Get subscription
        subscriber = pubsub_v1.SubscriberClient()
        subscription_name = get_env_specific_subscription()
        subscription_path = subscriber.subscription_path(project_id, subscription_name)
        
        # Pull messages (non-blocking)
        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": 10,
            },
            timeout=5.0,
        )
        
        if not response.received_messages:
            yield SkipReason("No new GCS events")
            return
        
        # Process messages
        run_requests = []
        ack_ids = []
        
        for received_message in response.received_messages:
            attributes = received_message.message.attributes
            event_type = attributes.get('eventType', '')
            object_name = attributes.get('objectId', '')
            bucket_name = attributes.get('bucketId', '')
            
            # Only process OBJECT_FINALIZE events
            if event_type != 'OBJECT_FINALIZE':
                ack_ids.append(received_message.ack_id)
                continue
            
            # Check if this file should trigger parsing
            pipeline_name = get_pipeline_for_file(object_name)
            
            if pipeline_name == 'parsing_pipeline':
                logger.info(f"GCS event: {object_name} in bucket {bucket_name}")
                
                run_requests.append(
                    RunRequest(
                        run_key=f"gcs_{bucket_name}_{object_name}_{received_message.message.message_id}",
                        tags={
                            "source": "gcs_pubsub_sensor",
                            "bucket": bucket_name,
                            "file": object_name,
                            "event_type": event_type,
                        }
                    )
                )
                ack_ids.append(received_message.ack_id)
            else:
                # Not for parsing, acknowledge but don't process
                ack_ids.append(received_message.ack_id)
        
        # Acknowledge all processed messages
        if ack_ids:
            subscriber.acknowledge(
                request={
                    "subscription": subscription_path,
                    "ack_ids": ack_ids,
                }
            )
        
        if run_requests:
            logger.info(f"Triggering {len(run_requests)} parsing pipeline runs")
            for run_request in run_requests:
                yield run_request
        else:
            yield SkipReason(f"Processed {len(ack_ids)} messages, none triggered parsing")
            
    except Exception as e:
        logger.error(f"Error in GCS sensor: {str(e)}")
        yield SkipReason(f"Error polling GCS events: {str(e)}")


# =========================
# Run Status Chain Sensors
# =========================

@run_status_sensor(run_status=DagsterRunStatus.SUCCESS, name="ingestion_to_parsing", minimum_interval_seconds=30)
def ingestion_to_parsing(context, dagster_run):
    """Trigger parsing_pipeline when ingestion_pipeline succeeds."""
    if dagster_run.job_name != "ingestion_pipeline":
        return
    tags = {
        "trigger": "chain",
        "parent_run_id": dagster_run.run_id,
        "parent_job": dagster_run.job_name,
    }
    yield RunRequest(job_name="parsing_pipeline", run_key=f"parsing_after_{dagster_run.run_id}", tags=tags)


@run_status_sensor(run_status=DagsterRunStatus.SUCCESS, name="parsing_to_translation", minimum_interval_seconds=30)
def parsing_to_translation(context, dagster_run):
    """Trigger translation_pipeline when parsing_pipeline succeeds."""
    if dagster_run.job_name != "parsing_pipeline":
        return
    tags = {
        "trigger": "chain",
        "parent_run_id": dagster_run.run_id,
        "parent_job": dagster_run.job_name,
    }
    yield RunRequest(job_name="translation_pipeline", run_key=f"translation_after_{dagster_run.run_id}", tags=tags)


@run_status_sensor(run_status=DagsterRunStatus.SUCCESS, name="translation_to_aspects", minimum_interval_seconds=30)
def translation_to_aspects(context, dagster_run):
    """Trigger both aspect extraction pipelines when translation_pipeline succeeds."""
    if dagster_run.job_name != "translation_pipeline":
        return
    tags = {
        "trigger": "chain",
        "parent_run_id": dagster_run.run_id,
        "parent_job": dagster_run.job_name,
    }
    # Trigger LLM-based aspects
    yield RunRequest(job_name="aspect_extraction_pipeline", run_key=f"llm_aspects_after_{dagster_run.run_id}", tags=tags)
    # Trigger PyABSA-based aspects
    yield RunRequest(job_name="pyabsa_aspect_extraction_pipeline", run_key=f"pyabsa_aspects_after_{dagster_run.run_id}", tags=tags)
