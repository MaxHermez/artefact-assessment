"""
Configuration and utility functions for the tourism data pipeline.
"""

import os
import psycopg2
from google.cloud import storage, pubsub_v1
from google.api_core.exceptions import AlreadyExists


def get_gcs_client():
    """Initialize GCS client with credentials from environment."""
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '/opt/dagster/app/gcs-credentials.json')
    if credentials_path and os.path.exists(credentials_path):
        return storage.Client.from_service_account_json(credentials_path)
    return storage.Client()


def get_db_connection():
    """
    Create a connection to the Supabase PostgreSQL database.
    
    DEPRECATED: Use the 'database' resource instead for new code.
    This function is kept for backward compatibility during migration.
    
    Example migration:
        # Old way:
        conn = get_db_connection()
        
        # New way (in ops/assets):
        @op(required_resource_keys={"database"})
        def my_op(context):
            with context.resources.database.get_connection() as conn:
                # use conn
    """
    return psycopg2.connect(
        host=os.getenv('SUPABASE_DB_HOST'),
        port=int(os.getenv('SUPABASE_DB_PORT', 6543)),
        database=os.getenv('SUPABASE_DB_NAME', 'postgres'),
        user=os.getenv('SUPABASE_DB_USER'),
        password=os.getenv('SUPABASE_DB_PASSWORD')
    )


def get_env_specific_name(base_name: str) -> str:
    """Generate environment-specific resource names to avoid conflicts across deployments."""
    env = os.getenv('DEPLOYMENT_ENV', 'dev')
    return f"{base_name}-{env}"


def ensure_gcs_notifications_setup():
    """
    Idempotent setup of GCS Pub/Sub notifications for automatic pipeline triggering.
    Safe to run multiple times - only creates resources if they don't exist.
    
    This automatically:
    1. Creates a Pub/Sub topic for GCS events
    2. Creates a subscription to receive events
    3. Configures GCS bucket notifications with appropriate prefixes:
       - raw-data/* -> triggers ingestion_pipeline
       - mappings/* -> triggers parsing_pipeline
    
    Environment variables required:
    - GCP_PROJECT_ID: GCP project ID
    - GCS_BUCKET_NAME: GCS bucket name
    
    Optional:
    - DEPLOYMENT_ENV: Environment identifier (dev/staging/prod) for resource naming
    - AUTO_SETUP_GCS_NOTIFICATIONS: Set to 'false' to disable auto-setup
    """
    # Check if auto-setup is enabled
    if os.getenv('AUTO_SETUP_GCS_NOTIFICATIONS', 'true').lower() != 'true':
        print("ℹ️  GCS notification auto-setup disabled (AUTO_SETUP_GCS_NOTIFICATIONS=false)")
        return
    
    project_id = os.getenv('GCP_PROJECT_ID')
    bucket_name = os.getenv('GCS_BUCKET_NAME')
    
    if not project_id or not bucket_name:
        print("ℹ️  Skipping GCS notification setup - missing GCP_PROJECT_ID or GCS_BUCKET_NAME")
        return
    
    try:
        # Use environment-specific names to avoid conflicts
        topic_name = get_env_specific_name('gcs-dagster-events')
        subscription_name = get_env_specific_name('gcs-events-sub')
        
        print(f"🔧 Setting up GCS notifications for environment: {os.getenv('DEPLOYMENT_ENV', 'dev')}")
        
        # Step 1: Create Pub/Sub topic (idempotent)
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)
        try:
            publisher.create_topic(request={"name": topic_path})
            print(f"  ✓ Created Pub/Sub topic: {topic_name}")
        except AlreadyExists:
            print(f"  ✓ Pub/Sub topic exists: {topic_name}")
        
        # Step 2: Create subscription (idempotent)
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(project_id, subscription_name)
        try:
            subscriber.create_subscription(
                request={
                    "name": subscription_path,
                    "topic": topic_path,
                    "ack_deadline_seconds": 600,  # 10 minutes for long-running pipelines
                }
            )
            print(f"  ✓ Created Pub/Sub subscription: {subscription_name}")
        except AlreadyExists:
            print(f"  ✓ Pub/Sub subscription exists: {subscription_name}")
        
        # Step 3: Create bucket notifications (idempotent)
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        
        # Check existing notifications
        existing_prefixes = {n.blob_name_prefix for n in bucket.list_notifications()}
        
        # Setup notifications with prefixes
        notification_configs = [
            ('raw-data/', get_env_specific_name('dagster-ingestion-trigger')),
            ('mappings/', get_env_specific_name('dagster-parsing-trigger')),
        ]
        
        for prefix, notif_id in notification_configs:
            if prefix not in existing_prefixes:
                notification = bucket.notification(
                    topic_name=topic_path,
                    custom_attributes={'environment': os.getenv('DEPLOYMENT_ENV', 'dev')},
                    event_types=['OBJECT_FINALIZE'],  # Trigger on file create/update
                    blob_name_prefix=prefix,
                    notification_id=notif_id,
                )
                notification.create()
                print(f"  ✓ Created bucket notification for prefix: {prefix}")
            else:
                print(f"  ✓ Bucket notification exists for prefix: {prefix}")
        
        print(f"✅ GCS notifications setup complete")
        print(f"""
📋 Next steps:
  1. Grant GCS service account Pub/Sub Publisher role:
     gcloud projects add-iam-policy-binding {project_id} \\
       --member=serviceAccount:service-{project_id}@gs-project-accounts.iam.gserviceaccount.com \\
       --role=roles/pubsub.publisher
  
  2. Start the Pub/Sub listener:
     python pubsub_listener.py
  
  3. Upload files to trigger pipelines:
     gsutil cp dataset.csv gs://{bucket_name}/raw-data/dataset.csv
     gsutil cp mappings.json gs://{bucket_name}/mappings/mappings.json
""")
        
    except Exception as e:
        print(f"⚠️  GCS notification setup failed (non-fatal): {e}")
        print(f"   You can set AUTO_SETUP_GCS_NOTIFICATIONS=false to disable auto-setup")
