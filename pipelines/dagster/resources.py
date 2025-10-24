"""
Dagster resources for the tourism data pipeline.

Resources provide managed access to external services like databases, APIs, and storage.
They enable dependency injection, testing, and centralized configuration.
"""

import os
from contextlib import contextmanager
from typing import Iterator, Optional

import psycopg2
from psycopg2.pool import SimpleConnectionPool
from google.cloud import storage

from dagster import ConfigurableResource, resource
from pydantic import Field


class DatabaseResource(ConfigurableResource):
    """
    PostgreSQL database resource for Supabase connection.
    
    Provides connection pooling and automatic cleanup.
    """
    
    host: str = Field(description="Database host")
    port: int = Field(default=6543, description="Database port")
    database: str = Field(default="postgres", description="Database name")
    user: str = Field(description="Database user")
    password: str = Field(description="Database password")
    min_connections: int = Field(default=1, description="Minimum connections in pool")
    max_connections: int = Field(default=10, description="Maximum connections in pool")
    
    _pool: SimpleConnectionPool = None
    
    def setup_for_execution(self, context) -> None:
        """Initialize connection pool when resource is set up."""
        self._pool = SimpleConnectionPool(
            self.min_connections,
            self.max_connections,
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )
        context.log.info(f"Initialized database connection pool (min={self.min_connections}, max={self.max_connections})")
    
    def teardown_after_execution(self, context) -> None:
        """Clean up connection pool when resource is torn down."""
        if self._pool:
            self._pool.closeall()
            context.log.info("Closed database connection pool")
    
    @contextmanager
    def get_connection(self) -> Iterator[psycopg2.extensions.connection]:
        """
        Get a database connection from the pool.
        
        Yields:
            A psycopg2 connection that is automatically returned to the pool.
        """
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)
    
    def get_connection_direct(self) -> psycopg2.extensions.connection:
        """
        Get a connection that must be manually managed.
        
        Use this when you need to pass a connection around.
        Remember to call putconn() when done.
        """
        return self._pool.getconn()
    
    def return_connection(self, conn: psycopg2.extensions.connection) -> None:
        """Return a connection to the pool."""
        self._pool.putconn(conn)


class GCSResource(ConfigurableResource):
    """
    Google Cloud Storage resource.
    
    Provides authenticated access to GCS buckets.
    """
    
    credentials_path: str = Field(
        default="/opt/dagster/app/gcs-credentials.json",
        description="Path to GCS service account credentials JSON"
    )
    project_id: Optional[str] = Field(default=None, description="GCP project ID (optional)")
    
    _client: storage.Client = None
    
    def setup_for_execution(self, context) -> None:
        """Initialize GCS client."""
        if self.credentials_path and os.path.exists(self.credentials_path):
            self._client = storage.Client.from_service_account_json(
                self.credentials_path,
                project=self.project_id
            )
            context.log.info(f"Initialized GCS client with credentials from {self.credentials_path}")
        else:
            self._client = storage.Client(project=self.project_id)
            context.log.info("Initialized GCS client with default credentials")
    
    def get_client(self) -> storage.Client:
        """Get the GCS client."""
        return self._client
    
    def get_bucket(self, bucket_name: str) -> storage.Bucket:
        """Get a specific GCS bucket."""
        return self._client.bucket(bucket_name)


# Legacy function-based resources for backward compatibility (not currently used)
# Keeping for reference - ConfigurableResource is the modern approach


# Resource definitions using environment variables
def get_database_resource_config() -> dict:
    """Get database resource config from environment variables."""
    return {
        "host": os.getenv("SUPABASE_DB_HOST"),
        "port": int(os.getenv("SUPABASE_DB_PORT", "6543")),
        "database": os.getenv("SUPABASE_DB_NAME", "postgres"),
        "user": os.getenv("SUPABASE_DB_USER"),
        "password": os.getenv("SUPABASE_DB_PASSWORD"),
    }


def get_gcs_resource_config() -> dict:
    """Get GCS resource config from environment variables."""
    return {
        "credentials_path": os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS",
            "/opt/dagster/app/gcs-credentials.json"
        ),
        "project_id": os.getenv("GCP_PROJECT_ID"),
    }
