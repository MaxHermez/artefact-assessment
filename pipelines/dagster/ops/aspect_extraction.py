"""
Aspect extraction operations that call a stateless external API (Cloud Run)
and persist extracted aspects into Supabase (PostgreSQL).

Design:
- load_reviews_for_aspects: load batches of reviews that do not yet have aspects
    (uses COALESCE(translated_content, content) as text). Ensures table exists.
- extract_aspects_batch_via_api: call external API per review with simple key auth.
- upsert_review_aspects: flatten and insert aspects into table review_aspect_extractions.

Env vars required for API calls:
- ASPECT_API_URL (e.g., https://<service>.a.run.app/extract)
- ASPECT_API_KEY
- ASPECT_API_TIMEOUT (optional, default 30 seconds)
"""

from typing import Dict, List, Optional
import os
import time
import hashlib
import json

import httpx
from psycopg2.extras import execute_batch
from psycopg2 import errors as pg_errors

from dagster import (
    op,
    In,
    Out,
    DynamicOut,
    DynamicOutput,
    OpExecutionContext,
    get_dagster_logger,
    Field,
    Int,
    AssetMaterialization,
)

from config import get_db_connection


def _ensure_review_aspect_extractions_table(conn) -> None:
    """Create the review_aspect_extractions table if it doesn't exist (idempotent)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS review_aspect_extractions (
                id BIGSERIAL PRIMARY KEY,
                review_id BIGINT NOT NULL REFERENCES reviews(id) ON DELETE CASCADE,
                aspect TEXT NOT NULL,
                category TEXT NULL,
                evidence_span TEXT NOT NULL,
                start_idx INTEGER NULL,
                end_idx INTEGER NULL,
                confidence NUMERIC NULL,
                model TEXT NULL,
                prompt_version TEXT NULL,
                polarity TEXT NULL,
                approach TEXT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_review_aspect_extractions_review_id ON review_aspect_extractions(review_id);
            -- Ensure new unique index based on evidence text rather than indices (which can be NULL)
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = 'uq_review_aspects_dedup'
                ) THEN
                    DROP INDEX IF EXISTS uq_review_aspects_dedup;
                END IF;
            END$$;
            CREATE UNIQUE INDEX IF NOT EXISTS uq_review_aspect_extractions_dedup
            ON review_aspect_extractions (review_id, aspect, evidence_span, COALESCE(prompt_version, ''), COALESCE(model, ''));

            -- Migrate existing schema to new nullable indices and add polarity
            ALTER TABLE review_aspect_extractions ALTER COLUMN start_idx DROP NOT NULL;
            ALTER TABLE review_aspect_extractions ALTER COLUMN end_idx DROP NOT NULL;
            ALTER TABLE review_aspect_extractions ADD COLUMN IF NOT EXISTS polarity TEXT NULL;
            ALTER TABLE review_aspect_extractions ADD COLUMN IF NOT EXISTS approach TEXT NULL;
            """
        )
        conn.commit()


def _get_or_create_aspect_cache_table_name(conn) -> str:
    """Return the cache table name to use, creating it if necessary.

    Handles the case where a composite TYPE named 'aspect_extraction_cache' exists
    (from a previous object) which would cause CREATE TABLE to error with
    pg_type_typname_nsp_index UniqueViolation. In that case, we fall back to creating
    and using a table named 'aspect_extraction_cache_tbl'.
    """
    primary = "aspect_extraction_cache"
    fallback = "aspect_extraction_cache_tbl"

    with conn.cursor() as cur:
        # If primary table exists, use it
        cur.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (primary,),
        )
        if cur.fetchone():
            return primary

        # If fallback table exists, use it
        cur.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (fallback,),
        )
        if cur.fetchone():
            return fallback

        # Check if a TYPE already exists with the primary name in public schema
        cur.execute(
            """
            SELECT 1
            FROM pg_type t
            JOIN pg_namespace n ON n.oid = t.typnamespace
            WHERE t.typname = %s AND n.nspname = 'public'
            """,
            (primary,),
        )
        type_conflicts = cur.fetchone() is not None

    # Try to create the appropriate table
    table_to_create = fallback if type_conflicts else primary
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_to_create} (
            cache_key TEXT PRIMARY KEY,
            model TEXT NULL,
            prompt_version TEXT NULL,
            response_json JSONB NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """
    with conn.cursor() as cur:
        try:
            cur.execute(ddl)
            conn.commit()
            return table_to_create
        except Exception as e:
            # As an extra guard, if we still hit the type unique violation on primary,
            # retry with fallback name.
            if (
                table_to_create == primary
                and (
                    isinstance(e, pg_errors.UniqueViolation)
                    or "pg_type_typname_nsp_index" in str(e)
                )
            ):
                conn.rollback()
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {fallback} (
                        cache_key TEXT PRIMARY KEY,
                        model TEXT NULL,
                        prompt_version TEXT NULL,
                        response_json JSONB NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    );
                    """
                )
                conn.commit()
                return fallback
            raise


def _make_cache_key(text: str) -> str:
    """Build a stable cache key using model/prompt defaults and text hash.

    If ASPECT_REQUEST_MODEL / ASPECT_PROMPT_VERSION are set, include them so cache respects versioning.
    Otherwise, assume defaults (gpt-5-mini, v1) to align with API defaults.
    """
    model = os.getenv("ASPECT_REQUEST_MODEL", "gpt-5-mini")
    prompt_version = os.getenv("ASPECT_PROMPT_VERSION", "v1")
    h = hashlib.sha256(f"{model}|{prompt_version}|{text}".encode("utf-8")).hexdigest()
    return h


@op(
    description="Load reviews in batches that need aspect extraction",
    out=DynamicOut(List[Dict], description="Batches of reviews to extract aspects for"),
    config_schema={
        "batch_size": Field(Int, description="Number of reviews per batch", default_value=200),
    },
)
def load_reviews_for_aspects(context: OpExecutionContext):
    """Yield batches of reviews where aspects haven't been extracted yet.

    A review is eligible if COALESCE(translated_content, content) is not NULL
    and there are no rows in review_aspect_extractions for that review_id.
    """
    logger = get_dagster_logger()
    batch_size: int = context.op_config.get("batch_size", 200)

    conn = get_db_connection()
    try:
        _ensure_review_aspect_extractions_table(conn)

        with conn.cursor() as cur:
            # Count eligible reviews
            cur.execute(
                """
                SELECT COUNT(*)
                FROM reviews r
                WHERE COALESCE(r.translated_content, r.content) IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM review_aspect_extractions rae WHERE rae.review_id = r.id
                )
                """
            )
            total_count = cur.fetchone()[0]
            logger.info(f"Found {total_count} reviews needing aspect extraction")

            # Iterate with offset pagination
            offset = 0
            batch_num = 0
            while True:
                cur.execute(
                    """
                    SELECT r.id, COALESCE(r.translated_content, r.content) AS text
                    FROM reviews r
                    WHERE COALESCE(r.translated_content, r.content) IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM review_aspect_extractions rae WHERE rae.review_id = r.id
                    )
                    ORDER BY r.id
                    LIMIT %s OFFSET %s
                    """,
                    (batch_size, offset),
                )
                rows = cur.fetchall()
                if not rows:
                    break

                batch = [{"id": row[0], "text": row[1]} for row in rows]
                yield DynamicOutput(batch, mapping_key=f"batch_{batch_num}")
                logger.info(f"Yielded batch {batch_num + 1} with {len(batch)} reviews")

                offset += batch_size
                batch_num += 1

            context.log_event(
                AssetMaterialization(
                    asset_key="reviews_for_aspects",
                    description="Loaded reviews needing aspect extraction",
                    metadata={
                        "total_reviews": total_count,
                        "batch_size": batch_size,
                        "num_batches": batch_num,
                    },
                )
            )

    except Exception as e:
        logger.error(f"Failed to load reviews for aspects: {e}")
        raise
    finally:
        conn.close()


@op(
    description="Call external Aspect API for a batch of reviews",
    ins={"reviews": In(List[Dict])},
    out=Out(List[Dict], description="Batch results with aspects per review"),
)
def extract_aspects_batch_via_api(context: OpExecutionContext, reviews: List[Dict]) -> List[Dict]:
    """Call the aspect extraction API batch endpoint, chunking into 32-item requests."""
    logger = get_dagster_logger()
    if not reviews:
        return []

    api_url = os.getenv("ASPECT_API_URL", "").rstrip("/")
    api_key = os.getenv("ASPECT_API_KEY")
    # Batch endpoint now processes items concurrently, so timeout can be reasonable
    timeout = float(os.getenv("ASPECT_API_TIMEOUT", "60"))

    if not api_url or not api_key:
        raise RuntimeError("ASPECT_API_URL and ASPECT_API_KEY must be set")

    batch_endpoint = f"{api_url}/extract-batch"
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
    results = []
    start_time = time.time()

    # Chunk reviews into batches of 32 (API limit)
    chunk_size = 32
    with httpx.Client(timeout=timeout) as client:
        for i in range(0, len(reviews), chunk_size):
            chunk = reviews[i:i + chunk_size]
            chunk_num = i // chunk_size
            logger.info(f"Processing chunk {chunk_num + 1}/{(len(reviews) + chunk_size - 1) // chunk_size} with {len(chunk)} reviews")
            
            # Build batch payload
            items = []
            for row in chunk:
                items.append({
                    "id": str(row.get("id")),
                    "text": row.get("text", ""),
                    "options": {
                        "max_aspects": 10,
                        "language": "en"
                    }
                })
            
            payload = {"items": items}
            
            try:
                logger.info(f"Calling {batch_endpoint} with {len(items)} items, timeout={timeout}s")
                chunk_start = time.time()
                resp = client.post(batch_endpoint, json=payload, headers=headers)
                chunk_duration = time.time() - chunk_start
                logger.info(f"API call completed in {chunk_duration:.2f}s, status={resp.status_code}")
                resp.raise_for_status()
                body = resp.json()
                
                # Process batch response
                for item in body.get("items", []):
                    # Find original text from reviews
                    orig = next((r for r in chunk if str(r.get("id")) == str(item.get("id"))), {})
                    results.append({
                        "id": item.get("id"),
                        "aspects": item.get("aspects", []),
                        "meta": item.get("meta", {}),
                        "text": orig.get("text", ""),
                    })
                logger.info(f"Successfully processed chunk {chunk_num + 1} with {len(body.get('items', []))} results")
            except httpx.TimeoutException as e:
                logger.error(f"Timeout on chunk {chunk_num + 1} after {time.time() - chunk_start:.2f}s: {e}")
                for row in chunk:
                    results.append({
                        "id": row.get("id"),
                        "aspects": [],
                        "meta": {"error": f"timeout: {str(e)}"},
                        "text": row.get("text", ""),
                    })
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error on chunk {chunk_num + 1}: {e.response.status_code} - {e.response.text[:200]}")
                for row in chunk:
                    results.append({
                        "id": row.get("id"),
                        "aspects": [],
                        "meta": {"error": f"http_{e.response.status_code}"},
                        "text": row.get("text", ""),
                    })
            except Exception as e:
                logger.error(f"Unexpected error on chunk {chunk_num + 1}: {type(e).__name__}: {e}")
                for row in chunk:
                    results.append({
                        "id": row.get("id"),
                        "aspects": [],
                        "meta": {"error": str(e)},
                        "text": row.get("text", ""),
                    })

    duration_ms = int((time.time() - start_time) * 1000)
    context.log_event(
        AssetMaterialization(
            asset_key="aspect_api_batch",
            description="Called aspect API batch endpoint",
            metadata={"reviews": len(reviews), "duration_ms": duration_ms},
        )
    )
    return results


@op(
    description="Call PyABSA API (batch) for a batch of reviews",
    ins={"reviews": In(List[Dict])},
    out=Out(List[Dict], description="Batch results with aspects per review"),
)
def extract_aspects_batch_via_pyabsa(context: OpExecutionContext, reviews: List[Dict]) -> List[Dict]:
    logger = get_dagster_logger()
    if not reviews:
        logger.warning("No reviews to process for aspects (pyabsa)")
        return []

    api_url = os.getenv("PYABSA_API_URL")
    api_key = os.getenv("PYABSA_API_KEY")
    timeout = float(os.getenv("PYABSA_API_TIMEOUT", "60"))

    if not api_url or not api_key:
        raise RuntimeError("PYABSA_API_URL and PYABSA_API_KEY must be set in the environment")

    # Ensure we hit the batch endpoint
    if not api_url.rstrip("/").endswith("extract-batch"):
        api_url = api_url.rstrip("/") + "/extract-batch"

    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
    results: List[Dict] = []

    # Build batch payload up to the API limit (32)
    batch_items = []
    for r in reviews[:32]:
        batch_items.append({"id": str(r.get("id")), "text": r.get("text")})

    payload = {"items": batch_items}

    start_time = time.time()
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(api_url, json=payload, headers=headers)
            resp.raise_for_status()
            body = resp.json() or {}
            for item in body.get("items", []):
                # Attach source text for index computation downstream
                src = next((rv for rv in reviews if str(rv.get("id")) == str(item.get("id"))), None)
                item["text"] = (src or {}).get("text")
                results.append(item)
    except Exception as e:
        logger.error(f"PyABSA API batch call failed: {e}")
        # Fall back to empty results for all items in this batch
        for r in reviews:
            results.append({"id": r.get("id"), "aspects": [], "meta": {"error": str(e)}, "text": r.get("text")})

    duration_ms = int((time.time() - start_time) * 1000)
    context.log_event(
        AssetMaterialization(
            asset_key="pyabsa_api_batch",
            description="Called PyABSA aspect API for a batch",
            metadata={"reviews_attempted": len(reviews), "duration_ms": duration_ms},
        )
    )
    return results


@op(
    description="Persist extracted aspects to Supabase with rate limiting",
    ins={"batch_results": In(List[Dict])},
    out=Out(int, description="Number of aspect rows inserted"),
)
def upsert_review_aspects(context: OpExecutionContext, batch_results: List[Dict]) -> int:
    logger = get_dagster_logger()
    if not batch_results:
        logger.warning("No batch results to upsert")
        return 0

    # Rate limiting for Supabase free tier: small delay before processing
    time.sleep(1)

    # Flatten aspects
    rows_to_insert: List[Dict] = []
    for item in batch_results:
        review_id = item.get("id")
        source_text = item.get("text") or ""
        meta = item.get("meta") or {}
        model_name = meta.get("model") if isinstance(meta, dict) else None
        inferred_approach = (
            "pyabsa" if (isinstance(model_name, str) and "pyabsa" in model_name.lower()) else "llm"
        )
        for a in item.get("aspects", []) or []:
            aspect = a.get("aspect")
            evidence = (a.get("evidence_span") or "").strip()
            
            # Skip if aspect is None or empty - required field
            if not aspect or not aspect.strip():
                logger.warning(f"Skipping aspect with no name for review_id={review_id}, evidence={evidence[:50]}")
                continue
            
            start_idx = None
            end_idx = None
            if evidence and source_text:
                # Try exact match first
                idx = source_text.find(evidence)
                if idx == -1:
                    # Fallback to case-insensitive search
                    idx = source_text.lower().find(evidence.lower())
                if idx != -1:
                    start_idx = idx
                    end_idx = idx + len(evidence)

            rows_to_insert.append({
                "review_id": review_id,
                "aspect": aspect.strip(),
                "category": None,
                "evidence_span": evidence,
                "start_idx": start_idx,
                "end_idx": end_idx,
                "confidence": a.get("confidence"),
                "model": model_name,
                "prompt_version": meta.get("prompt_version") if isinstance(meta, dict) else None,
                "polarity": a.get("polarity"),
                "approach": inferred_approach,
            })

    if not rows_to_insert:
        logger.info("No aspects extracted in this batch")
        return 0

    conn = get_db_connection()
    try:
        _ensure_review_aspect_extractions_table(conn)
        with conn.cursor() as cur:
            insert_sql = (
                """
                INSERT INTO review_aspect_extractions (
                    review_id, aspect, category, evidence_span,
                    start_idx, end_idx, confidence, model, prompt_version, polarity, approach
                ) VALUES (
                    %(review_id)s, %(aspect)s, %(category)s, %(evidence_span)s,
                    %(start_idx)s, %(end_idx)s, %(confidence)s, %(model)s, %(prompt_version)s, %(polarity)s, %(approach)s
                )
                ON CONFLICT DO NOTHING
                """
            )
            execute_batch(cur, insert_sql, rows_to_insert, page_size=500)
            conn.commit()

        inserted = len(rows_to_insert)
        logger.info(f"Inserted up to {inserted} aspect rows (duplicates skipped)")
        context.log_event(
            AssetMaterialization(
                asset_key="review_aspect_extractions",
                description="Persisted extracted aspects (approach-agnostic store)",
                metadata={
                    "rows_attempted": len(rows_to_insert),
                    "rows_inserted_max": inserted,
                },
            )
        )
        return inserted
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to upsert review aspects: {e}")
        raise
    finally:
        conn.close()
