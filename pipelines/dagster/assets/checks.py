"""
Asset checks for data quality validation in the tourism data pipeline.

These checks ensure data integrity, completeness, and consistency across tables.
"""

from dagster import asset_check, AssetCheckResult, AssetCheckSeverity
from config import get_db_connection


@asset_check(asset="reviews_table", description="Verify all reviews have non-empty content")
def check_reviews_have_content() -> AssetCheckResult:
    """Check that all reviews have either content or translated_content."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN COALESCE(translated_content, content) IS NULL 
                               OR COALESCE(translated_content, content) = '' THEN 1 END) as empty_content
                FROM reviews
            """)
            total, empty = cur.fetchone()
            
            passed = empty == 0
            return AssetCheckResult(
                passed=passed,
                severity=AssetCheckSeverity.ERROR if not passed else AssetCheckSeverity.WARN,
                metadata={
                    "total_reviews": total,
                    "empty_content_count": empty,
                    "empty_content_percent": round(empty / total * 100, 2) if total > 0 else 0,
                },
                description=f"Found {empty} reviews with empty content out of {total} total" if not passed 
                           else f"All {total} reviews have content"
            )
    finally:
        conn.close()


@asset_check(asset="reviews_table", description="Check language distribution is reasonable")
def check_reviews_language_distribution() -> AssetCheckResult:
    """Verify language distribution and flag if any language dominates unexpectedly."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    language,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
                FROM reviews
                WHERE language IS NOT NULL
                GROUP BY language
                ORDER BY count DESC
                LIMIT 10
            """)
            rows = cur.fetchall()
            
            if not rows:
                return AssetCheckResult(
                    passed=False,
                    severity=AssetCheckSeverity.WARN,
                    description="No language information found in reviews"
                )
            
            # Check if any single language dominates > 95%
            top_language_pct = rows[0][2] if rows else 0
            passed = top_language_pct < 95.0
            
            language_dist = {row[0]: {"count": row[1], "percentage": row[2]} for row in rows}
            
            return AssetCheckResult(
                passed=passed,
                severity=AssetCheckSeverity.WARN,
                metadata={
                    "language_distribution": language_dist,
                    "top_language": rows[0][0],
                    "top_language_percent": top_language_pct,
                    "unique_languages": len(rows),
                },
                description=f"Top language ({rows[0][0]}) represents {top_language_pct}% of reviews"
            )
    finally:
        conn.close()


@asset_check(asset="destinations_table", description="Verify destinations are referenced in reviews")
def check_destinations_referenced() -> AssetCheckResult:
    """Check that destinations in the reference table are actually used by reviews."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) as total_destinations,
                    COUNT(CASE WHEN NOT EXISTS (
                        SELECT 1 FROM reviews r WHERE r.destination_id = d.id
                    ) THEN 1 END) as unreferenced
                FROM destinations d
            """)
            total, unreferenced = cur.fetchone()
            
            if total == 0:
                return AssetCheckResult(
                    passed=True,
                    severity=AssetCheckSeverity.WARN,
                    description="No destinations found (may not be parsed yet)"
                )
            
            unreferenced_pct = (unreferenced / total * 100) if total > 0 else 0
            passed = unreferenced_pct < 10.0  # Warn if >10% are orphaned
            
            return AssetCheckResult(
                passed=passed,
                severity=AssetCheckSeverity.WARN,
                metadata={
                    "total_destinations": total,
                    "unreferenced_count": unreferenced,
                    "unreferenced_percent": round(unreferenced_pct, 2),
                },
                description=f"{unreferenced} destinations ({unreferenced_pct:.1f}%) are not referenced by any reviews"
            )
    finally:
        conn.close()


@asset_check(asset="offerings_table", description="Verify offerings are referenced in reviews")
def check_offerings_referenced() -> AssetCheckResult:
    """Check that offerings in the reference table are actually used by reviews."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) as total_offerings,
                    COUNT(CASE WHEN NOT EXISTS (
                        SELECT 1 FROM reviews r WHERE r.offering_id = o.id
                    ) THEN 1 END) as unreferenced
                FROM offerings o
            """)
            total, unreferenced = cur.fetchone()
            
            if total == 0:
                return AssetCheckResult(
                    passed=True,
                    severity=AssetCheckSeverity.WARN,
                    description="No offerings found (may not be parsed yet)"
                )
            
            unreferenced_pct = (unreferenced / total * 100) if total > 0 else 0
            passed = unreferenced_pct < 10.0  # Warn if >10% are orphaned
            
            return AssetCheckResult(
                passed=passed,
                severity=AssetCheckSeverity.WARN,
                metadata={
                    "total_offerings": total,
                    "unreferenced_count": unreferenced,
                    "unreferenced_percent": round(unreferenced_pct, 2),
                },
                description=f"{unreferenced} offerings ({unreferenced_pct:.1f}%) are not referenced by any reviews"
            )
    finally:
        conn.close()


@asset_check(asset="review_aspects_table", description="Verify aspects have evidence spans")
def check_aspects_have_evidence() -> AssetCheckResult:
    """Check that extracted aspects have evidence text from the review."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) as total_aspects,
                    COUNT(CASE WHEN evidence_span IS NULL OR evidence_span = '' THEN 1 END) as missing_evidence,
                    COUNT(CASE WHEN start_idx IS NULL OR end_idx IS NULL THEN 1 END) as missing_indices
                FROM review_aspect_extractions
            """)
            total, missing_evidence, missing_indices = cur.fetchone()
            
            if total == 0:
                return AssetCheckResult(
                    passed=True,
                    severity=AssetCheckSeverity.WARN,
                    description="No aspects found (may not be extracted yet)"
                )
            
            missing_evidence_pct = (missing_evidence / total * 100) if total > 0 else 0
            missing_indices_pct = (missing_indices / total * 100) if total > 0 else 0
            
            # It's OK if indices are missing (API might not return them), but evidence should exist
            passed = missing_evidence_pct < 5.0
            
            return AssetCheckResult(
                passed=passed,
                severity=AssetCheckSeverity.ERROR if not passed else AssetCheckSeverity.WARN,
                metadata={
                    "total_aspects": total,
                    "missing_evidence_count": missing_evidence,
                    "missing_evidence_percent": round(missing_evidence_pct, 2),
                    "missing_indices_count": missing_indices,
                    "missing_indices_percent": round(missing_indices_pct, 2),
                },
                description=f"{missing_evidence} aspects ({missing_evidence_pct:.1f}%) have no evidence span"
            )
    finally:
        conn.close()


@asset_check(asset="review_aspects_table", description="Check average aspects per review is reasonable")
def check_aspects_per_review() -> AssetCheckResult:
    """Verify the distribution of aspects per review is within expected ranges."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    COUNT(DISTINCT review_id) as reviews_with_aspects,
                    COUNT(*) as total_aspects,
                    AVG(aspect_count)::numeric(5,2) as avg_aspects,
                    MIN(aspect_count) as min_aspects,
                    MAX(aspect_count) as max_aspects,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY aspect_count) as median_aspects
                FROM (
                    SELECT review_id, COUNT(*) as aspect_count
                    FROM review_aspect_extractions
                    GROUP BY review_id
                ) aspect_counts
            """)
            row = cur.fetchone()
            
            if not row or row[0] == 0:
                return AssetCheckResult(
                    passed=True,
                    severity=AssetCheckSeverity.WARN,
                    description="No aspects found (may not be extracted yet)"
                )
            
            reviews_with_aspects, total, avg, min_val, max_val, median = row
            avg_aspects = float(avg) if avg else 0
            median_aspects = float(median) if median else 0
            
            # Check if average is reasonable (between 1 and 50)
            passed = 1.0 <= avg_aspects <= 50.0
            
            return AssetCheckResult(
                passed=passed,
                severity=AssetCheckSeverity.WARN,
                metadata={
                    "reviews_with_aspects": reviews_with_aspects,
                    "total_aspects": total,
                    "avg_aspects_per_review": float(avg_aspects),
                    "median_aspects_per_review": float(median_aspects),
                    "min_aspects": min_val,
                    "max_aspects": max_val,
                },
                description=f"Average {avg_aspects:.1f} aspects per review (median: {median_aspects:.1f}, range: {min_val}-{max_val})"
            )
    finally:
        conn.close()


@asset_check(asset="review_aspects_table", description="Verify no orphaned aspects (referential integrity)")
def check_no_orphaned_aspects() -> AssetCheckResult:
    """Check that all aspects reference valid reviews (should be enforced by FK but verify)."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) as total_aspects,
                    COUNT(CASE WHEN NOT EXISTS (
                        SELECT 1 FROM reviews r WHERE r.id = rae.review_id
                    ) THEN 1 END) as orphaned
                FROM review_aspect_extractions rae
            """)
            total, orphaned = cur.fetchone()
            
            if total == 0:
                return AssetCheckResult(
                    passed=True,
                    severity=AssetCheckSeverity.WARN,
                    description="No aspects found (may not be extracted yet)"
                )
            
            passed = orphaned == 0
            
            return AssetCheckResult(
                passed=passed,
                severity=AssetCheckSeverity.ERROR if not passed else AssetCheckSeverity.WARN,
                metadata={
                    "total_aspects": total,
                    "orphaned_count": orphaned,
                },
                description=f"Found {orphaned} orphaned aspects with no matching review" if not passed
                           else f"All {total} aspects reference valid reviews"
            )
    finally:
        conn.close()
