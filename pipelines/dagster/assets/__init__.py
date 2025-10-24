"""
Dagster assets for tourism data pipeline tables.
"""

from assets.tables import (
    reviews_table,
    destinations_table,
    offerings_table,
    review_aspects_table,
)

from assets.checks import (
    check_reviews_have_content,
    check_reviews_language_distribution,
    check_destinations_referenced,
    check_offerings_referenced,
    check_aspects_have_evidence,
    check_aspects_per_review,
    check_no_orphaned_aspects,
)

__all__ = [
    # Table assets
    "reviews_table",
    "destinations_table", 
    "offerings_table",
    "review_aspects_table",
    # Asset checks
    "check_reviews_have_content",
    "check_reviews_language_distribution",
    "check_destinations_referenced",
    "check_offerings_referenced",
    "check_aspects_have_evidence",
    "check_aspects_per_review",
    "check_no_orphaned_aspects",
]
