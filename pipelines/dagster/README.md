# Dagster Tourism Data Pipelines

Three modular pipelines for processing tourism review data.

## Pipelines

1. **`ingestion_pipeline`** - Load CSV from GCS to Supabase
2. **`parsing_pipeline`** - Extract destinations/offerings from tags
3. **`translation_pipeline`** - Translate non-English reviews via OpenAI
- **`aspect_extraction_pipeline`** - Extract aspects via external HTTP API and persist to Supabase

### Orchestration flow (chained)

End-to-end sequence triggered by GCS Pub/Sub:

- GCS sensor → `ingestion_pipeline`
- On success → `parsing_pipeline`
- On success → `translation_pipeline`
- On success → both `aspect_extraction_pipeline` (LLM) and `pyabsa_aspect_extraction_pipeline` (PyABSA) in parallel

Enable these sensors in the Dagster UI (Automation tab):

- `gcs_ingestion_sensor` (kicks off the chain)
- `ingestion_to_parsing`
- `parsing_to_translation`
- `translation_to_aspects`

Optional: disable `gcs_parsing_sensor` if you don’t want parsing to be triggered independently by mapping uploads.

### Aspect extraction pipeline

This job batches reviews that do not yet have aspects and calls a stateless HTTP endpoint to extract aspects using an LLM. Results are stored in `review_aspect_extractions`.

Environment variables required at runtime:

- `ASPECT_API_URL` (e.g., `https://<service>-<hash>-<region>.a.run.app/extract`)
- `ASPECT_API_KEY`
- `ASPECT_API_TIMEOUT` (optional, default `30` seconds)

Run it from the Dagster UI or CLI once the Cloud Run service is available. The job will automatically create the `review_aspect_extractions` table if it does not exist.

## Quick Start

```bash
# 1. Install
pip install -r requirements.txt

# 2. Configure
cp .env.example .env
# Edit .env with your values (set GCP_PROJECT_ID for auto-trigger)

# 3. Run Dagster (daemon required for sensors)
dagster dev
# Open http://localhost:3000

# 4. Enable sensors in Dagster UI
# Go to "Automation" > Turn on sensors
```

## GCS Auto-Trigger with Sensors

**Dagster sensors automatically monitor GCS Pub/Sub** for file uploads!

**Setup (one-time):**
```bash
export GCP_PROJECT_ID=your-project
export GCS_BUCKET_NAME=your-bucket
export DEPLOYMENT_ENV=dev  # or staging/prod

# Start Dagster - auto-creates Pub/Sub resources
dagster dev

# Grant IAM permission (command shown in logs)
gcloud projects add-iam-policy-binding YOUR-PROJECT \
  --member=serviceAccount:service-XXX@gs-project-accounts.iam.gserviceaccount.com \
  --role=roles/pubsub.publisher

# Enable sensors in UI: Automation tab > Turn on sensors
```

**How it works:**
- `gcs_ingestion_sensor` - Monitors `raw-data/*` files → triggers ingestion
- `gcs_parsing_sensor` - Monitors `mappings/*` files → triggers parsing
- Sensors poll every 30 seconds
- No separate process needed - runs in Dagster daemon!

**Cleanup:** `python cleanup_gcs_resources.py`

## Setup

## Manual Execution

```bash
# Via UI
dagster dev  # Open http://localhost:3000

# Via CLI
dagster job execute -f repository.py -j ingestion_pipeline
dagster job execute -f repository.py -j parsing_pipeline
dagster job execute -f repository.py -j translation_pipeline
```

## Docker

```bash
# Start (from repo root)
docker compose up -d --build

# View logs
docker compose logs dagster-webserver

# UI: http://localhost:3001
```

## Environment Variables

See `.env.example` for all options. Key ones:
```bash
# Required
GCP_PROJECT_ID=your-project
GCS_BUCKET_NAME=your-bucket
SUPABASE_DB_HOST=localhost
SUPABASE_DB_PASSWORD=your-password
OPENAI_API_KEY=sk-...

# Optional
DEPLOYMENT_ENV=dev  # For multi-env deployments
AUTO_SETUP_GCS_NOTIFICATIONS=true  # Auto-setup Pub/Sub
```
