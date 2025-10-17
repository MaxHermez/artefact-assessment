# PyABSA Aspect Extraction API

A FastAPI service that provides aspect-based sentiment analysis using [PyABSA](https://github.com/yangheng95/PyABSA)'s multilingual model. Designed for Cloud Run deployment with model pre-loading and batch processing support.

## Features

- **Single and batch processing**: `/extract` for single texts, `/extract-batch` for up to 32 texts
- **Pre-loaded models**: PyABSA multilingual model downloaded during Docker build for fast cold starts
- **Consistent API**: Same response schema as the LLM-based aspect service
- **Structured logging**: Request tracing and performance metrics
- **Auto-scaling**: Optimized for Cloud Run with proper resource sizing

## API Endpoints

### POST /extract
Extract aspects from a single text.

**Request:**
```json
{
  "id": "review_123",
  "text": "The battery life is terrible but the camera is excellent."
}
```

**Response:**
```json
{
  "id": "review_123",
  "aspects": [
    {
      "aspect": "battery life",
      "evidence_span": "battery life",
      "polarity": "Negative",
      "confidence": 0.9925
    },
    {
      "aspect": "camera",
      "evidence_span": "camera",
      "polarity": "Positive",
      "confidence": 0.9025
    }
  ],
  "meta": {
    "model": "pyabsa-multilingual",
    "prompt_version": "v1",
    "latency_ms": 245
  }
}
```

### POST /extract-batch
Extract aspects from multiple texts (max 32 per request).

**Request:**
```json
{
  "items": [
    {
      "id": "review_1",
      "text": "Great battery, poor screen quality."
    },
    {
      "id": "review_2", 
      "text": "Excellent camera and fast processor."
    }
  ],
  "options": {}
}
```

**Response:**
```json
{
  "items": [
    {
      "id": "review_1",
      "aspects": [...],
      "meta": {...}
    },
    {
      "id": "review_2", 
      "aspects": [...],
      "meta": {...}
    }
  ]
}
```

### GET /health
Health check endpoint.

## Local Development

### Prerequisites
- Python 3.11+
- 2-4 GB RAM (for PyABSA model)
- CUDA-compatible GPU (optional, falls back to CPU)

### Setup and Run

```bash
# Navigate to service directory
cd services/pyabsa-api

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export PYABSA_API_KEY="dev-shared-key"
export LOG_LEVEL="INFO"

# Run the service
uvicorn app.main:app --host 0.0.0.0 --port 8080 --workers 1
```

**Note:** First run will download PyABSA models (~500MB), which may take 2-5 minutes depending on connection.

### Test the API

```bash
# Health check
curl http://localhost:8080/health

# Single extraction
curl -X POST http://localhost:8080/extract \
  -H "Authorization: Bearer dev-shared-key" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "test_1",
    "text": "The battery life is amazing but the screen is too dim."
  }'

# Batch extraction
curl -X POST http://localhost:8080/extract-batch \
  -H "Authorization: Bearer dev-shared-key" \
  -H "Content-Type: application/json" \
  -d '{
    "items": [
      {"id": "test_1", "text": "Great camera quality."},
      {"id": "test_2", "text": "Poor battery performance."}
    ]
  }'
```

## Docker Build and Run

### Build with Model Pre-loading

```bash
# Build image (includes model download)
docker build -t pyabsa-api .

# This will:
# 1. Install PyABSA and dependencies
# 2. Download multilingual model during build (~500MB)
# 3. Cache models in the image for fast startup
```

### Run Container Locally

```bash
# Run with environment variables
docker run -p 8080:8080 \
  -e PYABSA_API_KEY="dev-shared-key" \
  -e LOG_LEVEL="INFO" \
  pyabsa-api

# Test the containerized service
curl http://localhost:8080/health
```

## Cloud Run Deployment

### Build and Deploy

```bash
# Set your GCP project
export GCP_PROJECT_ID="artefact-assessment"
export SERVICE_NAME="pyabsa-api"
export REGION="us-central1"
```

or on windows powershell:
```powershell
$GCP_PROJECT_ID = "artefact-assessment"
$SERVICE_NAME = "pyabsa-api"
$REGION = "us-central1"
$TAG = "$REGION-docker.pkg.dev/$GCP_PROJECT_ID/$SERVICE_NAME/$SERVICE_NAME"
```

```bash
# Build and push to Container Registry
gcloud builds submit --tag $REGION-docker.pkg.dev/$GCP_PROJECT_ID/$SERVICE_NAME/$SERVICE_NAME:latest .
# gcloud builds submit --tag us-central1-docker.pkg.dev/artefact-assessment/pyabsa-api/pyabsa-api:latest .

# Deploy to Cloud Run with optimized settings
gcloud run deploy $SERVICE_NAME \
  --image gcr.io/$GCP_PROJECT_ID/$SERVICE_NAME \
  --platform managed \
  --region $REGION \
  --allow-unauthenticated \
  --set-env-vars LOG_LEVEL=INFO \
  --set-secrets PYABSA_API_KEY=pyabsa-api-key:latest \
  --memory 6Gi \
  --cpu 2 \
  --concurrency 1 \
  --timeout 300 \
  --min-instances 0 \
  --max-instances 10
```

```powershell
gcloud run deploy $SERVICE_NAME `
  --image $TAG `
  --platform managed `
  --region $REGION `
  --allow-unauthenticated `
  --set-env-vars LOG_LEVEL=INFO `
  --set-secrets PYABSA_API_KEY=ASPECT_API_KEY:latest `
  --memory 6Gi `
  --cpu 2 `
  --concurrency 1 `
  --timeout 300 `
  --min-instances 0 `
  --max-instances 10
```

### Resource Sizing

| Resource | Recommendation | Reasoning |
|----------|----------------|-----------|
| **Memory** | 6Gi | PyABSA model + PyTorch overhead |
| **CPU** | 2 vCPUs | Balanced for inference workload |
| **Concurrency** | 1 | Thread-safety; model state |
| **Timeout** | 300s | Batch processing buffer |
| **Min instances** | 0-1 | 0 for cost, 1 for latency |

### Create Secret for API Key

```bash
# Create the API key secret
echo -n "your-production-api-key" | gcloud secrets create pyabsa-api-key --data-file=-

# Grant access to Cloud Run service account
gcloud secrets add-iam-policy-binding pyabsa-api-key \
  --member="serviceAccount:$(gcloud run services describe $SERVICE_NAME --region=$REGION --format='value(spec.template.spec.serviceAccountName)')" \
  --role="roles/secretmanager.secretAccessor"
```

### Get Service URL

```bash
# Get the deployed service URL
gcloud run services describe $SERVICE_NAME --region=$REGION --format='value(status.url)'
# Output: https://pyabsa-api-<hash>-<region>.a.run.app
```

## Integration with Dagster

Add the PyABSA service URL to your Dagster environment:

```bash
# In .env or Dagster config
PYABSA_API_URL=https://pyabsa-api-<hash>-<region>.a.run.app
PYABSA_API_KEY=your-production-api-key
PYABSA_API_TIMEOUT=60
```

Then use the same Dagster ops with the PyABSA URL:

```python
# In Dagster job config
{
  "ops": {
    "extract_aspects_batch_via_api": {
      "config": {
        "api_url": "${PYABSA_API_URL}/extract-batch"  # Note: batch endpoint
      }
    }
  }
}
```

The response format is compatible with your existing `upsert_review_aspects` op, but will be stored with `approach="pyabsa"`.

## Performance Notes

- **Cold start**: ~5-10 seconds (models pre-loaded in image)
- **Warm inference**: ~100-500ms per text (depending on length)
- **Batch processing**: More efficient than individual calls due to model reuse
- **Memory usage**: ~2-2.5GB resident (model + framework)
- **GPU acceleration**: Automatic if available, falls back to CPU

## Troubleshooting

### Common Issues

**Model download failures:**
```bash
# Check internet connection during build
# Retry docker build if download interrupted
docker build --no-cache -t pyabsa-api .
```

**Memory issues:**
```bash
# Increase Cloud Run memory to 4Gi
gcloud run services update $SERVICE_NAME --memory 4Gi --region $REGION
```

**Authentication errors:**
```bash
# Verify API key in request headers
curl -H "Authorization: Bearer your-api-key" http://localhost:8080/health
```

**Timeout issues:**
```bash
# Increase Cloud Run timeout for large batches
gcloud run services update $SERVICE_NAME --timeout 600 --region $REGION
```

### Logs and Monitoring

```bash
# View Cloud Run logs
gcloud logs tail "resource.type=cloud_run_revision AND resource.labels.service_name=$SERVICE_NAME" --region=$REGION

# Monitor performance
gcloud run services describe $SERVICE_NAME --region=$REGION
```

## Development Notes

- **Thread safety**: Single worker, concurrency=1 recommended
- **Batch size**: Limited to 32 items to balance throughput and latency
- **Model caching**: Models cached in `/root/.pyabsa/` during build
- **Error handling**: Graceful degradation for failed items in batch
- **Logging**: Structured JSON logs with request tracing