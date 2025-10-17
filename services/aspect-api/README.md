# Aspect Extraction API (Cloud Run)

FastAPI service that exposes a POST /extract endpoint for aspect extraction using OpenAI. Matches the schema expected by the Dagster pipeline.

## Endpoints

- `GET /health` → `{ "status": "ok" }`
- `POST /extract` → Extract aspects. Auth via `X-API-Key` header.

## Request schema

```
{
  "id": "optional string",
  "text": "required string",
  "options": {
  "model": "string (optional, default gpt-5-mini)",
    "prompt_version": "string (optional)",
    "max_aspects": 10,
    "language": "en"
  }
}
```

## Response schema

```
{
  "id": "echoed id or null",
  "aspects": [
    {
      "aspect": "string",
      "category": "string|null",
      "evidence_span": "string",
      "start": 0,
      "end": 10,
      "confidence": 0.9
    }
  ],
  "meta": {
  "model": "gpt-5-mini",
    "prompt_version": "v1",
    "tokens_estimate": 123,
    "latency_ms": 800
  }
}
```

## Local run (optional)

```
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

$env:OPENAI_API_KEY = "<your-openai-key>"
$env:ASPECT_API_KEY = "dev-shared-key"
$env:ASPECT_REQUEST_MODEL = "gpt-5-mini"
$env:ASPECT_PROMPT_VERSION = "v1"

python -m uvicorn app.main:app --reload --port 8080
```

Test:

```
$Body = @{ id = "demo_1"; text = "Battery lasts two days but the camera is mediocre." } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/extract" -Headers @{ "X-API-Key" = "dev-shared-key"; "Content-Type" = "application/json" } -Body $Body
```

## Build and deploy on Cloud Run

```
gcloud auth login
gcloud config set project <PROJECT_ID>
gcloud config set run/region <REGION>

gcloud services enable run.googleapis.com cloudbuild.googleapis.com secretmanager.googleapis.com

# Create secrets and set values
Set-Content -NoNewline -Path openai_key.txt -Value "<your-openai-key>"
Set-Content -NoNewline -Path aspect_api_key.txt -Value "<shared-api-key>"

gcloud secrets create OPENAI_API_KEY --replication-policy="automatic"
gcloud secrets create ASPECT_API_KEY --replication-policy="automatic"

gcloud secrets versions add OPENAI_API_KEY --data-file=openai_key.txt
gcloud secrets versions add ASPECT_API_KEY --data-file=aspect_api_key.txt

Remove-Item openai_key.txt, aspect_api_key.txt

# Grant Secret Manager access to the Cloud Run runtime service account (Option A - recommended)

```powershell
# Set your project id
$PROJECT_ID = "<your-project-id>"  # e.g. artefact-assessment
$PROJECT_NUMBER = (gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
$RUN_SA = "$PROJECT_NUMBER-compute@developer.gserviceaccount.com"

# Allow the runtime service account to access each secret
gcloud secrets add-iam-policy-binding OPENAI_API_KEY `
  --member "serviceAccount:$RUN_SA" `
  --role "roles/secretmanager.secretAccessor"

gcloud secrets add-iam-policy-binding ASPECT_API_KEY `
  --member "serviceAccount:$RUN_SA" `
  --role "roles/secretmanager.secretAccessor"

# (Optional) verify which service account Cloud Run uses after the first deploy
# gcloud run services describe aspect-api --region <REGION> --format="value(spec.template.spec.serviceAccountName)"
```

# Build
gcloud builds submit --tag us-central1-docker.pkg.dev/artefact-assessment/aspect-api/aspect-api:latest .

# Deploy
gcloud run deploy aspect-api --image us-central1-docker.pkg.dev/artefact-assessment/aspect-api/aspect-api:latest --platform managed --region us-central1 --allow-unauthenticated --set-env-vars "ASPECT_REQUEST_MODEL=gpt-5-mini,ASPECT_PROMPT_VERSION=v1" --set-secrets "OPENAI_API_KEY=OPENAI_API_KEY:latest,ASPECT_API_KEY=ASPECT_API_KEY:latest"

# Get URL and test
$svc = gcloud run services describe aspect-api --format="value(status.url)"
$Body = @{ id = "rev_1"; text = "Price is fair but shipping was slow." } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "$svc/extract" -Headers @{ "X-API-Key" = "<shared-api-key>"; "Content-Type" = "application/json" } -Body $Body
```

## Prerequisites

- Python 3.11
- An OpenAI API key with access to your chosen model (default: gpt-5-mini)
- Google Cloud SDK (gcloud) if deploying to Cloud Run
- Docker (optional for local container run)

## Environment variables

- `OPENAI_API_KEY`: OpenAI secret key (required)
- `ASPECT_API_KEY`: shared header key for the service (required)
- `ASPECT_REQUEST_MODEL`: default model (optional, default: `gpt-5-mini`)
- `ASPECT_PROMPT_VERSION`: prompt version tag (optional, default: `v1`)
- `LOG_LEVEL`: `INFO`/`DEBUG` (optional)

## Optional: local Docker run

```powershell
# Build image locally (from services\aspect-api)
docker build -t aspect-api:local .

# Run container
docker run -p 8080:8080 `
  -e OPENAI_API_KEY=<your-openai-key> `
  -e ASPECT_API_KEY=dev-shared-key `
  -e ASPECT_REQUEST_MODEL=gpt-5-mini `
  -e ASPECT_PROMPT_VERSION=v1 `
  aspect-api:local
```

## Wire Dagster to the API

Set these where Dagster runs (container/env):

- `ASPECT_API_URL=https://<your-cloud-run-host>/extract`
- `ASPECT_API_KEY=<shared-api-key>`
- `ASPECT_API_TIMEOUT=30`
- Optional: `ASPECT_REQUEST_MODEL`, `ASPECT_PROMPT_VERSION` (affects cache keys)

## Troubleshooting

- 401 unauthorized: `X-API-Key` missing or incorrect.
- 400 bad request to OpenAI: model/params unsupported for your key. Default model is `gpt-5-mini`; ensure access, or override via `options.model`.
- 429 rate limited: transient; the service retries automatically on rate limits/connectivity.
- 502 upstream error: transient upstream issue; retries apply.
- Logs: set `LOG_LEVEL=DEBUG` to see structured logs. Include `X-Request-ID` in your request; the service echoes it and uses it in logs.
