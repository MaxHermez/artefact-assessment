FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies including git (required by pyabsa)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set environment variable to silence git warnings
ENV GIT_PYTHON_REFRESH=quiet

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download PyABSA models at build time to avoid cold start delays
RUN python -c "\
from pyabsa import AspectTermExtraction as ATEPC; \
print('Pre-downloading PyABSA multilingual model...'); \
extractor = ATEPC.AspectExtractor('multilingual', auto_device=False); \
print('Model download complete')"

# Copy application code
COPY app/ ./app/

# Expose port
EXPOSE 8080

# Set environment variables
ENV LOG_LEVEL=INFO
ENV PYTHONPATH=/app
ENV TRANSFORMERS_NO_FAST_TOKENIZER=1

# Run the application (honor Cloud Run PORT env var, default to 8080)
ENV PORT=8080
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080} --workers 1"]