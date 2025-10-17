"""
PyABSA-based Aspect Extraction API

Provides aspect-based sentiment analysis using PyABSA's multilingual model.
Supports both single text and batch processing to amortize model initialization costs.
"""

import os
import time
import logging
import asyncio
from threading import Thread
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header, Request
from pydantic import BaseModel, Field
import uvicorn

# Force slow tokenizers globally to avoid missing attributes on some Fast tokenizers
# os.environ.setdefault("TRANSFORMERS_NO_FAST_TOKENIZER", "1")

# Configure logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global extractor instance and load state
extractor = None
_model_load_started = False
_model_load_error: Optional[str] = None

def load_pyabsa_model():
    """Load the PyABSA model once at startup."""
    global extractor, _model_load_error
    try:
        from pyabsa import AspectTermExtraction as ATEPC
        logger.info("Loading PyABSA multilingual model...")
        start_time = time.time()
        extractor = ATEPC.AspectExtractor('multilingual', auto_device=True)
        load_time = time.time() - start_time
        logger.info(f"PyABSA model loaded successfully in {load_time:.2f}s")
    except Exception as e:
        _model_load_error = str(e)
        logger.error(f"Failed to load PyABSA model: {e}", exc_info=True)

def _start_model_load_in_background():
    """Kick off model loading in a background thread to avoid blocking server startup."""
    global _model_load_started
    if _model_load_started:
        return
    _model_load_started = True

    def _target():
        try:
            load_pyabsa_model()
        except Exception:
            # error already logged inside load_pyabsa_model
            pass

    Thread(target=_target, daemon=True).start()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown without blocking port binding."""
    # Startup: start model loading in background so the server can bind to the PORT quickly
    _start_model_load_in_background()
    yield
    # Shutdown
    logger.info("Application shutting down")

app = FastAPI(
    title="PyABSA Aspect Extraction API",
    description="Aspect-based sentiment analysis using PyABSA multilingual model",
    version="1.0.0",
    lifespan=lifespan,
)

# Simple header key auth (same as aspect-api)
API_KEY = os.getenv("PYABSA_API_KEY", "dev-shared-key")

# Request/Response models
class SingleExtractRequest(BaseModel):
    id: str = Field(..., description="Unique identifier for the text")
    text: str = Field(..., description="Text to analyze for aspects and sentiment")

class BatchExtractRequest(BaseModel):
    items: List[SingleExtractRequest] = Field(..., description="List of texts to analyze")
    options: Optional[Dict[str, Any]] = Field(default={}, description="Additional options")

class AspectResult(BaseModel):
    aspect: str = Field(..., description="Extracted aspect term")
    evidence_span: str = Field(..., description="Text span supporting the aspect")
    polarity: str = Field(..., description="Sentiment polarity: Positive, Negative, or Neutral")
    confidence: float = Field(..., description="Confidence score (0-1)")

class MetaInfo(BaseModel):
    model: str = Field(..., description="Model used for extraction")
    prompt_version: str = Field(..., description="Version identifier")
    latency_ms: int = Field(..., description="Processing time in milliseconds")

class SingleExtractResponse(BaseModel):
    id: str = Field(..., description="Input identifier")
    aspects: List[AspectResult] = Field(..., description="Extracted aspects with sentiment")
    meta: MetaInfo = Field(..., description="Metadata about the extraction")

class BatchExtractResponse(BaseModel):
    items: List[SingleExtractResponse] = Field(..., description="Results for each input")

# Request ID middleware
@app.middleware("http")
async def add_request_id(request: Request, call_next):
    """Add request ID for tracing."""
    request_id = f"req_{int(time.time() * 1000)}"
    request.state.request_id = request_id
    
    start_time = time.time()
    response = await call_next(request)
    duration_ms = int((time.time() - start_time) * 1000)
    
    response.headers["X-Request-ID"] = request_id
    logger.info(f"Request {request_id} completed in {duration_ms}ms")
    return response

def extract_aspects_from_pyabsa_result(pyabsa_result: Dict, text_id: str, processing_time_ms: int) -> SingleExtractResponse:
    """Convert PyABSA output to our standard format.
    
    PyABSA returns a dict with structure:
    {
        'sentence': str,
        'tokens': List[str],
        'aspect': List[str],  # e.g., ['battery life', 'camera']
        'sentiment': List[str],  # e.g., ['Negative', 'Positive']
        'confidence': List[float],  # e.g., [0.9925, 0.9025]
        'position': List[List[int]],  # e.g., [[1, 2], [7]]
        'IOB': List[str],
        'probs': List[List[float]]
    }
    """
    aspects = []
    
    # Extract all relevant fields
    aspect_terms = pyabsa_result.get('aspect', [])
    sentiments = pyabsa_result.get('sentiment', [])
    confidences = pyabsa_result.get('confidence', [])
    positions = pyabsa_result.get('position', [])
    tokens = pyabsa_result.get('tokens', [])
    
    # Handle the case where PyABSA may return empty results
    if not aspect_terms:
        return SingleExtractResponse(
            id=text_id,
            aspects=[],
            meta=MetaInfo(
                model="pyabsa-multilingual",
                prompt_version="v1",
                latency_ms=processing_time_ms
            )
        )
    
    for i, aspect in enumerate(aspect_terms):
        # Get corresponding sentiment and confidence, with defaults
        sentiment = sentiments[i] if i < len(sentiments) else 'Neutral'
        confidence = confidences[i] if i < len(confidences) else 0.0
        
        # Extract evidence span from tokens using positions
        # positions is a list of token indices, e.g., [1, 2] or [7]
        evidence_span = aspect  # Default to aspect term itself
        if i < len(positions) and tokens:
            position_indices = positions[i]
            if position_indices and isinstance(position_indices, list):
                try:
                    # Extract tokens at the specified positions
                    evidence_tokens = [tokens[idx] for idx in position_indices if idx < len(tokens)]
                    if evidence_tokens:
                        evidence_span = ' '.join(evidence_tokens)
                except (IndexError, TypeError) as e:
                    logger.warning(f"Failed to extract evidence span for aspect '{aspect}': {e}")
                    # Keep default evidence_span as aspect
        
        aspects.append(AspectResult(
            aspect=aspect,
            evidence_span=evidence_span,
            polarity=sentiment,
            confidence=float(confidence)
        ))
    
    return SingleExtractResponse(
        id=text_id,
        aspects=aspects,
        meta=MetaInfo(
            model="pyabsa-multilingual",
            prompt_version="v1",
            latency_ms=processing_time_ms
        )
    )

def process_single_text(text: str, text_id: str) -> SingleExtractResponse:
    """Process a single text with PyABSA.
    
    PyABSA's predict() method:
    - Input: list of strings (even for single text)
    - Output: list of dicts (one dict per input text)
    """
    if not extractor:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    start_time = time.time()
    try:
        # PyABSA expects a list of texts, returns a list of result dicts
        results = extractor.predict(
            [text],
            print_result=False,
            save_result=False,
            ignore_error=True,
            pred_sentiment=True
        )
        
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        # Check if we got results
        if not results or len(results) == 0:
            logger.warning(f"PyABSA returned empty results for {text_id}")
            return SingleExtractResponse(
                id=text_id,
                aspects=[],
                meta=MetaInfo(
                    model="pyabsa-multilingual",
                    prompt_version="v1",
                    latency_ms=processing_time_ms
                )
            )
        
        # Extract the first result (since we only sent one text)
        pyabsa_result = results[0]
        logger.debug(f"PyABSA result for {text_id}: {pyabsa_result}")
        
        return extract_aspects_from_pyabsa_result(pyabsa_result, text_id, processing_time_ms)
        
    except Exception as e:
        processing_time_ms = int((time.time() - start_time) * 1000)
        logger.error(f"PyABSA extraction failed for {text_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail=f"Aspect extraction failed: {str(e)}"
        )

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": ("healthy" if extractor is not None else ("error" if _model_load_error else "starting")),
        "model_loaded": extractor is not None,
        "model_error": _model_load_error,
        "timestamp": int(time.time())
    }

@app.post("/extract", response_model=SingleExtractResponse)
async def extract_aspects(
    request: SingleExtractRequest,
    x_api_key: Optional[str] = Header(None)
):
    """Extract aspects and sentiment from a single text."""
    if not x_api_key or x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="invalid api key")
    logger.info(f"Processing single extraction for ID: {request.id}")
    return process_single_text(request.text, request.id)

@app.post("/extract-batch", response_model=BatchExtractResponse)
async def extract_aspects_batch(
    request: BatchExtractRequest,
    x_api_key: Optional[str] = Header(None)
):
    """Extract aspects and sentiment from multiple texts in batch."""
    if not x_api_key or x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="invalid api key")
    logger.info(f"Processing batch extraction for {len(request.items)} items")
    
    if len(request.items) > 32:
        raise HTTPException(
            status_code=400, 
            detail="Batch size exceeds maximum of 32 items"
        )
    
    results = []
    for item in request.items:
        try:
            result = process_single_text(item.text, item.id)
            results.append(result)
        except HTTPException as e:
            # Log error but continue with other items
            logger.error(f"Failed to process item {item.id}: {e.detail}")
            results.append(SingleExtractResponse(
                id=item.id,
                aspects=[],
                meta=MetaInfo(
                    model="pyabsa-multilingual",
                    prompt_version="v1",
                    latency_ms=0
                )
            ))
    
    return BatchExtractResponse(items=results)

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8080,
        log_level=LOG_LEVEL.lower(),
        workers=1
    )