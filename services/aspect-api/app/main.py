import os
import json
import time
import logging
import uuid
import asyncio
from enum import Enum
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field
from tenacity import retry, wait_exponential_jitter, stop_after_attempt, retry_if_exception_type
from openai import OpenAI
from openai import RateLimitError, APIError, APIConnectionError, AuthenticationError, BadRequestError

# Env/config
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger("aspect_api")

ASPECT_API_KEY = os.getenv("ASPECT_API_KEY")  # required (secret)
DEFAULT_MODEL = os.getenv("ASPECT_REQUEST_MODEL", "gpt-5-mini")
DEFAULT_PROMPT_VERSION = os.getenv("ASPECT_PROMPT_VERSION", "v1")

if not os.getenv("OPENAI_API_KEY"):
    # Let startup fail fast in Cloud Run if secret not set
    logger.error("OPENAI_API_KEY is not set")
    raise RuntimeError("OPENAI_API_KEY is not set")

if not ASPECT_API_KEY:
    logger.error("ASPECT_API_KEY is not set")
    raise RuntimeError("ASPECT_API_KEY is not set")

client = OpenAI()  # uses OPENAI_API_KEY from env

SYSTEM_PROMPT = """You are an information extraction system.
Extract concise, non-overlapping product/service aspects (noun phrases) and polarities (positive, neutral, negative) from reviews.
Return STRICT JSON only. Evidence spans must be verbatim substrings of the input.
If none, return {"aspects": []}.
"""

USER_INSTRUCTIONS = """Task:
- Identify aspects mentioned.
- For each aspect, include:
  - aspect: short phrase
  - polarity: "positive", "neutral", or "negative"
  - evidence_span: the exact substring supporting the aspect
  - confidence: 0.0-1.0 subjective confidence

Rules:
- Avoid duplicates; merge overlaps, pick the most specific phrasing.
- Use null for unknowns or invalids.

Output JSON schema:
{
  "aspects": [
    {
      "aspect": "string",
      "polarity": "string",
      "evidence_span": "string",
      "confidence": "number"
    }
  ]
}
"""

class ExtractOptions(BaseModel):
    model: Optional[str] = Field(default=None)
    prompt_version: Optional[str] = Field(default=None)
    max_aspects: Optional[int] = Field(default=None, ge=1, le=50)
    language: Optional[str] = Field(default=None, min_length=2, max_length=8)

class ExtractRequest(BaseModel):
    id: Optional[str] = None
    text: str = Field(min_length=1)
    options: Optional[ExtractOptions] = None

class Polarity(str, Enum):
    positive = "positive"
    neutral = "neutral"
    negative = "negative"

class AspectItem(BaseModel):
    aspect: str = Field(description="The extracted aspect phrase")
    polarity: Polarity = Field(description="The polarity of the extracted aspect")
    evidence_span: str = Field(description="The exact substring from the input text that supports the aspect")
    confidence: Optional[float] = Field(description="The confidence score for the extracted aspect")

class ExtractResponse(BaseModel):
    id: Optional[str] = None
    aspects: List[AspectItem]

app = FastAPI(title="Aspect Extraction API", version="1.0.0")


@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    # Correlate logs with a request ID
    req_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    request.state.req_id = req_id

    start = time.time()
    try:
        response = await call_next(request)
        duration_ms = int((time.time() - start) * 1000)
        response.headers["X-Request-ID"] = req_id
        logger.info(
            "request completed",
            extra={
                "props": {
                    "req_id": req_id,
                    "method": request.method,
                    "path": request.url.path,
                    "status": response.status_code,
                    "duration_ms": duration_ms,
                    "client": request.client.host if request.client else None,
                }
            }
        )
        return response
    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        logger.exception(
            "unhandled exception during request",
            extra={
                "props": {
                    "req_id": req_id,
                    "method": request.method,
                    "path": request.url.path,
                    "duration_ms": duration_ms,
                }
            }
        )
        # Re-raise to let FastAPI produce the error response
        raise

def _build_messages(text: str) -> List[Dict[str, str]]:
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": USER_INSTRUCTIONS + "\n\nInput:\n" + text},
    ]

@retry(
    wait=wait_exponential_jitter(initial=1, max=20),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((RateLimitError, APIConnectionError, APIError)),
)
def _call_openai(messages: List[Dict[str, str]], model: str) -> Any:
    return client.chat.completions.create(
        model=model,
        messages=messages,
        response_format={"type": "json_object"},
    )

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/extract", response_model=ExtractResponse)
def extract(req: ExtractRequest, request: Request, x_api_key: Optional[str] = Header(None)):
    # Simple header key auth
    if not x_api_key or x_api_key != ASPECT_API_KEY:
        raise HTTPException(status_code=401, detail="invalid api key")

    model = req.options.model if req.options and req.options.model else DEFAULT_MODEL
    prompt_version = req.options.prompt_version if req.options and req.options.prompt_version else DEFAULT_PROMPT_VERSION

    if not req.text or not req.text.strip():
        raise HTTPException(status_code=400, detail="text is required")

    req_id = getattr(request.state, "req_id", "unknown")
    text_len = len(req.text) if req.text else 0
    logger.info(
        "extract request",
        extra={
            "props": {
                "req_id": req_id,
                "id": req.id,
                "model": model,
                "prompt_version": prompt_version,
                "text_len": text_len,
            }
        }
    )

    messages = _build_messages(req.text)
    started = time.time()

    try:
        resp = _call_openai(messages, model=model)
        content = resp.choices[0].message.content or "{}"
        data = json.loads(content)
    except BadRequestError as e:
        # Typically invalid model or parameters; surface clearly to caller and logs
        logger.exception("OpenAI bad request", extra={"props": {"req_id": req_id}})
        raise HTTPException(status_code=400, detail="bad request to OpenAI: check model/params") from e
    except AuthenticationError as e:
        logger.exception("OpenAI auth error", extra={"props": {"req_id": req_id}})
        raise HTTPException(status_code=500, detail="OpenAI auth error") from e
    except RateLimitError as e:
        logger.warning("OpenAI rate limited", extra={"props": {"req_id": req_id}})
        raise HTTPException(status_code=429, detail="rate limited") from e
    except (APIConnectionError, APIError) as e:
        logger.exception("OpenAI upstream error", extra={"props": {"req_id": req_id}})
        raise HTTPException(status_code=502, detail="upstream error") from e
    except Exception as e:
        logger.exception("Unhandled exception in extract", extra={"props": {"req_id": req_id}})
        raise HTTPException(status_code=500, detail=str(e)) from e

    aspects = data.get("aspects", [])
    # Normalize/validate minimal fields
    for a in aspects:
        a.setdefault("category", None)
        a.setdefault("confidence", None)

    latency_ms = int((time.time() - started) * 1000)
    tokens_estimate = getattr(resp, "usage", None).prompt_tokens if hasattr(resp, "usage") else None

    result = {
        "id": req.id,
        "aspects": aspects,
        "meta": {
            "model": model,
            "prompt_version": prompt_version,
            "tokens_estimate": tokens_estimate,
            "latency_ms": latency_ms,
        },
    }
    logger.info(
        "extract response",
        extra={
            "props": {
                "req_id": req_id,
                "id": req.id,
                "aspects_count": len(aspects),
                "latency_ms": latency_ms,
            }
        }
    )
    return result


class BatchExtractRequest(BaseModel):
    items: List[ExtractRequest]


def _process_single_item(item: ExtractRequest, req_id: str) -> Dict[str, Any]:
    """Process a single item - designed to run in thread pool."""
    model = item.options.model if item.options and item.options.model else DEFAULT_MODEL
    prompt_version = item.options.prompt_version if item.options and item.options.prompt_version else DEFAULT_PROMPT_VERSION

    if not item.text or not item.text.strip():
        return {
            "id": item.id,
            "aspects": [],
            "meta": {
                "model": model,
                "prompt_version": prompt_version,
                "tokens_estimate": None,
                "latency_ms": 0,
                "error": "text is required",
            },
        }

    messages = _build_messages(item.text)
    started = time.time()

    try:
        resp = _call_openai(messages, model=model)
        content = resp.choices[0].message.content or "{}"
        data = json.loads(content)
        aspects = data.get("aspects", [])
        for a in aspects:
            a.setdefault("category", None)
            a.setdefault("confidence", None)
        tokens_estimate = getattr(resp, "usage", None).prompt_tokens if hasattr(resp, "usage") else None
        latency_ms = int((time.time() - started) * 1000)
        return {
            "id": item.id,
            "aspects": aspects,
            "meta": {
                "model": model,
                "prompt_version": prompt_version,
                "tokens_estimate": tokens_estimate,
                "latency_ms": latency_ms,
            },
        }
    except BadRequestError:
        logger.exception("OpenAI bad request (batch)", extra={"props": {"req_id": req_id, "id": item.id}})
        latency_ms = int((time.time() - started) * 1000)
        return {
            "id": item.id,
            "aspects": [],
            "meta": {"model": model, "prompt_version": prompt_version, "tokens_estimate": None, "latency_ms": latency_ms, "error": "bad request"},
        }
    except AuthenticationError:
        logger.exception("OpenAI auth error (batch)", extra={"props": {"req_id": req_id, "id": item.id}})
        latency_ms = int((time.time() - started) * 1000)
        return {
            "id": item.id,
            "aspects": [],
            "meta": {"model": model, "prompt_version": prompt_version, "tokens_estimate": None, "latency_ms": latency_ms, "error": "auth error"},
        }
    except RateLimitError:
        logger.warning("OpenAI rate limited (batch)", extra={"props": {"req_id": req_id, "id": item.id}})
        latency_ms = int((time.time() - started) * 1000)
        return {
            "id": item.id,
            "aspects": [],
            "meta": {"model": model, "prompt_version": prompt_version, "tokens_estimate": None, "latency_ms": latency_ms, "error": "rate limited"},
        }
    except (APIConnectionError, APIError):
        logger.exception("OpenAI upstream error (batch)", extra={"props": {"req_id": req_id, "id": item.id}})
        latency_ms = int((time.time() - started) * 1000)
        return {
            "id": item.id,
            "aspects": [],
            "meta": {"model": model, "prompt_version": prompt_version, "tokens_estimate": None, "latency_ms": latency_ms, "error": "upstream error"},
        }
    except Exception as e:
        logger.exception("Unhandled exception in extract-batch", extra={"props": {"req_id": req_id, "id": item.id}})
        latency_ms = int((time.time() - started) * 1000)
        return {
            "id": item.id,
            "aspects": [],
            "meta": {"model": model, "prompt_version": prompt_version, "tokens_estimate": None, "latency_ms": latency_ms, "error": str(e)},
        }


@app.post("/extract-batch")
async def extract_batch(req: BatchExtractRequest, request: Request, x_api_key: Optional[str] = Header(None)):
    # Simple header key auth
    if not x_api_key or x_api_key != ASPECT_API_KEY:
        raise HTTPException(status_code=401, detail="invalid api key")

    if not req.items or len(req.items) == 0:
        raise HTTPException(status_code=400, detail="items is required")
    if len(req.items) > 32:
        raise HTTPException(status_code=400, detail="batch size exceeds maximum of 32 items")

    req_id = getattr(request.state, "req_id", "unknown")
    logger.info(
        "extract-batch request",
        extra={
            "props": {
                "req_id": req_id,
                "num_items": len(req.items),
            }
        }
    )

    # Process all items concurrently using thread pool
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=min(32, len(req.items))) as executor:
        results = await asyncio.gather(
            *[loop.run_in_executor(executor, _process_single_item, item, req_id) for item in req.items]
        )

    logger.info(
        "extract-batch response",
        extra={"props": {"req_id": req_id, "num_items": len(results)}}
    )
    return {"items": list(results)}
