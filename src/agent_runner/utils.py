import json
import os
import time
from contextlib import contextmanager
from typing import Any, Optional

from prometheus_client import REGISTRY, Counter, Gauge, Histogram


class Settings:
    def __init__(self, **kwargs):
        self.temporal_host = kwargs.get("temporal_host") or os.getenv("TEMPORAL_HOST", "localhost:7233")
        self.temporal_namespace = kwargs.get("temporal_namespace") or os.getenv("TEMPORAL_NAMESPACE", "default")
        self.temporal_task_queue = kwargs.get("temporal_task_queue") or os.getenv("TEMPORAL_TASK_QUEUE", "agent-queue")
        self.sandbox_backend = kwargs.get("sandbox_backend") or os.getenv("SANDBOX_BACKEND", "subprocess")
        self.sandbox_min_pool = kwargs.get("sandbox_min_pool") or int(os.getenv("POOL_MIN", "3"))
        self.sandbox_max_pool = kwargs.get("sandbox_max_pool") or int(os.getenv("POOL_MAX", "5"))
        self.sandbox_acquire_timeout = kwargs.get("sandbox_acquire_timeout") or float(os.getenv("ACQUIRE_TIMEOUT", "5"))
        self.sandbox_acquire_retries = kwargs.get("sandbox_acquire_retries") or int(os.getenv("ACQUIRE_RETRIES", "3"))
        self.sandbox_acquire_retry_delay = kwargs.get("sandbox_acquire_retry_delay") or float(os.getenv("ACQUIRE_RETRY_DELAY", "0.5"))
        self.agent_timeout = kwargs.get("agent_timeout") or int(os.getenv("AGENT_TIMEOUT", "60"))
        self.reflection_max_iterations = kwargs.get("reflection_max_iterations") or int(os.getenv("MAX_ITERATIONS", "3"))
        self.reflection_quality_threshold = kwargs.get("reflection_quality_threshold") or float(os.getenv("QUALITY_THRESHOLD", "0.8"))
        self.host = kwargs.get("host") or os.getenv("HOST", "0.0.0.0")
        self.port = kwargs.get("port") or int(os.getenv("PORT", "8080"))
        self.auth_enabled = kwargs.get("auth_enabled") if kwargs.get("auth_enabled") is not None else (os.getenv("AUTH_ENABLED", "false").lower() == "true")
        self.api_keys = kwargs.get("api_keys") if kwargs.get("api_keys") is not None else [k.strip() for k in os.getenv("API_KEYS", "").split(",") if k.strip()]
        self.redis_url = kwargs.get("redis_url") or os.getenv("REDIS_URL", "")
        self.rate_limit_requests = kwargs.get("rate_limit_requests") or int(os.getenv("RATE_LIMIT", "100"))
        self.rate_limit_window = kwargs.get("rate_limit_window") or int(os.getenv("RATE_LIMIT_WINDOW", "60"))
        self.agent_binary = kwargs.get("agent_binary") or os.getenv("AGENT_BINARY", "/opt/agent-runner/scripts/agent_worker.py")


_settings: Optional[Settings] = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def emit(event: str, **ctx):
    s = get_settings()
    record = {
        "event": event,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "service": "agent-runner",
    }
    record.update({k: v for k, v in ctx.items() if v is not None})
    print(json.dumps(record, default=str), flush=True)


def info(event, **ctx):
    emit(event, level="info", **ctx)


def warn(event, **ctx):
    emit(event, level="warn", **ctx)


def error(event, **ctx):
    emit(event, level="error", **ctx)


http_duration = Histogram(
    "http_request_duration_seconds", "HTTP latency", ["method", "path", "status"], registry=REGISTRY
)
pool_size = Gauge("sandbox_pool_size", "Idle VMs", registry=REGISTRY)
pool_wait = Histogram("sandbox_acquire_wait_seconds", "Pool acquire latency", registry=REGISTRY)
crashes = Counter("sandbox_crash_total", "VM crashes", ["agent"], registry=REGISTRY)
loops = Counter("reflection_loop_iterations_total", "Loop outcomes", ["outcome"], registry=REGISTRY)


@contextmanager
def span(name: str, **attrs):
    info("span_start", span=name, **attrs)
    try:
        yield
    finally:
        info("span_end", span=name, **attrs)
