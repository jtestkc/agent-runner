import asyncio
import hashlib
import hmac
import time
import uuid

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import generate_latest
from sse_starlette.sse import EventSourceResponse
from temporalio.client import Client

from . import models
from .utils import (
    REGISTRY,
    get_settings,
    http_duration,
    info,
    span,
    warn,
)


class Bus:
    def __init__(self):
        self._subs = {}
        self._lock = asyncio.Lock()

    async def sub(self, run_id):
        q = asyncio.Queue()
        async with self._lock:
            self._subs.setdefault(run_id, []).append(q)
        return q

    async def unsub(self, run_id, q):
        async with self._lock:
            xs = self._subs.get(run_id, [])
            if q in xs:
                xs.remove(q)

    async def pub(self, run_id, event):
        async with self._lock:
            xs = list(self._subs.get(run_id, []))
        for q in xs:
            await q.put(event)


bus = Bus()


class Store:
    def __init__(self):
        self._mem = {}
        self._redis = None
        self._lock = asyncio.Lock()

    async def get(self, key):
        if self._redis:
            return await self._redis.get(key)
        return self._mem.get(key)

    async def set(self, key, run_id):
        async with self._lock:
            if self._redis:
                await self._redis.set(key, run_id)
            else:
                self._mem[key] = run_id


class Manager:
    def __init__(self):
        self._client = None
        self.store = Store()

    async def _conn(self):
        if not self._client:
            s = get_settings()
            self._client = await Client.connect(s.temporal_host, namespace=s.temporal_namespace)
        return self._client

    async def start_workflow(self, workflow_id, input_text, max_iterations, quality_threshold):
        from .worker import ReflectionWorkflow, WorkflowInput

        client = await self._conn()
        s = get_settings()
        handle = await client.start_workflow(
            ReflectionWorkflow.run,
            WorkflowInput(
                input_text=input_text,
                run_id=workflow_id,
                max_iterations=max_iterations,
                quality_threshold=quality_threshold,
            ),
            id=workflow_id,
            task_queue=s.temporal_task_queue,
            run_timeout=None,
        )
        return handle.result_run_id

    async def get_handle(self, run_id):
        client = await self._conn()
        return client.get_workflow_handle(run_id)


_mgr = None


def get_manager():
    global _mgr
    if _mgr is None:
        _mgr = Manager()
    return _mgr


cfg = get_settings()

app = FastAPI(title="Agent Runner", version="0.1.0")
_cache = {}


def _get_ip(request):
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _check_auth(request):
    if not cfg.auth_enabled:
        return "anon"
    key = request.headers.get("x-api-key") or request.query_params.get("api_key")
    if not key:
        raise HTTPException(401, "missing API key")
    for k in cfg.api_keys:
        if k and hmac.compare_digest(
            hashlib.sha256(key.encode()).hexdigest(), hashlib.sha256(k.encode()).hexdigest()
        ):
            return "t:" + hashlib.sha256(k.encode()).hexdigest()[:16]
    raise HTTPException(401, "invalid API key")


async def _throttle(request, tenant):
    if not cfg.redis_url:
        return
    import redis.asyncio as aioredis

    r = aioredis.from_url(cfg.redis_url)
    key = f"rl:{tenant}:{_get_ip(request)}"
    window = f"{key}:{int(time.time()) // cfg.rate_limit_window}"
    try:
        count = await r.incr(window)
        if count == 1:
            await r.expire(window, cfg.rate_limit_window + 1)
        if count > cfg.rate_limit_requests:
            raise HTTPException(429, "rate limit exceeded")
    except HTTPException:
        raise
    except Exception as e:
        warn("redis_error", error=str(e))


@app.on_event("startup")
async def _start():
    from .vm_pool import get_pool

    await get_pool().start()
    try:
        await get_manager()._conn()
    except Exception as e:
        warn("temporal_deferred", error=str(e))


@app.on_event("shutdown")
async def _stop():
    from .vm_pool import get_pool

    await get_pool().stop()


@app.middleware("http")
async def _mw(request, call_next):
    path = request.url.path
    if path in ("/health", "/ready", "/metrics", "/docs", "/openapi.json"):
        return await call_next(request)
    try:
        tenant = _check_auth(request)
    except HTTPException as e:
        return JSONResponse(e.status_code, {"detail": e.detail})
    request.state.tenant = tenant
    await _throttle(request, tenant)
    t0 = time.time()
    try:
        resp = await call_next(request)
    except HTTPException as e:
        http_duration.labels(request.method, path, e.status_code).observe(time.time() - t0)
        raise
    except Exception:
        http_duration.labels(request.method, path, 500).observe(time.time() - t0)
        raise
    http_duration.labels(request.method, path, resp.status_code).observe(time.time() - t0)
    return resp


@app.get("/health")
async def health():
    return PlainTextResponse("OK")


@app.get("/ready")
async def ready():
    ok = True
    d = {}
    try:
        m = get_manager()
        await m._conn()
        d["temporal"] = "ok"
    except Exception as e:
        ok = False
        d["temporal"] = f"err: {e}"
    from .vm_pool import get_pool

    pool = get_pool()
    d["pool"] = len(pool._idle) if hasattr(pool, "_idle") else 0
    if d["pool"] <= 0:
        ok = False
    return JSONResponse({"ready": ok, "details": d}, 200 if ok else 503)


@app.post("/run", response_model=models.RunResponse)
async def run(req: models.RunRequest, request: Request):
    tenant = request.state.tenant
    mgr = get_manager()
    if req.idempotency_key:
        existing = await mgr.store.get(req.idempotency_key)
        if existing:
            return models.RunResponse(run_id=existing, status=models.RunStatus.QUEUED)
    run_id = models.new_run_id()
    if req.idempotency_key:
        await mgr.store.set(req.idempotency_key, run_id)
    s = get_settings()
    await mgr.start_workflow(
        workflow_id=run_id,
        input_text=req.input,
        max_iterations=s.reflection_max_iterations,
        quality_threshold=s.reflection_quality_threshold,
    )
    _cache[run_id] = models.RunStatusResponse(status=models.RunStatus.QUEUED)
    await bus.pub(run_id, {"event": "start", "status": "QUEUED"})
    info("run_accepted", run_id=run_id, tenant=tenant)
    return models.RunResponse(run_id=run_id, status=models.RunStatus.QUEUED)


@app.get("/run/{run_id}/status", response_model=models.RunStatusResponse)
async def status(run_id: str):
    mgr = get_manager()
    try:
        h = await mgr.get_handle(run_id)
        desc = await h.describe()
        state = desc.status.name
        if state == "RUNNING":
            resp = models.RunStatusResponse(status=models.RunStatus.RUNNING)
        elif state in ("COMPLETED", "CONTINUED_AS_NEW"):
            result = None
            error = None
            if state == "COMPLETED":
                try:
                    result = await h.result()
                except Exception as e:
                    error = str(e)
            resp = models.RunStatusResponse(
                status=models.RunStatus.FAILED if error else models.RunStatus.COMPLETED,
                result=result,
                error=error,
            )
        elif state == "FAILED":
            resp = models.RunStatusResponse(status=models.RunStatus.FAILED, error="workflow failed")
        else:
            resp = models.RunStatusResponse(status=models.RunStatus.QUEUED)
        _cache[run_id] = resp
        return resp
    except Exception as e:
        cached = _cache.get(run_id)
        if cached:
            return cached
        warn("status_err", run_id=run_id, error=str(e))
        return JSONResponse(
            {"status": "UNKNOWN", "result": None, "error": "not found"}, status_code=404
        )


@app.get("/run/{run_id}/stream")
async def stream(run_id: str):
    async def gen():
        q = await bus.sub(run_id)
        try:
            while True:
                try:
                    yield {"event": "step", "data": await asyncio.wait_for(q.get(), timeout=30)}
                except asyncio.TimeoutError:
                    yield {"event": "ping", "data": {}}
        finally:
            await bus.unsub(run_id, q)

    return EventSourceResponse(gen())


@app.get("/metrics")
async def metrics():
    return PlainTextResponse(generate_latest(REGISTRY).decode(), media_type="text/plain")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("agent_runner.main:app", host=cfg.host, port=cfg.port)
