[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://python.org)

# Agent Runner

Durable multi-agent orchestration with Temporal and Firecracker microVM isolation.

## Isolation Model

| Backend | Isolation | Network | Startup | Use Case |
|---------|-----------|---------|---------|----------|
| **Firecracker** | Full KVM microVM | `--net none` | ~500ms | Production (recommended) |
| **Docker** | Container | `--network none` | ~100ms | Fallback when KVM unavailable |
| **Subprocess** | Process-level | None | ~50ms | Local dev / testing |

Firecracker is preferred because VMs provide stronger isolation than containers — each agent step gets a fresh kernel with no shared syscalls, no host filesystem access, and no network. Docker is a practical fallback on hosts without KVM support (e.g., CI runners). Subprocess has zero isolation and must never be used in production.

## Retry & Timeout Strategy

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Initial interval | 1s | Quick retry on transient infra failures |
| Backoff multiplier | 2x | Standard exponential backoff |
| Max interval | 30s | Cap to avoid excessive wait |
| Max attempts | 5 | Fail fast — a stuck agent won't recover after 5 tries |
| Per-activity timeout | 60s | Agents are stateless simulations; 60s is generous |
| Loop timeout | 90s | Revision + re-analysis combined must finish in 90s |
| Pool acquire timeout | 5s | If no VM is free in 5s, fail the activity |

## Multi-Tenant Hardening

- **Rate limiting**: Token bucket (100 req/min per IP) via Redis. Falls back to no limit if Redis is unavailable.
- **Authentication**: Mock HMAC-SHA256 API key check. Keys are stored as env vars (`API_KEYS`). Set `AUTH_ENABLED=true` to enforce.
- **Resource isolation**: Each Firecracker VM gets `cpu.max=0.5`, `memory.max=256MiB`, `--pids-limit=128`, and `--read-only` filesystem via cgroups v2.
- **Idempotency**: `Idempotency-Key` header prevents duplicate workflow starts. In-memory store by default; Redis when `REDIS_URL` is set.

## Quick Start

```bash
cp .env.example .env
pip install -r requirements.txt

# Start the API
python -m src.agent_runner.main

# Start the Temporal worker (separate terminal)
python -m src.agent_runner.worker

# Run tests
python -m pytest tests/test_core.py -v
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness check |
| GET | `/ready` | Readiness (Temporal + VM pool) |
| POST | `/run` | Start a workflow |
| GET | `/run/{id}/status` | Poll workflow status |
| GET | `/run/{id}/stream` | SSE event stream |
| GET | `/metrics` | Prometheus metrics |
