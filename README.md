# Agent Runner

Mult-agent system backed by Temporal. Runs untrusted code inside Firecracker VMs (or Docker if KVM isnt available).

## What it does

Takes a prompt, splits it into subtasks, researches, writes a draft, criticises it, and revises until its good enough. All agent steps run in isolated sandboxes with no network.

## Quick Start

```bash
pip install -r requirements.txt
python -m src.agent_runner.main     # API server
python -m src.agent_runner.worker   # Temporal worker
python -m pytest tests/test_core.py -v
```

## Sandboxes

| Backend | Startup | Good for |
|---------|---------|----------|
| Firecracker | ~500ms | Prod |
| Docker | ~100ms | When KVM not around |
| Subprocess | ~50ms | Local dev |

Firecracker spins up a fresh kernel per step — stronger than containers. Docker works as fallback. Subprocess has zero isolation, dont use in prod.

## Firecracker Setup

```bash
# needs a linux box with kvm
sudo bash scripts/build_rootfs.sh
# or via docker
docker build -f scripts/Dockerfile.fc-builder -t fc-builder .
docker run --rm -v /opt/firecracker:/output fc-builder
```

## Retries & Timeouts

- Activity timeout: 60s
- Max retries: 5 (1s, 2s, 4s, 8s, 16s backoff)
- Pool wait: 5s then fail

## Hardening

- Rate limit: 100 req/min per IP (Redis, falls back to unlimited)
- API key auth: toggle with `AUTH_ENABLED`, keys from `API_KEYS` env
- Idempotency: `Idempotency-Key` header prevents dupes

## Endpoints

| Method | Path | What |
|--------|------|------|
| GET | `/health` | Alive? |
| GET | `/ready` | Temporal + pool ready? |
| POST | `/run` | Start a workflow |
| GET | `/run/{id}/status` | Check status |
| GET | `/run/{id}/stream` | SSE events |
| GET | `/metrics` | Prometheus |
