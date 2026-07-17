# Agent Runner

Mult-agent system backed by Temporal. Runs untrusted code inside Firecracker VMs (or Docker if KVM isnt available).

Takes a prompt, splits it into subtasks, researches, writes a draft, criticises it, and revises until its good enough. All agent steps run in isolated sandboxes with no network.

```bash
pip install -r requirements.txt
python -m src.agent_runner.main
python -m src.agent_runner.worker
python -m pytest tests/test_core.py -v
```

**Firecracker setup** (needs Linux + KVM):
```bash
sudo bash scripts/build_rootfs.sh
# or
docker build -f scripts/Dockerfile.fc-builder -t fc-builder .
docker run --rm -v /opt/firecracker:/output fc-builder
```

**Retries** — activity timeout 60s, max 5 retries (1s/2s/4s/8s/16s), pool wait 5s.

**Hardening** — rate limit 100 req/min per IP (Redis), API key auth via `AUTH_ENABLED`/`API_KEYS`, idempotency via `Idempotency-Key` header.

**Endpoints** — `GET /health`, `GET /ready`, `POST /run`, `GET /run/{id}/status`, `GET /run/{id}/stream` (SSE), `GET /metrics`.
