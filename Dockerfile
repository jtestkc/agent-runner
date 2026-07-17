FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    AGENT_RUNNER_sandbox_backend=docker

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl && \
    curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-cli.tgz | \
        tar xz -C /usr/local/bin --strip-components=1 docker/docker && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY scripts ./scripts

RUN pip install --upgrade pip && pip install -e .

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "python -m agent_runner.${AGENT_RUNNER_ROLE:-api}"]
