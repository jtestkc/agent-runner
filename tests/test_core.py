import json
import os
import struct
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import asyncio
import pytest
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Worker, UnsandboxedWorkflowRunner

import agent_runner.vm_pool as vp
from agent_runner.agents import dispatch
from agent_runner.models import QualityScore, RunStatus, SummaryResult, WorkflowResult
from agent_runner.utils import Settings
from agent_runner.vm_pool import Pool, _subprocess, pick

_SHARED_SETTINGS = None


def _setup(backend="subprocess", min_pool=1, max_pool=2):
    global _SHARED_SETTINGS
    saved = vp._SETTINGS
    vp._pool_instance = None
    vp._SETTINGS = Settings(
        sandbox_backend=backend,
        sandbox_min_pool=min_pool,
        sandbox_max_pool=max_pool,
        agent_binary=str(Path(__file__).resolve().parents[1] / "scripts" / "agent_worker.py"),
    )
    _SHARED_SETTINGS = saved
    return saved


def _restore(saved=None):
    vp._pool_instance = None
    vp._SETTINGS = saved or _SHARED_SETTINGS


# ---------------------------------------------------------------------------
# Agent tests
# ---------------------------------------------------------------------------


def test_orchestrator_decomposes():
    r = dispatch("orchestrator", {"input": "plan A; execute B; review C"})
    assert r["agent"] == "orchestrator"
    assert r["meta"]["subtasks"] == ["plan A", "execute B", "review C"]


def test_research_is_deterministic_and_network_free():
    r1 = dispatch("research", {"input": "market trends"})
    r2 = dispatch("research", {"input": "market trends"})
    assert r1 == r2
    assert r1["findings"]
    assert "sim://" in r1["sources"][0]


def test_analysis_uses_research():
    research_data = dispatch("research", {"input": "test"})
    r = dispatch("analysis", {"input": "test", "research": research_data})
    assert r["draft"]
    assert len(r["insights"]) > 0


def test_critic_passes_good_draft():
    draft = "The recommended approach is to proceed with a comprehensive strategic plan. " \
            "Key themes include growth, efficiency, resilience, and innovation. " \
            "This strategy will drive measurable outcomes across the organization. " \
            "We recommend proceeding with a phased approach that prioritises the highest-impact " \
            "workstream and reviews progress weekly."
    r = dispatch("critic", {"draft": draft})
    assert r["passed"] is True


def test_critic_fails_short_draft():
    r = dispatch("critic", {"draft": "short"})
    assert r["passed"] is False
    assert "revision_instructions" in r


def test_revise_improves_draft():
    r = dispatch("revise", {"draft": "short draft", "instructions": "expand more"})
    assert "Revision applied" in r["draft"]
    assert len(r["draft"]) > len("short draft")


def test_dispatch_unknown_agent_errors():
    try:
        dispatch("nobody", {})
        assert False
    except ValueError:
        pass


def test_quality_score_weighted():
    s = QualityScore(accuracy=0.8, completeness=0.6, clarity=1.0)
    expected = 0.4 * 0.8 + 0.4 * 0.6 + 0.2 * 1.0
    assert s.overall == expected


# ---------------------------------------------------------------------------
# Pool tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_acquire_release():
    saved = _setup()
    p = Pool()
    await p.start()
    assert len(p._idle) >= 1
    lease = await p.acquire(timeout=2)
    assert lease is not None
    assert len(p._idle) == 0
    await lease.release()
    assert len(p._idle) == 1
    await p.stop()
    _restore(saved)


@pytest.mark.asyncio
async def test_pool_exhausted_raises():
    p = Pool()
    p._idle = []
    p._total = 0
    try:
        await p.acquire(timeout=0.2)
        assert False
    except vp.Empty:
        pass


def test_build_runner_subprocess():
    r = pick("subprocess")
    assert callable(r)


@pytest.mark.asyncio
async def test_subprocess_runner_executes_worker():
    saved = _setup()
    vp._SETTINGS = Settings(
        sandbox_backend="subprocess",
        agent_binary=str(Path(__file__).resolve().parents[1] / "scripts" / "agent_worker.py"),
    )
    out = await _subprocess("orchestrator", {"input": "hi"})
    assert out["agent"] == "orchestrator"
    _restore(saved)


# ---------------------------------------------------------------------------
# API tests
# ---------------------------------------------------------------------------


def test_health_endpoint():
    from agent_runner.main import app
    from fastapi.testclient import TestClient

    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.text == "OK"


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reflective_workflow_runs():
    from temporalio.testing import WorkflowEnvironment
    from agent_runner.worker import (
        ReflectionWorkflow,
        WorkflowInput,
        analysis,
        critic as critic_act,
        log_outcome,
        orch,
        research,
        revise,
        summary,
    )
    from agent_runner.vm_pool import get_pool

    saved = _setup(min_pool=2, max_pool=3)

    env = await WorkflowEnvironment.start_time_skipping()
    async with env:
        async with Worker(
            env.client,
            task_queue="test-q",
            workflows=[ReflectionWorkflow],
            activities=[orch, research, analysis, critic_act, revise, summary, log_outcome],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            await get_pool().start()
            try:
                handle = await env.client.start_workflow(
                    ReflectionWorkflow.run,
                    WorkflowInput(input_text="Analyse the market and recommend a plan.", run_id="test-run-1"),
                    id="test-run-1",
                    task_queue="test-q",
                )
                result = await handle.result()
                assert result["summary"]["final_output"]
                assert result["summary"]["total_iterations"] >= 1
                steps = [t["step"] for t in result["trace"]]
                assert "orchestrator" in steps
                assert "research" in steps
                assert "summary" in steps
            finally:
                await get_pool().stop()

    _restore(saved)


@pytest.mark.asyncio
async def test_activity_runs_in_sandbox():
    from agent_runner.vm_pool import get_pool
    from agent_runner.worker import orch as orch_act

    saved = _setup(min_pool=1, max_pool=2)

    await get_pool().start()
    try:
        env = ActivityEnvironment()
        out = await env.run(orch_act, "hello world")
        assert out["agent"] == "orchestrator"
    finally:
        await get_pool().stop()

    _restore(saved)


# ---------------------------------------------------------------------------
# Firecracker protocol tests
# ---------------------------------------------------------------------------

_FC_SKIP = not hasattr(asyncio, "start_unix_server")


def test_vsock_frame_struct():
    data = b'{"agent":"orchestrator","payload":{"input":"hi"}}'
    hdr = struct.pack(
        "<QQIIIIHHII",
        2, 3, 5201, 1024, len(data), 1, 5, 0, 65536, 0,
    )
    assert len(hdr) == 44
    frame = struct.pack("<I", 44 + len(data)) + hdr + data
    assert len(frame) == 4 + 44 + len(data)

    raw_len = struct.unpack("<I", frame[:4])[0]
    assert raw_len == 44 + len(data)
    assert frame[4:48] == hdr
    assert frame[48:] == data

    fields = struct.unpack("<QQIIIIHHII", hdr)
    assert fields == (2, 3, 5201, 1024, len(data), 1, 5, 0, 65536, 0)


@pytest.mark.skipif(_FC_SKIP, reason="requires Unix socket support")
@pytest.mark.asyncio
async def test_fc_api_http():
    from agent_runner.vm_pool import _fc_api

    with tempfile.TemporaryDirectory() as tmp:
        sock = os.path.join(tmp, "api.sock")

        async def handler(reader, writer):
            req = await reader.read(4096)
            assert b"PUT /boot-source" in req
            assert b"Content-Type: application/json" in req
            writer.write(
                b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n"
            )
            await writer.drain()
            writer.close()

        srv = await asyncio.start_unix_server(handler, path=sock)

        try:
            status, body = await _fc_api(
                sock, "PUT", "/boot-source",
                {"kernel_image_path": "/vmlinux"},
            )
            assert status == 204
        finally:
            srv.close()
            await srv.wait_closed()


@pytest.mark.skipif(_FC_SKIP, reason="requires Unix socket support")
@pytest.mark.asyncio
async def test_firecracker_runner_mocked():
    from agent_runner.vm_pool import _firecracker
    from unittest.mock import patch

    expected = {"agent": "orchestrator", "output": "fc-test-ok"}
    result_data = json.dumps(expected).encode()

    with tempfile.TemporaryDirectory() as tmp:
        api_path = os.path.join(tmp, "fc.sock")
        vsock_path = os.path.join(tmp, "v.sock")

        api_calls = []

        async def handle_api(reader, writer):
            req = await reader.read(8192)
            api_calls.append(req)
            writer.write(
                b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n"
            )
            await writer.drain()
            writer.close()

        async def handle_vsock(reader, writer):
            raw = await reader.readexactly(4)
            pkt_len = struct.unpack("<I", raw)[0]
            pkt_data = await reader.readexactly(pkt_len)
            envelope = json.loads(pkt_data[44:].decode())
            assert envelope["agent"] == "orchestrator"

            hdr = struct.pack(
                "<QQIIIIHHII",
                3, 2, 1024, 5201, len(result_data), 1, 5, 0, 65536, 0,
            )
            body = hdr + result_data
            writer.write(struct.pack("<I", len(body)) + body)
            await writer.drain()
            writer.close()

        api_srv = await asyncio.start_unix_server(handle_api, path=api_path)
        vsock_srv = await asyncio.start_unix_server(handle_vsock, path=vsock_path)

        class FakeProc:
            returncode = 0
            stdout = None
            stderr = None
            def kill(self):
                pass
            async def wait(self):
                return 0

        class MockTD:
            def __init__(self, *a, **kw):
                self.name = tmp
            def __enter__(self):
                return self.name
            def __exit__(self, *a):
                pass
            def cleanup(self):
                pass

        patches = [
            patch(
                "asyncio.create_subprocess_exec",
                return_value=FakeProc(),
            ),
            patch(
                "tempfile.TemporaryDirectory",
                MockTD,
            ),
        ]

        for p in patches:
            p.start()

        try:
            result = await _firecracker("orchestrator", {"input": "hi"})
            assert result == expected
            assert len(api_calls) >= 5
        finally:
            for p in patches:
                p.stop()
            api_srv.close()
            vsock_srv.close()
            await api_srv.wait_closed()
            await vsock_srv.wait_closed()
