import sys
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
