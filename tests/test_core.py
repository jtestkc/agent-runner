import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Worker, UnsandboxedWorkflowRunner

from agent_runner.agents import critic, dispatch, orchestrator, research, revise, summary
from agent_runner.models import WorkflowResult
from agent_runner.vm_pool import Pool


def test_health_endpoint():
    from agent_runner.main import app
    from fastapi.testclient import TestClient

    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.text == "OK"


@pytest.mark.asyncio
async def test_workflow_start():
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
    from agent_runner.utils import Settings

    import agent_runner.vm_pool as vp

    saved = vp._SETTINGS
    vp._SETTINGS = Settings(
        sandbox_backend="subprocess",
        sandbox_min_pool=2,
        sandbox_max_pool=3,
        agent_binary=str(Path(__file__).resolve().parents[1] / "scripts" / "agent_worker.py"),
    )
    vp._pool_instance = None

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
                    WorkflowInput(input_text="test", run_id="test-1"),
                    id="test-1",
                    task_queue="test-q",
                )
                result = await handle.result()
                assert result["summary"]["final_output"]
                assert len(result["trace"]) >= 3
            finally:
                await get_pool().stop()

    vp._pool_instance = None
    vp._SETTINGS = saved


@pytest.mark.asyncio
async def test_vm_pool():
    import agent_runner.vm_pool as vp
    from agent_runner.utils import Settings

    saved = vp._SETTINGS
    vp._SETTINGS = Settings(sandbox_backend="subprocess", sandbox_min_pool=1, sandbox_max_pool=2)

    p = Pool()
    await p.start()
    assert len(p._idle) >= 1

    lease = await p.acquire(timeout=2)
    assert lease is not None
    assert len(p._idle) == 0

    await lease.release()
    assert len(p._idle) == 1

    await p.stop()
    vp._SETTINGS = saved


def test_loop_logic():
    for _ in range(3):
        r = dispatch("research", {"input": "analyze the market"})
        a = dispatch("analysis", {"input": "analyze the market", "research": r})
        c = dispatch("critic", {"draft": a["draft"]})
        if c["passed"]:
            assert c["score"]["overall"] >= 0.8
            break
        r2 = dispatch(
            "revise",
            {"draft": a["draft"], "instructions": c.get("revision_instructions", "")},
        )
        a = dispatch(
            "analysis",
            {"input": "analyze the market", "research": {"findings": r2["draft"]}},
        )
        c = dispatch("critic", {"draft": a["draft"]})
        if c["passed"]:
            break
