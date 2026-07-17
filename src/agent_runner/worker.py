from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

from .agents import dispatch
from .utils import info, loops, span
from .vm_pool import get_pool

AGENT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=5,
)
AGENT_TIMEOUT = timedelta(seconds=60)
LOOP_TIMEOUT = timedelta(seconds=90)
CONTINUE_AS_NEW_AFTER = 25


@dataclass
class WorkflowInput:
    input_text: str
    run_id: str
    max_iterations: int = 3
    quality_threshold: float = 0.8


async def _run(agent, args):
    with span(f"activity.{agent}", agent=agent):
        info("agent_start", agent=agent)
        pool = get_pool()
        lease = await pool.acquire()
        try:
            result = await lease.run(agent, args)
        finally:
            await lease.release()
        info("agent_end", agent=agent)
        return result


@activity.defn(name="orch")
async def orch(input_text):
    return await _run("orchestrator", {"input": input_text})


@activity.defn(name="research")
async def research(input_text):
    return await _run("research", {"input": input_text})


@activity.defn(name="analysis")
async def analysis(input_text, data):
    return await _run("analysis", {"input": input_text, "research": data})


@activity.defn(name="critic")
async def critic(draft):
    return await _run("critic", {"draft": draft})


@activity.defn(name="revise")
async def revise(draft, instructions):
    return await _run("revise", {"draft": draft, "instructions": instructions})


@activity.defn(name="summary")
async def summary(final_output, total_iterations, final_score):
    return await _run(
        "summary",
        {
            "final_output": final_output,
            "total_iterations": total_iterations,
            "final_score": final_score,
        },
    )


@activity.defn(name="log_outcome")
async def log_outcome(outcome):
    loops.labels(outcome=outcome).inc()


@workflow.defn(name="reflection")
class ReflectionWorkflow:
    def __init__(self):
        self._n = 0
        self._trace = []

    @workflow.run
    async def run(self, raw):
        from .models import WorkflowResult

        inp = WorkflowInput(**raw) if isinstance(raw, dict) else raw
        info("wf_start", run_id=inp.run_id)
        self._trace.append({"step": "start", "agent": "orchestrator"})

        r1 = await workflow.execute_activity(
            orch, inp.input_text, start_to_close_timeout=AGENT_TIMEOUT, retry_policy=AGENT_RETRY
        )
        self._trace.append({"step": "orchestrator", "output": r1.get("output")})

        r2 = await workflow.execute_activity(
            research, inp.input_text, start_to_close_timeout=AGENT_TIMEOUT, retry_policy=AGENT_RETRY
        )
        self._trace.append({"step": "research"})

        r3 = await workflow.execute_activity(
            analysis,
            args=[inp.input_text, r2],
            start_to_close_timeout=AGENT_TIMEOUT,
            retry_policy=AGENT_RETRY,
        )
        draft = r3.get("draft", "")

        for i in range(inp.max_iterations):
            self._n += 1
            r4 = await workflow.execute_activity(
                critic, draft, start_to_close_timeout=AGENT_TIMEOUT, retry_policy=AGENT_RETRY
            )
            score = r4.get("score", {})
            overall = (
                0.4 * score.get("accuracy", 0)
                + 0.4 * score.get("completeness", 0)
                + 0.2 * score.get("clarity", 0)
            )
            passed = r4.get("passed", False)
            instructions = r4.get("revision_instructions") or "Improve the draft."
            self._trace.append(
                {"step": "critic", "iteration": i + 1, "score": round(overall, 3), "passed": passed}
            )

            await workflow.execute_local_activity(
                log_outcome, "pass" if passed else "fail", start_to_close_timeout=timedelta(seconds=5)
            )

            if passed:
                final_output = draft
                final_score = overall
                break

            r5 = await workflow.execute_activity(
                revise,
                args=[draft, instructions],
                start_to_close_timeout=LOOP_TIMEOUT,
                retry_policy=AGENT_RETRY,
            )
            r6 = await workflow.execute_activity(
                analysis,
                args=[inp.input_text, {"findings": r5.get("draft", draft)}],
                start_to_close_timeout=AGENT_TIMEOUT,
                retry_policy=AGENT_RETRY,
            )
            draft = r6.get("draft", draft)

            if self._n >= CONTINUE_AS_NEW_AFTER:
                info("continue_as_new", iterations=self._n)
                workflow.continue_as_new(inp)
        else:
            final_output = draft
            final_score = 0.0

        r7 = await workflow.execute_activity(
            summary,
            args=[final_output, self._n, final_score],
            start_to_close_timeout=AGENT_TIMEOUT,
            retry_policy=AGENT_RETRY,
        )
        self._trace.append({"step": "summary", "final_score": round(final_score, 3)})
        info("wf_complete", iterations=self._n, score=round(final_score, 3))
        return WorkflowResult(summary=r7, trace=self._trace).model_dump()


async def run_worker():
    import asyncio
    from temporalio.client import Client
    from temporalio.worker import UnsandboxedWorkflowRunner, Worker as TemporalWorker

    from .utils import get_settings
    from .vm_pool import get_pool

    s = get_settings()
    client = await Client.connect(s.temporal_host, namespace=s.temporal_namespace)
    await get_pool().start()
    worker = TemporalWorker(
        client,
        task_queue=s.temporal_task_queue,
        workflows=[ReflectionWorkflow],
        activities=[orch, research, analysis, critic, revise, summary, log_outcome],
        workflow_runner=UnsandboxedWorkflowRunner(),
    )
    info("worker_started", queue=s.temporal_task_queue, backend=s.sandbox_backend)
    await worker.run()


if __name__ == "__main__":
    import asyncio
    asyncio.run(run_worker())
