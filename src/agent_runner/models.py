import uuid
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field


class RunStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class RunRequest(BaseModel):
    input: str = Field(..., min_length=1, max_length=4096)
    idempotency_key: Optional[str] = Field(None, max_length=256)


class RunResponse(BaseModel):
    run_id: str
    status: RunStatus


class RunStatusResponse(BaseModel):
    status: RunStatus
    result: Optional[dict] = None
    error: Optional[str] = None


class QualityScore(BaseModel):
    accuracy: float = Field(..., ge=0.0, le=1.0)
    completeness: float = Field(..., ge=0.0, le=1.0)
    clarity: float = Field(..., ge=0.0, le=1.0)

    @property
    def overall(self):
        return 0.4 * self.accuracy + 0.4 * self.completeness + 0.2 * self.clarity


class AgentResult(BaseModel):
    agent: str
    output: str
    meta: dict = Field(default_factory=dict)


class ResearchResult(BaseModel):
    findings: str
    sources: list[str] = Field(default_factory=list)


class AnalysisResult(BaseModel):
    draft: str
    insights: list[str] = Field(default_factory=list)


class CriticResult(BaseModel):
    score: QualityScore
    passed: bool
    revision_instructions: Optional[str] = None
    rationale: str = ""


class SummaryResult(BaseModel):
    final_output: str
    total_iterations: int
    final_score: float


class WorkflowResult(BaseModel):
    summary: SummaryResult
    trace: list[dict] = Field(default_factory=list)


def new_run_id():
    return str(uuid.uuid4())
