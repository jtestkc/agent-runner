import hashlib
import re

from .models import AnalysisResult, CriticResult, QualityScore, ResearchResult, SummaryResult


def _seed(text):
    return int(hashlib.sha256(text.encode()).hexdigest(), 16)


def _corpus(query):
    seed = _seed(query)
    topics = ["market", "technical", "operational", "risk", "compliance"]
    return [f"{topics[i % 5]} signal #{(seed >> (i * 7)) % 1000} for '{query[:24]}'" for i in range(5)]


def orchestrator(input_text):
    subtasks = [s for s in re.split(r"[.;]\s*", input_text) if s.strip()]
    return {
        "agent": "orchestrator",
        "output": f"Decomposed into {len(subtasks)} sub-task(s).",
        "meta": {"subtasks": subtasks[:8], "query": input_text[:120]},
    }


def research(input_text):
    c = _corpus(input_text)
    return ResearchResult(
        findings="\n".join(f"- {x}" for x in c),
        sources=[f"sim://{i}" for i in range(len(c))],
    ).model_dump()


def analysis(input_text, data=None):
    findings = (data or {}).get("findings", "")
    lines = [l for l in findings.splitlines() if l.strip()]
    insights = [f"Insight derived from {l.split('signal')[0].strip()}" for l in lines[:3]]
    draft = (
        f"Based on {len(lines)} signals, the recommended approach is to proceed with a measured plan. "
        f"Key themes: " + "; ".join(insights[:2]) + "."
    )
    return AnalysisResult(draft=draft, insights=insights).model_dump()


def critic(draft):
    text = draft or ""
    completeness = min(1.0, len(text.split()) / 60.0)
    clarity = 0.5 + 0.5 * (1.0 if text.count(".") >= 1 else 0.0)
    accuracy = min(
        1.0, 0.4 + 0.6 * (1.0 if re.search(r"theme|approach|plan|recommend", text, re.I) else 0.4)
    )
    score = QualityScore(accuracy=accuracy, completeness=completeness, clarity=clarity)
    if score.overall >= 0.8:
        return CriticResult(
            score=score, passed=True, rationale="Draft meets the quality bar across all axes."
        ).model_dump()
    gaps = []
    if completeness < 0.7:
        gaps.append("expand with more supporting evidence")
    if clarity < 0.7:
        gaps.append("improve structure and punctuation")
    if accuracy < 0.7:
        gaps.append("state concrete recommendations and themes")
    return CriticResult(
        score=score,
        passed=False,
        revision_instructions="Revise the draft: " + "; ".join(gaps) + ".",
        rationale=f"Score {score.overall:.2f} below threshold 0.80 ({gaps}).",
    ).model_dump()


def revise(draft, instructions):
    improved = (
        draft.rstrip(". ")
        + f" Revision applied: {instructions} "
        + "Concrete evidence: (1) additional market signal shows demand, "
        "(2) operational readiness is confirmed, (3) risk is bounded. "
        "Themes: growth, efficiency, resilience. "
        "Recommendation: proceed with a phased plan that prioritises the highest-impact workstream "
        "and reviews progress weekly."
    )
    return {"agent": "analysis", "draft": improved, "insights": ["revised draft"]}


def summary(final_output, total_iterations, final_score):
    return SummaryResult(
        final_output=final_output,
        total_iterations=total_iterations,
        final_score=round(final_score, 3),
    ).model_dump()


def dispatch(agent, payload):
    if agent == "orchestrator":
        return orchestrator(payload.get("input", ""))
    if agent == "research":
        return research(payload.get("input", ""))
    if agent == "analysis":
        return analysis(payload.get("input", ""), payload.get("research"))
    if agent == "critic":
        return critic(payload.get("draft", ""))
    if agent == "revise":
        return revise(payload.get("draft", ""), payload.get("instructions", ""))
    if agent == "summary":
        return summary(
            payload.get("final_output", ""),
            payload.get("total_iterations", 0),
            payload.get("final_score", 0.0),
        )
    raise ValueError(f"unknown agent: {agent}")
