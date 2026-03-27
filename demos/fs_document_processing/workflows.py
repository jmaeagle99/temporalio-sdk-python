"""Workflow, activities, and data classes for the document processing demo.

Defines a four-step pipeline: generate a large document, analyze it, enrich it,
and produce a small summary. Each activity deliberately creates multi-megabyte
payloads to exercise Temporal's external storage feature.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkflowInput:
    """Small input from the client — no large payloads cross the client boundary."""

    title: str
    document_size: int  # how many bytes the first activity should generate


@dataclass(frozen=True)
class AnalysisResult:
    word_count: int
    key_phrases: list[str]
    raw_analysis: str  # intentionally large (~2.5 MB)


@dataclass(frozen=True)
class EnrichmentInput:
    title: str
    original_content: str
    analysis: str


@dataclass(frozen=True)
class SummaryInput:
    title: str
    word_count: int
    key_phrases: list[str]
    enriched_content: str


@dataclass(frozen=True)
class DocumentSummary:
    title: str
    word_count: int
    key_phrases: list[str]
    summary: str


# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------


@activity.defn
async def generate_document(inp: WorkflowInput) -> str:
    """Generate a large synthetic document inside the worker.

    This keeps the client payload small — the large data is created on the
    worker side and stored externally when passed between activities.
    """
    paragraph = (
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
        "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
        "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris "
        "nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in "
        "reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla "
        "pariatur. Excepteur sint occaecat cupidatat non proident, sunt in "
        "culpa qui officia deserunt mollit anim id est laborum.\n\n"
    )
    repetitions = inp.document_size // len(paragraph) + 1
    content = (paragraph * repetitions)[: inp.document_size]
    return content


@activity.defn
async def analyze_document(content: str) -> AnalysisResult:
    """Analyze the document and produce a large analysis payload."""
    words = content.split()
    word_count = len(words)

    # Extract "key phrases" (just grab some representative chunks).
    key_phrases = [
        " ".join(words[i : i + 5]) for i in range(0, min(len(words), 50), 10)
    ]

    # Produce a large analysis blob (~10 MB) to exercise external storage.
    analysis_body = (
        f"=== Analysis ===\n"
        f"Word count: {word_count}\n"
        f"Key phrases: {', '.join(key_phrases)}\n\n"
    )
    # Pad to ~10 MB with repeated analysis notes.
    target_size = 10 * 1024 * 1024
    note = (
        f"[analysis-detail] Document section verified — {word_count} words processed.\n"
    )
    repetitions = target_size // len(note)
    raw_analysis = analysis_body + note * repetitions

    return AnalysisResult(
        word_count=word_count,
        key_phrases=key_phrases,
        raw_analysis=raw_analysis,
    )


@activity.defn
async def enrich_document(inp: EnrichmentInput) -> str:
    """Combine the original document with analysis to produce an enriched version (~10 MB)."""
    enriched = (
        f"=== Enriched Document: {inp.title} ===\n\n"
        f"--- Original Content ---\n{inp.original_content}\n\n"
        f"--- Analysis ---\n{inp.analysis}\n\n"
        f"--- Enrichment Notes ---\n"
    )
    # Pad to ~10 MB.
    target_size = 10 * 1024 * 1024
    note = (
        f"[enrichment] Cross-referenced section of '{inp.title}' with analysis data.\n"
    )
    repetitions = max(0, (target_size - len(enriched)) // len(note))
    enriched += note * repetitions

    return enriched


@activity.defn
async def generate_summary(inp: SummaryInput) -> DocumentSummary:
    """Produce a small final summary (well under the 2 MB threshold)."""
    summary_text = (
        f"Document '{inp.title}' contains {inp.word_count} words. "
        f"Key themes include: {', '.join(inp.key_phrases[:3])}. "
        f"Enriched content is {len(inp.enriched_content):,} bytes."
    )
    # activity.logger.info("generate_summary: produced summary for '%s'", inp.title)
    return DocumentSummary(
        title=inp.title,
        word_count=inp.word_count,
        key_phrases=inp.key_phrases,
        summary=summary_text,
    )


# ---------------------------------------------------------------------------
# Workflow
# ---------------------------------------------------------------------------


@workflow.defn
class DocumentProcessingWorkflow:
    """Process a large document through generation, analysis, enrichment, and summarization.

    The workflow input is intentionally small — the large payloads are created
    and passed between activities on the worker side, where external storage
    transparently offloads them to S3.
    """

    @workflow.run
    async def run(self, inp: WorkflowInput) -> DocumentSummary:
        # Step 1 — Generate a large document (small input, large output).
        content = await workflow.execute_activity(
            generate_document,
            inp,
            start_to_close_timeout=timedelta(seconds=120),
        )

        # Step 2 — Analyze (large input, large output).
        analysis = await workflow.execute_activity(
            analyze_document,
            content,
            start_to_close_timeout=timedelta(seconds=120),
        )

        # Step 3 — Enrich (large input, large output).
        enriched_content = await workflow.execute_activity(
            enrich_document,
            EnrichmentInput(
                title=inp.title,
                original_content=content,
                analysis=analysis.raw_analysis,
            ),
            start_to_close_timeout=timedelta(seconds=120),
        )

        # Step 4 — Summarize (large input, small output).
        return await workflow.execute_activity(
            generate_summary,
            SummaryInput(
                title=inp.title,
                word_count=analysis.word_count,
                key_phrases=analysis.key_phrases,
                enriched_content=enriched_content,
            ),
            start_to_close_timeout=timedelta(seconds=120),
        )
