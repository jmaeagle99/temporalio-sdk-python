"""Workflow, activities, and data classes for the document processing demo.

Defines a four-step pipeline: generate a large document, analyze it, enrich it,
and produce a small summary. Each activity deliberately creates multi-megabyte
payloads to exercise Temporal's external storage feature.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import activity, workflow

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkflowInput:
    """Input for DocumentProcessingWorkflow.

    On the initial run only title and document_size are set. On a
    continue-as-new after enrichment, enriched_content, word_count, and
    key_phrases are populated so the resumed run skips straight to localization.
    """

    title: str
    document_size: int  # how many bytes the first activity should generate
    enriched_content: str | None = None
    word_count: int = 0
    key_phrases: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class AnalysisResult:
    word_count: int
    key_phrases: list[str]
    raw_analysis: str  # intentionally large (~2.5 MB)


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
async def fetch_reference_corpus(title: str) -> str:
    """Fetch a large corpus of related reference documents (~5 MB)."""
    entry = f"[reference] Related document for '{title}': " + "r" * 200 + "\n"
    target_size = 5 * 1024 * 1024
    return entry * (target_size // len(entry))


@activity.defn
async def fetch_style_guide(title: str) -> str:
    """Fetch the organization's style and formatting guide (~5 MB)."""
    rule = f"[style-rule] Formatting guideline for '{title}': " + "s" * 200 + "\n"
    target_size = 5 * 1024 * 1024
    return rule * (target_size // len(rule))


@activity.defn
async def fetch_translation_memory(title: str) -> str:
    """Fetch previously approved phrasings and translations for consistency (~5 MB)."""
    entry = f"[tm-entry] Approved phrasing for '{title}': " + "t" * 200 + "\n"
    target_size = 5 * 1024 * 1024
    return entry * (target_size // len(entry))


@activity.defn
async def fetch_glossary(title: str) -> str:
    """Fetch the domain glossary of approved terminology (~5 MB)."""
    entry = f"[glossary] Approved term for '{title}': " + "g" * 200 + "\n"
    target_size = 5 * 1024 * 1024
    return entry * (target_size // len(entry))


@activity.defn
async def fetch_brand_guidelines(title: str) -> str:
    """Fetch brand voice and messaging guidelines (~5 MB)."""
    entry = f"[brand] Brand guideline for '{title}': " + "b" * 200 + "\n"
    target_size = 5 * 1024 * 1024
    return entry * (target_size // len(entry))


@activity.defn
async def enrich_document(
    original_content: str,
    raw_analysis: str,
    reference_corpus: str,
    style_guide: str,
    translation_memory: str,
    glossary: str,
    brand_guidelines: str,
) -> str:
    """Combine seven large inputs to produce an enriched document (~10 MB).

    Seven large inputs, seven separate S3 objects uploaded when this activity is scheduled.
    """
    enriched = (
        f"=== Enriched Document ===\n\n"
        f"--- Original Content ---\n{original_content}\n\n"
        f"--- Analysis ---\n{raw_analysis}\n\n"
        f"--- Reference Corpus ---\n{reference_corpus[:256]}\n\n"
        f"--- Style Guide ---\n{style_guide[:256]}\n\n"
        f"--- Translation Memory ---\n{translation_memory[:256]}\n\n"
        f"--- Glossary ---\n{glossary[:256]}\n\n"
        f"--- Brand Guidelines ---\n{brand_guidelines[:256]}\n\n"
        f"--- Enrichment Notes ---\n"
    )
    target_size = 10 * 1024 * 1024
    note = "[enrichment] Cross-referenced section with all source inputs.\n"
    repetitions = max(0, (target_size - len(enriched)) // len(note))
    enriched += note * repetitions

    return enriched


@activity.defn
async def apply_localization(content: str) -> str:
    """Apply locale-specific rewrites and formatting to produce a localized document (~10 MB)."""
    localized = f"=== Localized Document ===\n\n{content}\n\n--- Localization Notes ---\n"
    target_size = 10 * 1024 * 1024
    note = "[l10n] Section rewritten for target locale.\n"
    repetitions = max(0, (target_size - len(localized)) // len(note))
    localized += note * repetitions
    return localized


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
# Workflows
# ---------------------------------------------------------------------------


@workflow.defn
class LocalizationWorkflow:
    """Adapt an enriched document for a target locale (large input, large output).

    Spawned as a child of DocumentProcessingWorkflow so that localization has its
    own execution history and retry scope independent of the parent pipeline.
    """

    @workflow.run
    async def run(self, enriched_content: str) -> str:
        return await workflow.execute_activity(
            apply_localization,
            enriched_content,
            start_to_close_timeout=timedelta(seconds=120),
        )


@workflow.defn
class DocumentProcessingWorkflow:
    """Process a large document through generation, analysis, enrichment, and summarization.

    The workflow input is intentionally small — the large payloads are created
    and passed between activities on the worker side, where external storage
    transparently offloads them to S3.
    """

    @workflow.run
    async def run(self, inp: WorkflowInput) -> DocumentSummary:
        if inp.enriched_content is not None:
            # Resumed after continue-as-new — skip straight to localization.

            # Step 5 — Localize (large input, large output) via child workflow.
            localized_content = await workflow.execute_child_workflow(
                LocalizationWorkflow.run,
                inp.enriched_content,
                id=f"{workflow.info().workflow_id}-localization",
                execution_timeout=timedelta(seconds=300),
            )

            # Step 6 — Summarize (large input, small output).
            return await workflow.execute_activity(
                generate_summary,
                SummaryInput(
                    title=inp.title,
                    word_count=inp.word_count,
                    key_phrases=inp.key_phrases,
                    enriched_content=localized_content,
                ),
                start_to_close_timeout=timedelta(seconds=120),
            )

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

        # Step 3 — Fetch enrichment sources concurrently (small input, large output each).
        reference_corpus, style_guide, translation_memory, glossary, brand_guidelines = (
            await asyncio.gather(
                workflow.execute_activity(
                    fetch_reference_corpus,
                    inp.title,
                    start_to_close_timeout=timedelta(seconds=120),
                ),
                workflow.execute_activity(
                    fetch_style_guide,
                    inp.title,
                    start_to_close_timeout=timedelta(seconds=120),
                ),
                workflow.execute_activity(
                    fetch_translation_memory,
                    inp.title,
                    start_to_close_timeout=timedelta(seconds=120),
                ),
                workflow.execute_activity(
                    fetch_glossary,
                    inp.title,
                    start_to_close_timeout=timedelta(seconds=120),
                ),
                workflow.execute_activity(
                    fetch_brand_guidelines,
                    inp.title,
                    start_to_close_timeout=timedelta(seconds=120),
                ),
            )
        )

        # Step 4 — Enrich (seven large inputs, large output).
        enriched_content = await workflow.execute_activity(
            enrich_document,
            args=[
                content,
                analysis.raw_analysis,
                reference_corpus,
                style_guide,
                translation_memory,
                glossary,
                brand_guidelines,
            ],
            start_to_close_timeout=timedelta(seconds=120),
        )

        # Enrichment phase complete — continue-as-new to trim history before
        # localization. The large enriched_content is carried forward as the
        # new workflow input and stored in S3 via external storage.
        workflow.continue_as_new(
            WorkflowInput(
                title=inp.title,
                document_size=inp.document_size,
                enriched_content=enriched_content,
                word_count=analysis.word_count,
                key_phrases=analysis.key_phrases,
            )
        )
