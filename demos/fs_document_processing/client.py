"""Client that starts the document processing workflow.

Connects to Temporal and submits a small workflow input. The worker (worker.py)
handles execution — large payloads are generated and passed between activities
entirely on the worker side.

Usage:
    python client.py
"""

from __future__ import annotations

import asyncio
import os
import uuid

from workflows import (
    DocumentProcessingWorkflow,
    WorkflowInput,
)

from temporalio.client import Client

TASK_QUEUE = "fs-document-processing"
TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")


async def main():
    """Connect to Temporal, start a worker, and execute the workflow."""

    # Connect to Temporal with the provided data converter.
    client = await Client.connect(
        TEMPORAL_ADDRESS,
    )

    # Small workflow input — the large document is generated inside the worker.
    workflow_input = WorkflowInput(
        title="Quarterly Business Report Q4 2025",
        document_size=10 * 1024 * 1024,
    )

    workflow_id = f"doc-processing-{uuid.uuid4().hex[:8]}"
    print(f"Starting workflow: {workflow_id}")
    print("  Running: generate → analyze → enrich → summarize …")

    result = await client.execute_workflow(
        DocumentProcessingWorkflow.run,
        workflow_input,
        id=workflow_id,
        task_queue=TASK_QUEUE,
    )

    print(f"  Title       : {result.title}")
    print(f"  Word count  : {result.word_count:,}")
    print(f"  Key phrases : {', '.join(result.key_phrases[:3])}")
    print(f"  Summary     : {result.summary}")


if __name__ == "__main__":
    asyncio.run(main())
