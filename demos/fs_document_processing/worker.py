"""Worker that runs the document processing workflow and activities.

Connects to Temporal with external storage enabled so that large payloads
passed between activities are transparently offloaded instead of being stored
inline in workflow history.

Two storage backends are supported:
  - Filesystem (default, no extra dependencies): set STORAGE_BACKEND=fs
  - Amazon S3: set STORAGE_BACKEND=s3 (requires AWS credentials and an S3 bucket)

Prerequisites:
    - A running Temporal server (default: localhost:7233)
    - For S3: AWS credentials configured and pip install "temporalio[aioboto3]"

Usage:
    # Filesystem (default)
    python worker.py

    # S3
    STORAGE_BACKEND=s3 S3_BUCKET=my-bucket python worker.py
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import os

from fsdriver import FilesystemStorageDriver
from workflows import (
    DocumentProcessingWorkflow,
    analyze_document,
    enrich_document,
    generate_document,
    generate_summary,
)

import temporalio.converter
from temporalio.client import Client
from temporalio.converter import ExternalStorage
from temporalio.worker import Worker

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
TASK_QUEUE = "fs-document-processing"

# Filesystem driver settings
FS_STORAGE_DIR = os.environ.get("FS_STORAGE_DIR", "/tmp/temporal-extstore")


async def main() -> None:
    """Connect to Temporal, start a worker, and execute the workflow."""
    logging.basicConfig(level=logging.INFO)

    driver = FilesystemStorageDriver(storage_dir=FS_STORAGE_DIR)

    data_converter = dataclasses.replace(
        temporalio.converter.default(),
        external_storage=ExternalStorage(drivers=[driver]),
    )

    client = await Client.connect(
        TEMPORAL_ADDRESS,
        data_converter=data_converter,
    )

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[DocumentProcessingWorkflow],
        activities=[
            generate_document,
            analyze_document,
            enrich_document,
            generate_summary,
        ],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
