"""Worker that runs the document processing workflow and activities.

Connects to Temporal with S3 external storage enabled so that large payloads
passed between activities are transparently offloaded to S3 instead of being
stored inline in workflow history.

Prerequisites:
    - A running Temporal server (default: localhost:7233)
    - AWS credentials configured (env vars, ~/.aws/credentials, or IAM role)
    - An existing S3 bucket
    - pip install "temporalio[aioboto3]"

Usage:
    python worker.py
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import os

import aioboto3
from workflows import (
    DocumentProcessingWorkflow,
    LocalizationWorkflow,
    analyze_document,
    apply_localization,
    enrich_document,
    fetch_brand_guidelines,
    fetch_glossary,
    fetch_reference_corpus,
    fetch_style_guide,
    fetch_translation_memory,
    generate_document,
    generate_summary,
)

import temporalio.converter
from temporalio.client import Client
from temporalio.contrib.aws.s3driver import S3StorageDriver
from temporalio.contrib.aws.s3driver.aioboto3 import new_aioboto3_client
from temporalio.converter import ExternalStorage
from temporalio.worker import Worker

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
TASK_QUEUE = "s3-document-processing"
S3_BUCKET = os.environ.get("S3_BUCKET", "justin-s3driver-demo")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")
AWS_PROFILE = os.environ.get("AWS_PROFILE")


async def main() -> None:
    """Connect to Temporal, start a worker, and execute the workflow."""
    logging.basicConfig(level=logging.INFO)

    session = aioboto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    async with session.client("s3") as s3_client:
        driver = S3StorageDriver(
            client=new_aioboto3_client(s3_client),
            bucket=S3_BUCKET,
        )

        data_converter = dataclasses.replace(
            temporalio.converter.default(),
            external_storage=ExternalStorage(
                drivers=[driver],
            ),
        )

        # Connect to Temporal with the provided data converter.
        client = await Client.connect(
            TEMPORAL_ADDRESS,
            data_converter=data_converter,
        )

        # Start worker and execute workflow in the same process.
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[DocumentProcessingWorkflow, LocalizationWorkflow],
            activities=[
                generate_document,
                analyze_document,
                fetch_reference_corpus,
                fetch_style_guide,
                fetch_translation_memory,
                fetch_glossary,
                fetch_brand_guidelines,
                enrich_document,
                apply_localization,
                generate_summary,
            ],
        )
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
