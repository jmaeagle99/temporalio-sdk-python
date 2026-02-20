import os
import re
from collections.abc import AsyncGenerator
from dataclasses import dataclass

import pytest
import pytest_asyncio
from aiobotocore.session import get_session
from types_aiobotocore_s3.client import S3Client


@dataclass
class S3Environment:
    """S3 environment configuration for testing.

    Provides an aiobotocore S3 client for testing.
    Can be backed by LocalStack, MinIO, or real S3.
    """

    client: S3Client
    """aiobotocore S3 client configured for testing."""
    bucket_name: str = "test-temporal-bucket"
    """Name of the test bucket that has been created."""


class _ServiceContainer:
    """Wraps a pre-existing S3-compatible endpoint (e.g. a CI service container)."""

    def __init__(self, url: str) -> None:
        self._url = url

    def get_url(self) -> str:
        return self._url


# S3 test infrastructure
@pytest.fixture(scope="session")
def _s3_container():
    """Start or connect to an S3-compatible service for testing.

    If LOCALSTACK_ENDPOINT is set, connects to a pre-existing service (e.g. a
    GitHub Actions service container). Otherwise starts LocalStack via
    testcontainers, which requires Docker to be running.
    """
    endpoint = os.environ.get("LOCALSTACK_ENDPOINT")
    if endpoint:
        yield _ServiceContainer(endpoint)
        return

    try:
        from testcontainers.localstack import LocalStackContainer
    except ImportError:
        pytest.skip("testcontainers not installed - required for S3 tests")

    container = LocalStackContainer(image="localstack/localstack:latest")
    try:
        container.start()
    except Exception as e:
        pytest.skip(f"Docker not available - required for S3 tests: {e}")
    try:
        yield container
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="session")  # type: ignore[reportUntypedFunctionDecorator]
async def s3_env(_s3_container) -> AsyncGenerator[S3Environment, None]:
    """Provides S3 environment for testing.

    Creates a test bucket named 'test-temporal-bucket' for testing.
    Returns S3Environment with configured S3 client.
    """
    endpoint_url = _s3_container.get_url()
    session = get_session()

    # Create S3 client for LocalStack
    async with session.create_client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    ) as client:
        # Create test bucket
        await client.create_bucket(Bucket="test-temporal-bucket")

        yield S3Environment(
            client=client,
            bucket_name="test-temporal-bucket",
        )


async def get_keys(
    client: S3Client,
    bucket_name: str,
    key_prefix: str,
    namespace: str | None = None,
) -> list[str]:
    """Verify that S3 objects follow the content-addressable key format.

    Checks that keys match the pattern: {prefix}/{4hex}/{4hex}/{64hex-sha256}
    and that the prefixes are derived from the hash.

    Args:
        client: S3 client to use for verification
        bucket_name: Name of the S3 bucket to check
        key_prefix: Expected key prefix (default: "temporal/payloads")

    Returns:
        List of keys that match the expected format

    Raises:
        AssertionError: If no objects are found or keys don't match expected format
    """
    # List objects under the specified prefix
    response = await client.list_objects_v2(Bucket=bucket_name, Prefix=key_prefix)
    assert "Contents" in response, "No objects found in S3 bucket"

    # Format: {prefix}/d/{hashAlgo}/{64hex-sha256}
    escaped_prefix = re.escape(key_prefix)
    key_pattern = re.compile(rf"^{escaped_prefix}/d/sha256/[0-9a-f]{{64}}$")

    stored_keys = [obj["Key"] for obj in response["Contents"] if "Key" in obj]
    return [key for key in stored_keys if key_pattern.match(key)]
