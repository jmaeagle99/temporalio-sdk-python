"""S3 storage driver client abstraction for the S3 storage driver.

.. warning::
    This API is experimental.
"""

from __future__ import annotations

import io
from abc import ABC, abstractmethod

from botocore.exceptions import ClientError
from types_aiobotocore_s3.client import S3Client


class S3StorageDriverClient(ABC):
    """Abstract base class for S3 object operations.

    Implementations must support ``put_object`` and ``get_object``. Multipart
    upload handling (if needed) is an internal concern of each implementation.

    .. warning::
        This API is experimental.
    """

    @abstractmethod
    async def put_object(self, *, bucket: str, key: str, data: bytes) -> None:
        """Upload *data* to the given S3 *bucket* and *key*."""

    @abstractmethod
    async def object_exists(self, *, bucket: str, key: str) -> bool:
        """Return ``True`` if an object exists at the given *bucket* and *key*."""

    @abstractmethod
    async def get_object(self, *, bucket: str, key: str) -> bytes:
        """Download and return the bytes stored at the given S3 *bucket* and *key*."""


class Aioboto3StorageDriverClient(S3StorageDriverClient):
    """Adapter that wraps an aioboto3 S3 client as an :class:`S3StorageDriverClient`.

    Internally delegates to ``upload_fileobj`` for uploads (which handles
    multipart automatically for objects above the multipart threshold) and
    ``get_object`` for downloads.

    .. warning::
        This API is experimental.
    """

    def __init__(self, client: S3Client) -> None:
        """Wrap an aioboto3 S3 client.

        Args:
            client: An aioboto3 S3 client, typically obtained from
                ``aioboto3.Session().client("s3")``.
        """
        self._client = client

    async def object_exists(self, *, bucket: str, key: str) -> bool:
        """Check existence via aioboto3's ``head_object``."""
        try:
            await self._client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            # head_object returns 404 as a ClientError when the key doesn't exist.
            if e.response.get("Error", {}).get("Code") == "404":
                return False
            raise

    async def put_object(self, *, bucket: str, key: str, data: bytes) -> None:
        """Upload *data* via aioboto3's ``upload_fileobj``."""
        # upload_fileobj is an aioboto3-specific method not in the
        # types_aiobotocore_s3 stubs; it handles multipart automatically.
        await self._client.upload_fileobj(io.BytesIO(data), bucket, key)  # type: ignore[arg-type]

    async def get_object(self, *, bucket: str, key: str) -> bytes:
        """Download bytes via aioboto3's ``get_object``."""
        response = await self._client.get_object(Bucket=bucket, Key=key)
        # StreamingBody.read() is untyped in aiobotocore, returns bytes at runtime.
        return await response["Body"].read()  # type: ignore[no-any-return]


def new_aioboto3_client(client: S3Client) -> S3StorageDriverClient:
    """Create an :class:`S3StorageDriverClient` from an aioboto3 S3 client.

    Args:
        client: An aioboto3 S3 client, typically obtained from
            ``aioboto3.Session().client("s3")``.
    """
    return Aioboto3StorageDriverClient(client)
