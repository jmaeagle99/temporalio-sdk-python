from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence

from botocore.exceptions import ClientError
from types_aiobotocore_s3.client import S3Client

from temporalio.api.common.v1 import Payload
from temporalio.converter import (
    ActivitySerializationContext,
    SerializationContext,
    WithSerializationContext,
    WorkflowSerializationContext,
)
from temporalio.extstore import (
    Driver,
    DriverClaim,
    DriverContext,
    PayloadNotFoundError,
)


class S3StorageDriverBucketSelector(ABC):
    """Base class for dynamically selecting an S3 bucket per payload.

    Implement this class and pass an instance as ``bucket_selector`` to
    :class:`S3Driver` when you need stateful or class-based bucket selection
    logic. Implementing :class:`~temporalio.converter.WithSerializationContext`
    allows the selector to receive workflow/activity context at serialization
    time.

    For simple cases a plain callable ``(DriverContext, Payload) -> str`` can
    be passed as ``bucket_selector`` instead.

    .. warning::
           This API is experimental.
    """

    @abstractmethod
    def select(self, context: DriverContext, payload: Payload) -> str:
        """Returns the S3 bucket name or access point ARN to use for storing this payload."""
        raise NotImplementedError


class S3Driver(Driver, WithSerializationContext):
    """Driver for storing and retrieving Temporal payloads in Amazon S3.

    Requires an aiobotocore :class:`~types_aiobotocore_s3.client.S3Client` and
    either ``bucket`` or ``bucket_selector``. Payloads are keyed by a SHA-256
    hash of their serialized bytes, optionally prefixed and segmented by
    namespace and workflow/activity identifiers derived from the serialization
    context.

    .. warning::
           This API is experimental.
    """

    def __init__(
        self,
        client: S3Client,
        bucket: str | None = None,
        bucket_selector: (
            S3StorageDriverBucketSelector
            | Callable[[DriverContext, Payload], str]
            | None
        ) = None,
        driver_name: str | None = "temporalio:driver:s3",
        key_prefix: str | None = None,
    ):
        """Constructs the S3 driver.

        Args:
            client: aiobotocore S3 client. The client should be created and
                managed by the caller; credentials, region, endpoint, and other
                AWS settings are configured on the client directly.
            bucket: S3 bucket name or access point ARN where payloads are
                stored.
            bucket_selector: Determines the S3 bucket for each payload.
                Accepts either a :class:`S3StorageDriverBucketSelector`
                instance for stateful or class-based selection, or a plain
                callable of the form ``(DriverContext, Payload) -> str``.
                A :class:`S3StorageDriverBucketSelector` may also implement
                :class:`~temporalio.converter.WithSerializationContext` to
                receive workflow/activity context.
            driver_name: Name of this driver instance. Defaults to
                ``"temporalio:driver:s3"``. Override only when registering
                multiple S3 drivers with distinct configurations under the same
                :attr:`~temporalio.extstore.Options.drivers` list.
            key_prefix: Path prefix prepended to every S3 object key.

        Raises:
            ValueError: If neither or both of ``bucket`` and
                ``bucket_selector`` are provided.
        """
        if bucket is None and bucket_selector is None:
            raise ValueError("One of bucket or bucket_selector must be provided")
        if bucket is not None and bucket_selector is not None:
            raise ValueError("Only one of bucket or bucket_selector may be provided")
        self._client = client
        self._bucket = bucket
        self._bucket_selector = bucket_selector
        self._driver_name = driver_name or "temporalio:driver:s3"
        self._key_prefix = key_prefix
        # Values are provided from with_context when available.
        self._namespace: str | None = None
        self._workflow_id: str | None = None
        self._activity_id: str | None = None

    def name(self) -> str:
        return self._driver_name

    def type(self) -> str:
        return "temporalio:driver:s3"

    def with_context(self, context: SerializationContext) -> "S3Driver":
        bucket_selector = self._bucket_selector
        if isinstance(bucket_selector, S3StorageDriverBucketSelector) and isinstance(
            bucket_selector, WithSerializationContext
        ):
            bucket_selector = bucket_selector.with_context(context)

        workflow_id = self._workflow_id
        activity_id = self._activity_id
        namespace = self._namespace
        if isinstance(context, WorkflowSerializationContext):
            workflow_id = context.workflow_id
            namespace = context.namespace
        if isinstance(context, ActivitySerializationContext):
            # Prioritize workflow over activity so that the same payload that
            # may be stored across workflow and activity boundaries are deduplicated.
            if context.workflow_id:
                workflow_id = context.workflow_id
            elif context.activity_id:
                activity_id = context.activity_id
            namespace = context.namespace

        if all(
            new is orig
            for new, orig in [
                (bucket_selector, self._bucket_selector),
                (namespace, self._namespace),
                (workflow_id, self._workflow_id),
                (activity_id, self._activity_id),
            ]
        ):
            return self

        driver = S3Driver(
            client=self._client,
            bucket=self._bucket,
            bucket_selector=bucket_selector,
            driver_name=self._driver_name,
            key_prefix=self._key_prefix,
        )
        driver._namespace = namespace
        driver._workflow_id = workflow_id
        driver._activity_id = activity_id
        return driver

    def _get_bucket(self, context: DriverContext, payload: Payload) -> str:
        """Resolve bucket using the configured strategy."""
        if self._bucket:
            return self._bucket
        elif isinstance(self._bucket_selector, S3StorageDriverBucketSelector):
            return self._bucket_selector.select(context, payload)
        elif self._bucket_selector is not None:
            return self._bucket_selector(context, payload)
        else:
            raise ValueError("No bucket configured")

    async def store(
        self,
        context: DriverContext,
        payloads: Sequence[Payload],
    ) -> list[DriverClaim]:
        """Stores payloads in S3 and returns a :class:`~temporalio.extstore.DriverClaim` for each one.

        Payloads are keyed by their SHA-256 hash, so identical serialized bytes
        share the same S3 object. Deduplication is best-effort because the same
        Python value may serialize differently across payload converter versions
        (e.g. proto binary). The returned list is the same length as
        ``payloads``.
        """
        claims: list[DriverClaim] = []

        for payload in payloads:
            bucket = self._get_bucket(context, payload)

            payload_bytes = payload.SerializeToString()
            hash_digest = hashlib.sha256(payload_bytes).hexdigest().lower()

            prefix_segments = self._key_prefix or ""
            if prefix_segments:
                prefix_segments = prefix_segments.rstrip("/")

            namespace_segments = (
                f"/ns/{self._namespace.lower()}" if self._namespace else ""
            )

            context_segments = ""
            # Prioritize workflow over activity so that the same payload that
            # may be stored across workflow and activity boundaries are deduplicated.
            if self._workflow_id:
                context_segments += f"/wi/{self._workflow_id.lower()}"
            elif self._activity_id:
                context_segments += f"/ai/{self._activity_id.lower()}"

            digest_segments = f"/d/sha256/{hash_digest}"

            key = f"{prefix_segments}{namespace_segments}{context_segments}{digest_segments}"

            await self._client.put_object(
                Bucket=bucket,
                Key=key,
                Body=payload_bytes,
            )

            claim = DriverClaim(
                data={
                    "bucket": bucket,
                    "key": key,
                    "hashAlgo": "sha256",
                    "hashDigest": hash_digest,
                },
            )
            claims.append(claim)

        return claims

    async def retrieve(
        self,
        context: DriverContext,  # noqa: ARG002
        claims: Sequence[DriverClaim],
    ) -> list[Payload]:
        """Retrieves payloads from S3 for the given :class:`~temporalio.extstore.DriverClaim` list.

        The returned list is the same length as ``claims``.
        """
        payloads: list[Payload] = []

        for claim in claims:
            bucket = claim.data["bucket"]
            key = claim.data["key"]

            try:
                response = await self._client.get_object(Bucket=bucket, Key=key)
            except ClientError as err:
                code = err.response.get("Error", {}).get("Code", "")
                if code == "NoSuchKey":
                    raise PayloadNotFoundError(
                        f"S3 object not found: s3://{bucket}/{key}",
                        claim=claim,
                        driver_name=self.name(),
                    ) from err
                if code == "NoSuchBucket":
                    raise PayloadNotFoundError(
                        f"S3 bucket not found: {bucket}",
                        claim=claim,
                        driver_name=self.name(),
                    ) from err
                raise
            payload_bytes = await response["Body"].read()

            payload = Payload()
            payload.ParseFromString(payload_bytes)
            payloads.append(payload)

        return payloads
