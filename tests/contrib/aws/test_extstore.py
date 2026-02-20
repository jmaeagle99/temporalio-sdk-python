import dataclasses
import uuid

import temporalio.converter
from temporalio.api.common.v1 import Payload
from temporalio.contrib.aws.s3driver import (
    S3Driver,
    S3StorageDriverBucketSelector,
)
from temporalio.converter import (
    ActivitySerializationContext,
    JSONPlainPayloadConverter,
    WithSerializationContext,
)
from temporalio.extstore import (
    DriverContext,
    Options,
    _StorageReference,
)
from tests.contrib.aws.conftest import S3Environment, get_keys


def assert_not_reference(payload: Payload):
    assert payload.metadata.get("encoding") != b"json/external-storage-reference"
    assert len(payload.external_payloads) == 0


def get_reference(payload: Payload) -> _StorageReference:
    assert payload.metadata.get("encoding") == b"json/external-storage-reference"
    assert len(payload.external_payloads) > 0

    claim_converter = JSONPlainPayloadConverter(
        encoding="json/external-storage-reference"
    )
    reference = claim_converter.from_payload(payload, _StorageReference)
    assert isinstance(reference, _StorageReference)
    return reference


def get_s3_key(
    reference: _StorageReference,
    bucket_name: str,
    key_prefix: str,
    namespace: str | None = None,
) -> str:
    assert "bucket" in reference.driver_claim.data
    assert "key" in reference.driver_claim.data
    assert "hashAlgo" in reference.driver_claim.data
    assert "hashDigest" in reference.driver_claim.data
    assert reference.driver_claim.data["bucket"] == bucket_name
    assert reference.driver_claim.data["hashAlgo"] == "sha256"

    # Verify the key format is content-addressable (prefix1/prefix2/hash)
    key = reference.driver_claim.data["key"]
    hash_algo = reference.driver_claim.data["hashAlgo"]
    hash_digest = reference.driver_claim.data["hashDigest"]
    namespace_segments = f"/ns/{namespace}" if namespace else ""
    expected_key = f"{key_prefix}{namespace_segments}/d/{hash_algo}/{hash_digest}"
    assert key == expected_key

    return key


async def test_extstore_s3_default(s3_env: S3Environment):
    """Test S3 external storage driver with data converter.

    Verifies that large payloads (>256KB) are stored in S3 as external references
    with content-addressable keys.
    """
    test_key_prefix = f"tests/{uuid.uuid4()}"

    driver = S3Driver(
        client=s3_env.client,
        bucket=s3_env.bucket_name,
        key_prefix=test_key_prefix,
    )

    converter = dataclasses.replace(
        temporalio.converter.default(),
        external_storage=Options(
            drivers=[driver],
        ),
    )

    large_value = "x" * (256 * 1024 + 100)

    encoded_payloads = await converter.encode([large_value])
    assert len(encoded_payloads) == 1

    reference = get_reference(encoded_payloads[0])
    assert reference.driver_name == driver.name()
    key = get_s3_key(reference, s3_env.bucket_name, test_key_prefix)

    matching_keys = await get_keys(
        s3_env.client,
        s3_env.bucket_name,
        key_prefix=test_key_prefix,
    )
    assert len(matching_keys) == 1
    assert matching_keys[0] == key

    # Decode the payload and verify we get the original value back
    decoded_values = await converter.decode(encoded_payloads, [str])
    assert len(decoded_values) == 1
    assert decoded_values[0] == large_value


async def test_extstore_s3_size_different_driver(s3_env: S3Environment):
    test_key_prefix = f"tests/{uuid.uuid4()}"

    hot_driver = S3Driver(
        client=s3_env.client,
        bucket=s3_env.bucket_name,
        key_prefix=test_key_prefix,
    )

    cold_driver = S3Driver(
        client=s3_env.client,
        bucket=s3_env.bucket_name,
        key_prefix=test_key_prefix,
    )

    converter = dataclasses.replace(
        temporalio.converter.default(),
        external_storage=Options(
            drivers=[hot_driver, cold_driver],
            driver_selector=lambda _context, payload: hot_driver
            if payload.ByteSize() < 1024 * 1024
            else cold_driver,
        ),
    )

    cold_value = "c" * (1024 * 1024 + 100)
    hot_value = "h" * (256 * 1024 + 100)
    passthrough_value = "p" * (10 * 1024)

    encoded_payloads = await converter.encode(
        [hot_value, cold_value, passthrough_value]
    )
    assert len(encoded_payloads) == 3

    hot_reference = get_reference(encoded_payloads[0])
    assert hot_reference.driver_name == hot_driver.name()
    get_s3_key(hot_reference, s3_env.bucket_name, test_key_prefix)

    cold_reference = get_reference(encoded_payloads[1])
    assert cold_reference.driver_name == cold_driver.name()
    get_s3_key(cold_reference, s3_env.bucket_name, test_key_prefix)

    assert_not_reference(encoded_payloads[2])


async def test_extstore_s3_size_different_bucket_func(s3_env: S3Environment):
    test_key_prefix = f"tests/{uuid.uuid4()}"

    cold_bucket_name = "cold-bucket"
    await s3_env.client.create_bucket(Bucket=cold_bucket_name)
    hot_bucket_name = "hot-bucket"
    await s3_env.client.create_bucket(Bucket=hot_bucket_name)

    driver = S3Driver(
        client=s3_env.client,
        bucket_selector=lambda _context, payload: hot_bucket_name
        if payload.ByteSize() < 1024 * 1024
        else cold_bucket_name,
        key_prefix=test_key_prefix,
    )

    converter = dataclasses.replace(
        temporalio.converter.default(),
        external_storage=Options(
            drivers=[driver],
        ),
    )

    cold_value = "c" * (1024 * 1024 + 100)
    passthrough_value = "p" * (10 * 1024)
    hot_value = "h" * (256 * 1024 + 100)

    encoded_payloads = await converter.encode(
        [cold_value, passthrough_value, hot_value]
    )
    assert len(encoded_payloads) == 3

    cold_reference = get_reference(encoded_payloads[0])
    assert cold_reference.driver_name == driver.name()
    get_s3_key(cold_reference, cold_bucket_name, test_key_prefix)

    assert_not_reference(encoded_payloads[1])

    hot_reference = get_reference(encoded_payloads[2])
    assert hot_reference.driver_name == driver.name()
    get_s3_key(hot_reference, hot_bucket_name, test_key_prefix)


async def test_extstore_s3_size_different_bucket_selector(s3_env: S3Environment):
    test_key_prefix = f"tests/{uuid.uuid4()}"

    class BucketSelector(S3StorageDriverBucketSelector, WithSerializationContext):
        def __init__(self, bucket_name: str):
            self._bucket_name = bucket_name

        def with_context(self, context):
            task_queue = None
            if isinstance(context, ActivitySerializationContext):
                task_queue = context.activity_task_queue
            return BucketSelector(task_queue or s3_env.bucket_name)

        def select(self, context: DriverContext, payload: Payload) -> str:
            return self._bucket_name

    driver = S3Driver(
        client=s3_env.client,
        bucket_selector=BucketSelector(s3_env.bucket_name),
        key_prefix=test_key_prefix,
    )

    converter = dataclasses.replace(
        temporalio.converter.default(),
        external_storage=Options(
            drivers=[driver],
            payload_size_threshold=1000,
        ),
    )

    value = "x" * (1000 + 100)

    encoded_payloads = await converter.encode([value])
    assert len(encoded_payloads) == 1
    reference = get_reference(encoded_payloads[0])
    get_s3_key(reference, s3_env.bucket_name, test_key_prefix)

    namespace = "my-namespace"
    task_queue_name = "customer-1-task-queue"
    await s3_env.client.create_bucket(Bucket=task_queue_name)
    converter = converter.with_context(
        ActivitySerializationContext(
            namespace=namespace,
            activity_id=None,
            activity_type=None,
            activity_task_queue=task_queue_name,
            workflow_id=None,
            workflow_type=None,
            is_local=False,
        )
    )

    encoded_payloads = await converter.encode([value])
    assert len(encoded_payloads) == 1
    reference = get_reference(encoded_payloads[0])
    get_s3_key(reference, task_queue_name, test_key_prefix, namespace)
