# AWS Integration for Temporal Python SDK

> ⚠️ **This package is currently at an experimental release stage.** ⚠️

This package provides AWS integrations for the Temporal Python SDK, including an Amazon S3 driver for [external storage](../../../README.md#external-storage).

## Installation

    python -m pip install "temporalio[aws]"

## S3 Driver

`temporalio.contrib.aws.s3driver.S3Driver` stores and retrieves Temporal payloads in Amazon S3. It requires an [`aiobotocore`](https://github.com/aio-libs/aiobotocore) `S3Client` and either a static `bucket` name or a `bucket_selector` (but not both).

```python
import aiobotocore.session
import dataclasses
from temporalio.client import Client
from temporalio.converter import DataConverter
from temporalio.extstore import Options
from temporalio.contrib.aws.s3driver import S3Driver

session = aiobotocore.session.get_session()
async with session.create_client("s3", region_name="us-east-1") as s3_client:
    driver = S3Driver(client=s3_client, bucket="my-temporal-payloads")

    client = await Client.connect(
        "localhost:7233",
        data_converter=dataclasses.replace(
            DataConverter.default,
            external_storage=Options(drivers=[driver]),
        ),
    )
```

Payloads are stored under content-addressable keys derived from a SHA-256 hash of the serialized payload bytes, segmented by namespace and workflow/activity identifiers when serialization context is available, e.g.:

    /my-prefix/ns/my-namespace/wi/my-workflow-id/d/sha256/<hash>

Some things to note about the S3 driver:

* Any driver used to store payloads must also be configured on the component that retrieves them. If the client stores workflow inputs using this driver, the worker must include it in its `Options.drivers` list to retrieve them.
* Credentials, region, endpoint, and other AWS settings are configured on the `aiobotocore` client directly.
* Identical serialized bytes share the same S3 object (content-addressable deduplication).
* `key_prefix` prepends a path prefix to every S3 object key.
* Override `driver_name` only when registering multiple `S3Driver` instances with distinct configurations under the same `Options.drivers` list.

### Dynamic Bucket Selection

To select the S3 bucket per payload, pass a callable or a `S3StorageDriverBucketSelector` subclass as `bucket_selector`:

```python
from temporalio.contrib.aws.s3driver import S3Driver, S3StorageDriverBucketSelector
from temporalio.extstore import DriverContext
from temporalio.api.common.v1 import Payload

# Callable form
driver = S3Driver(
    client=s3_client,
    bucket_selector=lambda context, payload: (
        "large-payloads" if payload.ByteSize() > 10 * 1024 * 1024 else "small-payloads"
    ),
)

# Class form (supports serialization context via WithSerializationContext)
class MyBucketSelector(S3StorageDriverBucketSelector):
    def select(self, context: DriverContext, payload: Payload) -> str:
        return "my-bucket"

driver = S3Driver(client=s3_client, bucket_selector=MyBucketSelector())
```

Implement `temporalio.converter.WithSerializationContext` on your `S3StorageDriverBucketSelector` subclass to receive workflow or activity context (namespace, workflow ID, etc.) at serialization time.
