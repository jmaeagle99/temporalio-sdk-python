"""Tests for external storage functionality."""

from typing import Sequence

import pytest

from temporalio.api.common.v1 import Payload
from temporalio.converter import (
    ActivitySerializationContext,
    DataConverter,
    JSONPlainPayloadConverter,
    WithSerializationContext,
    WorkflowSerializationContext,
)
from temporalio.exceptions import ApplicationError, FailureError, TemporalError
from temporalio.extstore import (
    Driver,
    DriverClaim,
    DriverContext,
    DriverError,
    DriverSelector,
    Options,
    PayloadNotFoundError,
    _StorageReference,
)


class MockDriver(Driver):
    """Mock storage driver for testing."""

    def __init__(
        self,
        driver_name: str = "mock-driver",
    ):
        self._driver_name = driver_name
        self._storage: dict[str, bytes] = {}
        self._store_calls = 0
        self._retrieve_calls = 0

    def name(self) -> str:
        return self._driver_name

    def type(self) -> str:
        return "mock"

    async def store(
        self,
        context: DriverContext,
        payloads: Sequence[Payload],
    ) -> list[DriverClaim]:
        self._store_calls += 1
        start_index = len(self._storage)

        entries = [
            (f"payload-{start_index + i}", payload.SerializeToString())
            for i, payload in enumerate(payloads)
        ]
        self._storage.update(entries)

        return [DriverClaim(data={"key": key}) for key, _ in entries]

    async def retrieve(
        self,
        context: DriverContext,
        claims: Sequence[DriverClaim],
    ) -> list[Payload]:
        self._retrieve_calls += 1

        def parse_claim(
            claim: DriverClaim,
        ) -> Payload:
            key = claim.data["key"]
            if key not in self._storage:
                raise KeyError(f"Payload not found: {key}")
            payload = Payload()
            payload.ParseFromString(self._storage[key])
            return payload

        return [parse_claim(claim) for claim in claims]


class WorkflowIdFeatureFlagDriverSelector(DriverSelector, WithSerializationContext):
    """Example driver that conditionally stores based on workflow ID feature flag."""

    def __init__(self, driver: Driver, enabled: bool = False):
        self._driver = driver
        self._enabled = enabled

    def select_driver(self, context: DriverContext, payload: Payload) -> Driver | None:
        return self._driver if self._enabled else None

    def with_context(self, context):
        workflow_id = None
        if isinstance(context, ActivitySerializationContext) and context.workflow_id:
            workflow_id = context.workflow_id
        if isinstance(context, WorkflowSerializationContext) and context.workflow_id:
            workflow_id = context.workflow_id

        # Create new instance with updated enabled flag and propagate context to inner driver
        driver = self._driver
        if isinstance(driver, WithSerializationContext):
            driver = driver.with_context(context)

        return WorkflowIdFeatureFlagDriverSelector(
            driver, WorkflowIdFeatureFlagDriverSelector.feature_flag_is_on(workflow_id)
        )

    @staticmethod
    def feature_flag_is_on(workflow_id: str | None) -> bool:
        """Mock implementation of a feature flag based on a workflow ID."""
        return workflow_id is not None and len(workflow_id) % 2 == 0


class TestDataConverterExternalStorage:
    """Tests for DataConverter with external storage."""

    async def test_extstore_encode_decode(self):
        """Test that large payloads are stored externally."""
        driver = MockDriver()

        # Configure with 100-byte threshold
        converter = DataConverter(
            external_storage=Options(
                drivers=[driver],
                payload_size_threshold=100,
            )
        )

        # Small value should not be externalized
        small_value = "small"
        encoded_small = await converter.encode([small_value])
        assert len(encoded_small) == 1
        assert not encoded_small[0].external_payloads  # Not externalized
        assert driver._store_calls == 0

        # Large value should be externalized
        large_value = "x" * 200
        encoded_large = await converter.encode([large_value])
        assert len(encoded_large) == 1
        assert len(encoded_large[0].external_payloads) > 0  # Externalized
        assert driver._store_calls == 1

        # Decode large value
        decoded = await converter.decode(encoded_large, [str])
        assert len(decoded) == 1
        assert decoded[0] == large_value
        assert driver._retrieve_calls == 1

    async def test_extstore_reference_structure(self):
        """Test that external storage creates proper reference structure."""
        converter = DataConverter(
            external_storage=Options(
                drivers=[MockDriver("test-driver")],
                payload_size_threshold=50,
            )
        )

        # Create large payload
        large_value = "x" * 100
        encoded = await converter.encode([large_value])

        # Verify reference structure
        reference_payload = encoded[0]
        assert len(reference_payload.external_payloads) > 0

        # The payload should contain a serialized _ExternalStorageReference
        # Deserialize it to verify structure using the same encoding
        claim_converter = JSONPlainPayloadConverter(
            encoding="json/external-storage-reference"
        )
        reference = claim_converter.from_payload(reference_payload, _StorageReference)

        assert isinstance(reference, _StorageReference)
        assert "test-driver" == reference.driver_name
        assert isinstance(reference.driver_claim, DriverClaim)
        assert "key" in reference.driver_claim.data

    async def test_extstore_composite_conditional(self):
        """Test using multiple drivers based on size."""
        hot_driver = MockDriver("hot-storage")
        cold_driver = MockDriver("cold-storage")

        options = Options(
            drivers=[hot_driver, cold_driver],
            driver_selector=lambda context, payload: hot_driver
            if payload.ByteSize() < 500
            else cold_driver,
            payload_size_threshold=100,
        )
        converter = DataConverter(external_storage=options)

        # Small payload (not externalized)
        small = "x" * 50
        encoded_small = await converter.encode([small])
        assert not encoded_small[0].external_payloads
        assert hot_driver._store_calls == 0
        assert cold_driver._store_calls == 0

        # Medium payload (hot storage)
        medium = "x" * 200
        encoded_medium = await converter.encode([medium])
        assert len(encoded_medium[0].external_payloads) > 0
        assert hot_driver._store_calls == 1
        assert cold_driver._store_calls == 0

        # Large payload (cold storage)
        large = "x" * 2000
        encoded_large = await converter.encode([large])
        assert len(encoded_large[0].external_payloads) > 0
        assert hot_driver._store_calls == 1  # Unchanged
        assert cold_driver._store_calls == 1

        # Verify retrieval from correct drivers
        decoded_medium = await converter.decode(encoded_medium, [str])
        assert decoded_medium[0] == medium
        assert hot_driver._retrieve_calls == 1

        decoded_large = await converter.decode(encoded_large, [str])
        assert decoded_large[0] == large
        assert cold_driver._retrieve_calls == 1

    async def test_extstore_serialization_context(self):
        driver = MockDriver()

        # The payload should not be stored externally when it doesn't have a serialization context
        # or if the workflow ID doesn't end with "-extstore". This is an example of feature flagging
        # external storage using the workflow ID. This is an advanced secnario and requires the "can_store"
        # filter to be a WithSerializationContext.
        options = Options(
            drivers=[driver],
            driver_selector=WorkflowIdFeatureFlagDriverSelector(driver),
            payload_size_threshold=1024,
        )
        converter = DataConverter(external_storage=options)

        large_value = "c" * (1024 + 100)

        encoded_payloads = await converter.encode([large_value])
        assert len(encoded_payloads) == 1
        assert driver._store_calls == 0
        assert driver._retrieve_calls == 0

        decoded_values = await converter.decode(encoded_payloads, [str])
        assert len(decoded_values) == 1
        assert driver._store_calls == 0
        assert driver._store_calls == 0

        namespace = "my-ns"

        # Now has serialization context, but workflow ID does not end with "-extstore"
        converter = converter.with_context(
            WorkflowSerializationContext(
                namespace=namespace,
                workflow_id="odd-length-workflow-id1",
            )
        )

        encoded_payloads = await converter.encode([large_value])
        assert len(encoded_payloads) == 1
        assert driver._store_calls == 0
        assert driver._retrieve_calls == 0

        decoded_values = await converter.decode(encoded_payloads, [str])
        assert len(decoded_values) == 1
        assert driver._store_calls == 0
        assert driver._store_calls == 0

        # Now has serialization context with workflow ID ending with "-extstore"
        converter = converter.with_context(
            WorkflowSerializationContext(
                namespace=namespace,
                workflow_id="even-length-workflow-id1",
            )
        )

        encoded_payloads = await converter.encode([large_value])
        assert len(encoded_payloads) == 1
        assert driver._store_calls == 1
        assert driver._retrieve_calls == 0

        decoded_values = await converter.decode(encoded_payloads, [str])
        assert len(decoded_values) == 1
        assert driver._store_calls == 1
        assert driver._store_calls == 1


class NotFoundDriver(Driver):
    """Driver that stores normally but raises PayloadNotFoundError on retrieve."""

    def __init__(self, driver_name: str = "not-found-driver"):
        self._driver_name = driver_name
        self._storage: dict[str, bytes] = {}

    def name(self) -> str:
        return self._driver_name

    def type(self) -> str:
        return "not-found"

    async def store(
        self,
        context: DriverContext,
        payloads: Sequence[Payload],
    ) -> list[DriverClaim]:
        entries = [
            (f"payload-{i}", payload.SerializeToString())
            for i, payload in enumerate(payloads)
        ]
        self._storage.update(entries)
        return [DriverClaim(data={"key": key}) for key, _ in entries]

    async def retrieve(
        self,
        context: DriverContext,
        claims: Sequence[DriverClaim],
    ) -> list[Payload]:
        raise PayloadNotFoundError(
            "Payload not found in not-found-driver",
            claim=claims[0] if claims else None,
            driver_name=self.name(),
        )


class TestPayloadNotFoundError:
    """Tests for PayloadNotFoundError class and middleware behaviour."""

    def test_class_hierarchy(self):
        """PayloadNotFoundError must be TemporalError but not ApplicationError or FailureError."""
        assert issubclass(PayloadNotFoundError, TemporalError)
        assert not issubclass(PayloadNotFoundError, ApplicationError)
        assert not issubclass(PayloadNotFoundError, FailureError)
        assert not issubclass(PayloadNotFoundError, DriverError)

    def test_default_message(self):
        err = PayloadNotFoundError()
        assert str(err) == "External payload not found"
        assert err.claim is None
        assert err.driver_name is None

    def test_properties(self):
        claim = DriverClaim(data={"key": "my-key"})
        err = PayloadNotFoundError("gone", claim=claim, driver_name="my-driver")
        assert str(err) == "gone"
        assert err.claim is claim
        assert err.driver_name == "my-driver"

    async def test_middleware_propagates_not_found(self):
        """PayloadNotFoundError from a driver must not be wrapped in DriverError."""
        converter = DataConverter(
            external_storage=Options(
                drivers=[NotFoundDriver()],
                payload_size_threshold=1,  # store everything
            )
        )

        # Store a payload so we have a reference to retrieve
        encoded = await converter.encode(["hello world " * 20])
        assert len(encoded[0].external_payloads) > 0

        # Retrieving should raise PayloadNotFoundError, not DriverError
        with pytest.raises(PayloadNotFoundError):
            await converter.decode(encoded, [str])


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
