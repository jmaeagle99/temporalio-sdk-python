"""Tests for external storage functionality."""

from typing import Sequence

import pytest

import temporalio.api.common.v1
from temporalio.converter import DataConverter, DefaultPayloadConverter
from temporalio.extstore import (
    ExternalStorageClaim,
    ExternalStorageDriver,
    ExternalStorageOptions,
    _ExternalStorageReference,
)


class MockStorageDriver(ExternalStorageDriver):
    """Mock storage driver for testing."""

    def __init__(self, driver_name: str = "mock-driver"):
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
        payloads: Sequence[temporalio.api.common.v1.Payload],
    ) -> list[ExternalStorageClaim]:
        self._store_calls += 1
        claims = []

        for payload in payloads:
            # Generate a unique key
            key = f"payload-{len(self._storage)}"
            # Store the serialized payload
            self._storage[key] = payload.SerializeToString()

            # Create claim
            claim = ExternalStorageClaim(data={"key": key.encode()})
            claims.append(claim)

        return claims

    async def retrieve(
        self,
        claims: Sequence[ExternalStorageClaim],
    ) -> list[temporalio.api.common.v1.Payload]:
        self._retrieve_calls += 1
        payloads = []

        for claim in claims:
            # Extract key from claim
            key = claim.data["key"].decode()
            # Retrieve stored data
            if key not in self._storage:
                raise KeyError(f"Payload not found: {key}")

            # Deserialize payload
            payload = temporalio.api.common.v1.Payload()
            payload.ParseFromString(self._storage[key])
            payloads.append(payload)

        return payloads


class TestExternalStorageDriver:
    """Tests for ExternalStorageDriver functionality."""

    @pytest.mark.asyncio
    async def test_mock_driver_store_and_retrieve(self):
        """Test that mock driver can store and retrieve payloads."""
        driver = MockStorageDriver()

        # Create test payload
        converter = DefaultPayloadConverter()
        test_value = "Hello, World!"
        payload = converter.to_payload(test_value)

        # Store payload
        claims = await driver.store([payload])
        assert len(claims) == 1
        assert driver._store_calls == 1

        # Retrieve payload
        retrieved = await driver.retrieve(claims)
        assert len(retrieved) == 1
        assert driver._retrieve_calls == 1

        # Verify payload content
        retrieved_value = converter.from_payload(retrieved[0], str)
        assert retrieved_value == test_value

    @pytest.mark.asyncio
    async def test_driver_batch_operations(self):
        """Test that driver handles batch operations correctly."""
        driver = MockStorageDriver()
        converter = DefaultPayloadConverter()

        # Create multiple test payloads
        test_values = ["value1", "value2", "value3"]
        payloads = [converter.to_payload(v) for v in test_values]

        # Store all payloads in one call
        claims = await driver.store(payloads)
        assert len(claims) == 3
        assert driver._store_calls == 1

        # Retrieve all payloads in one call
        retrieved = await driver.retrieve(claims)
        assert len(retrieved) == 3
        assert driver._retrieve_calls == 1

        # Verify all payloads
        for i, retrieved_payload in enumerate(retrieved):
            value = converter.from_payload(retrieved_payload, str)
            assert value == test_values[i]


class TestDataConverterWithExternalStorage:
    """Tests for DataConverter with external storage."""

    @pytest.mark.asyncio
    async def test_external_storage_encode_decode(self):
        """Test that large payloads are stored externally."""
        driver = MockStorageDriver()

        # Configure with 100-byte threshold
        converter = DataConverter(
            external_storage=ExternalStorageOptions(
                drivers=[driver], payload_size_threshold=100
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

    @pytest.mark.asyncio
    async def test_external_storage_reference_structure(self):
        """Test that external storage creates proper reference structure."""
        driver = MockStorageDriver("test-driver")
        converter = DataConverter(
            external_storage=ExternalStorageOptions(
                drivers=[driver], payload_size_threshold=50
            )
        )

        # Create large payload
        large_value = "x" * 100
        encoded = await converter.encode([large_value])

        # Verify reference structure
        reference_payload = encoded[0]
        assert len(reference_payload.external_payloads) > 0

        # The payload should contain a serialized _ExternalStorageReference
        # Deserialize it to verify structure
        payload_converter = converter.payload_converter
        reference = payload_converter.from_payload(
            reference_payload, _ExternalStorageReference
        )

        assert isinstance(reference, _ExternalStorageReference)
        assert reference.driver_name == "test-driver"
        assert isinstance(reference.storage_claim, ExternalStorageClaim)
        assert "key" in reference.storage_claim.data

    @pytest.mark.asyncio
    async def test_multiple_drivers(self):
        """Test using multiple drivers based on size."""
        hot_driver = MockStorageDriver("hot-storage")
        cold_driver = MockStorageDriver("cold-storage")

        # Custom selector: small → hot, large → cold
        def tiered_selector(ctx, payload):
            size = payload.ByteSize()
            if size > 1000:
                return cold_driver
            return hot_driver

        options = ExternalStorageOptions(
            drivers=[hot_driver, cold_driver],
            selector=tiered_selector,
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
