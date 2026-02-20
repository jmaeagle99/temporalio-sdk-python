"""Tests for external storage functionality."""

from typing import Sequence

import pytest

import temporalio.api.common.v1
from temporalio.converter import DataConverter
from temporalio.extstore import (
    ExternalStorageClaim,
    ExternalStorageDriver,
    ExternalStorageDriverContext,
    ExternalStorageDriverSelector,
    ExternalStorageOptions,
    ExternalStorageSelectionDriver,
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
        context: ExternalStorageDriverContext,
        payloads: Sequence[temporalio.api.common.v1.Payload],
    ) -> list[ExternalStorageClaim | None]:
        self._store_calls += 1
        start_index = len(self._storage)

        entries = [
            (f"payload-{start_index + i}", payload.SerializeToString())
            for i, payload in enumerate(payloads)
        ]
        self._storage.update(entries)

        return [ExternalStorageClaim(data={"key": key}) for key, _ in entries]

    async def retrieve(
        self,
        context: ExternalStorageDriverContext,
        claims: Sequence[ExternalStorageClaim],
    ) -> list[temporalio.api.common.v1.Payload]:
        self._retrieve_calls += 1

        def parse_claim(
            claim: ExternalStorageClaim,
        ) -> temporalio.api.common.v1.Payload:
            key = claim.data["key"]
            if key not in self._storage:
                raise KeyError(f"Payload not found: {key}")
            payload = temporalio.api.common.v1.Payload()
            payload.ParseFromString(self._storage[key])
            return payload

        return [parse_claim(claim) for claim in claims]


class TestDataConverterExternalStorage:
    """Tests for DataConverter with external storage."""

    @pytest.mark.asyncio
    async def test_external_storage_encode_decode(self):
        """Test that large payloads are stored externally."""
        driver = MockStorageDriver()

        # Configure with 100-byte threshold
        converter = DataConverter(
            external_storage=ExternalStorageOptions(
                driver=driver,
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

    @pytest.mark.asyncio
    async def test_external_storage_reference_structure(self):
        """Test that external storage creates proper reference structure."""
        converter = DataConverter(
            external_storage=ExternalStorageOptions(
                driver=MockStorageDriver("test-driver"),
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
        # Deserialize it to verify structure
        payload_converter = converter.payload_converter
        reference = payload_converter.from_payload(
            reference_payload, _ExternalStorageReference
        )

        assert isinstance(reference, _ExternalStorageReference)
        assert reference.driver_name == "test-driver"
        assert isinstance(reference.claim, ExternalStorageClaim)
        assert "key" in reference.claim.data

    @pytest.mark.asyncio
    async def test_multiple_drivers(self):
        """Test using multiple drivers based on size."""
        hot_driver = MockStorageDriver("hot-storage")
        cold_driver = MockStorageDriver("cold-storage")

        class TieredSelector(ExternalStorageDriverSelector):
            def select(
                self,
                context: ExternalStorageDriverContext,
                payload: temporalio.api.common.v1.Payload,
            ) -> ExternalStorageDriver | None:
                size = payload.ByteSize()
                if size > 1000:
                    return cold_driver
                return hot_driver

        options = ExternalStorageOptions(
            driver=ExternalStorageSelectionDriver(
                selector=TieredSelector(),
                drivers=[hot_driver, cold_driver],
            ),
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
