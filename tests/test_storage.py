import dataclasses
from typing import override

from temporalio.api.common.v1 import Payload
from temporalio.common import RawValue
from temporalio.converter import DataConverter
from temporalio.storage import (
    ExternalPayloadReference,
    ExternalStorageDriver,
    SingleExternalStorageProvider,
)


class MyStorageDriver(ExternalStorageDriver):
    def __init__(self):
        self._storage: dict[str, bytes] = {}
        self._next_claim_id = 1

    @override
    def get_name(self):
        return "MyStorageDriver"

    @override
    async def store(self, payload):
        claim_id = str(self._next_claim_id)
        self._next_claim_id += 1
        self._storage[claim_id] = payload.SerializeToString()
        return ExternalPayloadReference({"claim_id": claim_id})

    @override
    async def retrieve(self, reference):
        claim_id = str(reference.data["claim_id"])
        payload_bytes = self._storage[claim_id]
        return Payload.FromString(payload_bytes)


async def test_external_storage_reference_encode():
    value = "Hello, world!"
    converter = DataConverter.default
    payload = (await converter.encode([value]))[0]

    raw_value = RawValue(payload=payload)
    raw_payload = (await converter.encode([raw_value]))[0]
    print(f"Raw Payload: {raw_payload}")

    converter_with_storage = dataclasses.replace(
        DataConverter.default,
        external_storage=SingleExternalStorageProvider(
            driver=MyStorageDriver(),
            encode_decode_payload=False,
            external_size_threshold=0,
        ),
    )
    claim_payload = (await converter_with_storage.encode([value]))[0]
    print(f"Claim Payload: {claim_payload}")
    retrieved_value = (await converter_with_storage.decode([claim_payload]))[0]
    print(f"Retrieved Payload: {retrieved_value}")
