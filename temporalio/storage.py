from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, override

from temporalio.api.common.v1 import Payload
from temporalio.exceptions import TemporalError

if TYPE_CHECKING:
    from temporalio.converter import PayloadCodec, PayloadConverter


@dataclass(frozen=True)
class ExternalStorageContext:
    payload_codec: PayloadCodec | None
    payload_converter: PayloadConverter
    """Optional codec for encoding payload bytes."""


class ExternalStorageProvider:
    @abstractmethod
    async def store(self, context: ExternalStorageContext, payload: Payload) -> Payload:
        pass

    @abstractmethod
    async def retrieve(
        self, context: ExternalStorageContext, payload: Payload
    ) -> Payload:
        pass


@dataclass
class ExternalPayloadReference:
    data: dict[str, str | bytes]


class ExternalStorageDriver(ABC):
    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    async def store(self, payload: Payload) -> ExternalPayloadReference:
        pass

    @abstractmethod
    async def retrieve(self, reference: ExternalPayloadReference) -> Payload:
        pass


class PayloadStorageDriverNotFound(TemporalError):
    pass


class SingleExternalStorageProvider(ExternalStorageProvider):
    def __init__(
        self,
        driver: ExternalStorageDriver,
        encode_decode_payload: bool,
        external_size_threshold: int,
    ):
        self._driver = driver
        self._encoded_decode_payload = encode_decode_payload
        self._external_size_threshold = external_size_threshold

    @override
    async def store(self, context: ExternalStorageContext, payload: Payload) -> Payload:
        storable_payload = payload
        if self._encoded_decode_payload and context.payload_codec:
            storable_payload = (await context.payload_codec.encode([payload]))[0]

        size = storable_payload.ByteSize()

        if size <= self._external_size_threshold:
            # Intentionally return original payload since DataConverter will encrypt it again.
            # Downside is that the payload was already encoded, and the DataConverter is going to spend time
            # to reencode the same payload. Can we optimize this better?
            return payload

        claim_payload = context.payload_converter.to_payload(
            await self._driver.store(storable_payload)
        )

        claim_payload.metadata["external"] = b""
        claim_payload.metadata["external.size"] = size.to_bytes()
        claim_payload.metadata["external.drivername"] = self._driver.get_name().encode()

        external_detail = claim_payload.external_payloads.add()
        external_detail.size_bytes = size

        return claim_payload

    @override
    async def retrieve(
        self, context: ExternalStorageContext, payload: Payload
    ) -> Payload:
        if not payload.metadata.get("external"):
            # Payload was not stored in external storage but passed through
            return payload

        driver_name = payload.metadata.get("external.drivername")
        if not driver_name or driver_name.decode() != self._driver.get_name():
            raise PayloadStorageDriverNotFound()

        reference_dict = context.payload_converter.default.from_payload(payload)
        reference = ExternalPayloadReference(data=reference_dict["data"])
        stored_payload = await self._driver.retrieve(reference)

        if self._encoded_decode_payload and context.payload_codec:
            stored_payload = (await context.payload_codec.decode([stored_payload]))[0]

        return stored_payload
