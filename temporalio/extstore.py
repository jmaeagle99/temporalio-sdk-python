from __future__ import annotations

import dataclasses
import json
import warnings
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Self

from temporalio.api.common.v1 import Payload
from temporalio.converter import (
    JSONPlainPayloadConverter,
    SerializationContext,
    WithSerializationContext,
)
from temporalio.exceptions import TemporalError

if TYPE_CHECKING:
    from temporalio.converter import PayloadCodec


@dataclass(frozen=True)
class ExternalStorageClaim:
    """Claim for an externally stored payload.

    .. warning::
           This API is experimental.
    """

    data: dict[str, str]
    """Driver-defined data for identifying and retrieving an externally stored payload."""


class ExternalStorageDriver(ABC):
    """Base driver for storing and retrieve payloads from external storage systems.

    .. warning::
           This API is experimental.
    """

    @abstractmethod
    def name(self) -> str:
        """Returns the name of the storage driver. A storage driver may choose to
        allow this to parameterized upon creation to allow multiple instances of
        the same driver type to be used in storage options.
        """
        raise NotImplementedError

    @abstractmethod
    def type(self) -> str:
        """Returns the type of the storage driver. This string should be the same
        across all instantiations of the same driver class. This allows the equivalent
        driver implementation in different languages to be named the same.
        """
        raise NotImplementedError

    @abstractmethod
    async def store(
        self,
        context: ExternalStorageDriverContext,
        payloads: Sequence[Payload],
    ) -> list[ExternalStorageClaim | None]:
        """Stores a list of payloads in external storage and returns a storagex claim
        for each uploaded payload. Drivers shall return an ExternalStorageClaim for every
        payload.
        """
        raise NotImplementedError

    @abstractmethod
    async def retrieve(
        self,
        context: ExternalStorageDriverContext,
        claims: Sequence[ExternalStorageClaim],
    ) -> list[Payload]:
        """Retrieves a list of payloads from external storage that are associated
        with the provided list of storage claims. Drivers shall return a payload for
        every ExternalStorageClaim.
        """
        raise NotImplementedError


class ExternalStorageCompositeDriver(ExternalStorageDriver):
    """Base driver that delegates to other drivers for storing and retrieving payloads.

    .. warning::
           This API is experimental.
    """

    @abstractmethod
    def inner_drivers(self) -> list[ExternalStorageDriver]:
        """Returns the list of storage drivers to which the composite
        driver can delegate payload storage.
        """
        raise NotImplementedError


class ExternalStorageDriverContext:
    """Context for providing additional information about payload storage and retrieval.

    .. warning::
           This API is experimental.
    """

    pass


@dataclass(frozen=True)
class ExternalStorageConverter(WithSerializationContext):
    """Converters for converting and encoding external payloads to/from Python values.

    .. warning::
           This API is experimental.
    """

    payload_codec: PayloadCodec | None
    """Optional codec for encoding payload bytes."""

    def with_context(self, context: SerializationContext) -> Self:
        payload_codec = self.payload_codec
        if isinstance(payload_codec, WithSerializationContext):
            payload_codec = payload_codec.with_context(context)
        if payload_codec == self.payload_codec:
            return self
        cloned = dataclasses.replace(self)
        object.__setattr__(cloned, "payload_codec", payload_codec)
        return cloned


@dataclass(frozen=True)
class ExternalStorageOptions(WithSerializationContext):
    """Configuration for external storage behavior.

    .. warning::
           This API is experimental.
    """

    driver: ExternalStorageDriver
    """The driver to use to externally store payloads."""

    payload_size_threshold: int | None = 256 * 1024
    """The payload size at which external storage will be considered for
	a payload. Default is 256 KiB. Set to None to consider all payloads for external
    storage.
    """

    external_converter: ExternalStorageConverter | None = None
    """Converter used when storing/retrieving external payloads.
    If None, external storage will use the converter and codec specified on the
    DataConverter configued on the client. This allows differentiating how Python
    values are converted and encoded when store externally from those values that
    are stored in workflow history.
    """

    def with_context(self, context: SerializationContext) -> Self:
        driver = self.driver
        if isinstance(driver, WithSerializationContext):
            driver = driver.with_context(context)
        external_converter = self.external_converter
        if isinstance(external_converter, WithSerializationContext):
            external_converter = external_converter.with_context(context)
        if all(
            new is orig
            for new, orig in [
                (driver, self.driver),
                (external_converter, self.external_converter),
            ]
        ):
            return self
        cloned = dataclasses.replace(self)
        object.__setattr__(cloned, "driver", driver)
        object.__setattr__(cloned, "external_converter", external_converter)
        return cloned


class ExternalStorageReferenceWarning(RuntimeWarning):
    pass


class ExternalStorageDriverNotFoundError(TemporalError):
    pass


class ExternalStorageDriverError(TemporalError):
    pass


@dataclass(frozen=True)
class _ExternalStorageReference:
    driver_name: str
    claim: ExternalStorageClaim


class _ExternalStorageMiddleware:
    def __init__(self, options: ExternalStorageOptions | None):
        self._options = options
        self._claim_converter = JSONPlainPayloadConverter(
            encoding="json/external-storage-reference"
        )

    async def store_payload(self, payload: Payload) -> Payload:
        if self._options is None:
            return payload

        size_bytes = payload.ByteSize()
        if (
            self._options.payload_size_threshold is not None
            and size_bytes <= self._options.payload_size_threshold
        ):
            return payload

        context = ExternalStorageDriverContext()

        if (
            self._options.external_converter
            and self._options.external_converter.payload_codec
        ):
            payload = (
                await self._options.external_converter.payload_codec.encode([payload])
            )[0]
            size_bytes = payload.ByteSize()

        try:
            claims = await self._options.driver.store(context, [payload])
        except Exception as err:
            raise ExternalStorageDriverError from err

        if len(claims) != 1:
            raise ExternalStorageDriverError

        if claims[0] is None:
            return payload

        reference = _ExternalStorageReference(
            driver_name=self._options.driver.name(), claim=claims[0]
        )

        reference_payload = self._claim_converter.to_payload(reference)
        assert reference_payload is not None
        reference_payload.external_payloads.add().size_bytes = size_bytes
        return reference_payload

    async def store_payloads(
        self,
        payloads: Sequence[Payload],
    ) -> list[Payload]:
        results = list(payloads)
        if self._options is not None:
            # TODO: Parallelize by driver
            for index, payload in enumerate(payloads):
                results[index] = await self.store_payload(payload)
        return results

    async def retrieve_payload(
        self,
        payload: Payload,
    ) -> Payload:
        if self._options is None:
            if len(payload.external_payloads) > 0:
                warnings.warn("You have external payloads!")
            return payload

        if len(payload.external_payloads) == 0:
            return payload

        reference = self._claim_converter.from_payload(
            payload, _ExternalStorageReference
        )

        if reference.driver_name != self._options.driver.name():
            raise ExternalStorageDriverNotFoundError

        context = ExternalStorageDriverContext()

        try:
            stored_payloads = await self._options.driver.retrieve(
                context, [reference.claim]
            )
        except Exception as err:
            raise ExternalStorageDriverError from err

        if len(stored_payloads) != 1:
            raise ExternalStorageDriverError

        return stored_payloads[0]

    async def retrieve_payloads(
        self,
        payloads: Sequence[Payload],
    ) -> list[Payload]:
        results = list(payloads)
        if self._options is None:
            external_payloads = False
            for payload in payloads:
                if len(payload.external_payloads) > 0:
                    external_payloads = True
            if external_payloads:
                warnings.warn("You have external payloads!")
        else:
            # TODO: Parallelize by driver
            for index, payload in enumerate(payloads):
                results[index] = await self.retrieve_payload(payload)
        return results


class ExternalStorageDriverSelector(ABC):
    """Base class for selecting a driver to use for storing and retrieving each payload.

    .. warning::
           This API is experimental.
    """

    @abstractmethod
    def select(
        self, context: ExternalStorageDriverContext, payload: Payload
    ) -> ExternalStorageDriver | None:
        raise NotImplementedError


class ExternalStorageSelectionDriver(ExternalStorageCompositeDriver):
    """Composite driver that uses a selector to choose which inner driver to use for each payload.

    .. warning::
           This API is experimental.
    """

    def __init__(
        self,
        selector: ExternalStorageDriverSelector,
        drivers: Sequence[ExternalStorageDriver],
        name: str = "temporalio:driver:selection",
    ):
        self._selector = selector
        self._drivers = list(drivers)
        self._name = name
        self._type = type
        # Build a map from driver name to driver for efficient lookups during retrieval
        self._driver_by_name: dict[str, ExternalStorageDriver] = {
            driver.name(): driver for driver in self._drivers
        }

    def name(self) -> str:
        """Returns the name of the selection driver."""
        return self._name

    def type(self) -> str:
        """Returns the type of the selection driver."""
        return "temporalio:driver:selection"

    def inner_drivers(self) -> list[ExternalStorageDriver]:
        """Returns the list of inner drivers that can be selected."""
        return self._drivers

    async def store(
        self,
        context: ExternalStorageDriverContext,
        payloads: Sequence[Payload],
    ) -> list[ExternalStorageClaim | None]:
        """Stores payloads using the selector to choose which driver to use for each payload."""
        result: list[ExternalStorageClaim | None] = []

        for payload in payloads:
            # Use selector to pick the appropriate driver for this payload
            selected_driver = self._selector.select(context, payload)

            if selected_driver is None:
                # If no driver selected, don't store externally
                result.append(None)
            else:
                # Delegate to the selected driver
                claims = await selected_driver.store(context, [payload])
                if len(claims) != 1:
                    raise ValueError(
                        f"Expected exactly one claim from driver {selected_driver.name()}, got {len(claims)}"
                    )

                claim = claims[0]
                if claim is None:
                    result.append(None)
                else:
                    # Wrap the claim with metadata about which driver was used
                    # Encode the inner claim as JSON to avoid key collisions with other composite layers
                    wrapped_claim = ExternalStorageClaim(
                        data={
                            "driver_name": selected_driver.name(),
                            "driver_claim": json.dumps(claim.data),
                        }
                    )
                    result.append(wrapped_claim)

        return result

    async def retrieve(
        self,
        context: ExternalStorageDriverContext,
        claims: Sequence[ExternalStorageClaim],
    ) -> list[Payload]:
        """Retrieves payloads by delegating to the appropriate inner driver based on claim metadata."""
        result: list[Payload] = []

        for claim in claims:
            # Extract which driver was used from the claim metadata
            driver_name = claim.data.get("driver_name")
            if driver_name is None:
                raise ValueError("Claim missing driver_name metadata")

            driver = self._driver_by_name.get(driver_name)
            if driver is None:
                raise ValueError(f"No driver found with name: {driver_name}")

            # Decode the inner claim from JSON
            inner_claim_json = claim.data.get("driver_claim")
            if inner_claim_json is None:
                raise ValueError("Claim missing driver_claim data")

            unwrapped_claim = ExternalStorageClaim(data=json.loads(inner_claim_json))

            # Delegate to the appropriate driver
            payloads = await driver.retrieve(context, [unwrapped_claim])
            if len(payloads) != 1:
                raise ValueError(
                    f"Expected exactly one payload from driver {driver_name}, got {len(payloads)}"
                )

            result.append(payloads[0])

        return result
