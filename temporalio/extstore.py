from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import temporalio.api.common.v1
from temporalio.converter import (
    PayloadConverter,
    _CodecPayloadMiddleware,
    _PassthroughPayloadMiddleware,
    _PayloadMiddleware,
)

if TYPE_CHECKING:
    from temporalio.converter import PayloadCodec


@dataclass(frozen=True)
class ExternalStorageClaim:
    """Claim for externally stored payload.

    Drivers use the data field to store driver-specific information
    for identifying and retrieving payloads.

    Examples:
        S3: ``{"bucket": b"my-bucket", "key": b"abc123", "version": b"v1"}``
        GCS: ``{"bucket": b"my-bucket", "object": b"abc123", "generation": b"1"}``
        Filesystem: ``{"path": b"/tmp/payloads/abc123.bin"}``
    """

    data: dict[str, bytes]
    """Driver-defined data for identifying the stored payload."""


class ExternalStorageDriver(ABC):
    """Abstract base class for external storage drivers.

    Implementations provide batch storage and retrieval of payloads
    to/from external storage systems (S3, GCS, Azure Blob, etc.).
    """

    @abstractmethod
    def name(self) -> str:
        """Return the unique name of this driver instance.

        Example:
            ``"my-s3-bucket"`` or ``"production-gcs"``

        Returns:
            Unique driver instance name.
        """
        raise NotImplementedError

    @abstractmethod
    def type(self) -> str:
        """Return the driver type identifier.

        Example:
            ``"s3"``, ``"gcs"``, ``"azure-blob"``, ``"filesystem"``

        Returns:
            Driver type identifier.
        """
        raise NotImplementedError

    @abstractmethod
    async def store(
        self,
        payloads: Sequence[temporalio.api.common.v1.Payload],
    ) -> list[ExternalStorageClaim]:
        """Store multiple payloads in external storage.

        Args:
            payloads: Payloads to store externally.

        Returns:
            List of claims, one per payload, preserving order.
            ``claims[i]`` corresponds to ``payloads[i]``.

        Raises:
            Exception: If storage operation fails.
        """
        raise NotImplementedError

    @abstractmethod
    async def retrieve(
        self,
        claims: Sequence[ExternalStorageClaim],
    ) -> list[temporalio.api.common.v1.Payload]:
        """Retrieve multiple payloads from external storage.

        Args:
            claims: Claims for payloads to retrieve.

        Returns:
            List of payloads, one per claim, preserving order.
            ``payloads[i]`` corresponds to ``claims[i]``.

        Raises:
            Exception: If retrieval operation fails.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class _ExternalStorageReference:
    """Internal reference to externally stored payload.

    NOT PUBLIC - used internally by the framework.
    """

    driver_name: str
    """Name of the driver that stored this payload."""

    storage_claim: ExternalStorageClaim
    """Driver-specific claim for retrieving the payload."""


class ExternalStorageDriverContext:
    pass


ExternalStorageDriverSelector = Callable[
    [ExternalStorageDriverContext, temporalio.api.common.v1.Payload],
    ExternalStorageDriver | None,
]
"""Function to select which driver to use for a payload.

Args:
    context: Serialization context (workflow/activity info).
    payload: The payload being encoded.

Returns:
    Driver (ExternalStorageDriver): Use the driver to store/retrieve the payload.
    None: Don't externalize this payload (pass through).
"""


@dataclass(frozen=True)
class ExternalStorageConverter:
    """Converters for converting and encoding external payloads to/from Python values."""

    payload_codec: PayloadCodec | None = None
    """Optional codec for encoding payload bytes."""


@dataclass(frozen=True)
class ExternalStorageOptions:
    """Configuration for external storage behavior."""

    drivers: Sequence[ExternalStorageDriver]
    """List of available storage drivers."""

    selector: ExternalStorageDriverSelector | None = None
    """Function to select which driver to use for each payload.
    If returns None for a payload, that payload is passed through and will not be
    externallized with a driver.
    """

    payload_size_threshold: int = 100 * 1024
    """The payload size at which external storage will be considered for
	a payload. Default is 100 KiB. Set to 0, or negative to disable threshold
	and consider all payloads for external storage.
    """

    external_converter: ExternalStorageConverter | None = None
    """Converter used when storing/retrieving external payloads.
    If None, external storage will use the converters specified on the DataConverter
    class. This allows differentiating how Python values are converted and encoded
    when store externally from those values that are stored in workflow history.
    """


def _AddExternalStorageMiddleware(
    inner_middlware: _PayloadMiddleware,
    options: ExternalStorageOptions,
) -> _PayloadMiddleware:
    if len(options.drivers) == 0:
        return inner_middlware

    driver_map: dict[str, ExternalStorageDriver] = {}
    for driver in options.drivers:
        if driver.name() in driver_map:
            warnings.warn(
                f"Driver with name '{driver.name()}' already provided. Overwriting with latest driver.",
            )
        driver_map[driver.name()] = driver

    selector = options.selector
    if selector is None and len(driver_map) == 1:
        single_driver = next(iter(driver_map.values()))

        def _single_selector(
            ctx: ExternalStorageDriverContext,
            payload: temporalio.api.common.v1.Payload,
        ) -> ExternalStorageDriver | None:
            return single_driver

        selector = _single_selector

    if selector is None:
        return inner_middlware

    external_middleware = inner_middlware
    if options.external_converter is not None:
        external_middleware = _PassthroughPayloadMiddleware()
        if options.external_converter.payload_codec is not None:
            external_middleware = _CodecPayloadMiddleware(
                external_middleware,
                options.external_converter.payload_codec,
            )

    return _ExternalStoragePayloadMiddleware(
        inner_middlware,
        driver_map,
        selector,
        options.payload_size_threshold,
        external_middleware,
    )


class _ExternalStoragePayloadMiddleware(_PayloadMiddleware):
    """Middleware layer for external storage of large payloads."""

    def __init__(
        self,
        inner_middleware: _PayloadMiddleware,
        driver_map: dict[str, ExternalStorageDriver],
        selector: ExternalStorageDriverSelector,
        threshold: int,
        external_middleware: _PayloadMiddleware,
    ):
        self._inner_middlware = inner_middleware
        self._driver_map = driver_map
        self._selector = selector
        self._threshold = threshold
        self._external_middleware = external_middleware

    async def encode(
        self,
        payloads: Sequence[temporalio.api.common.v1.Payload],
    ) -> list[temporalio.api.common.v1.Payload]:
        # 1. Create pass through payloads using next middleware
        passthrough_payloads = await self._inner_middlware.encode(payloads)

        # 2. Collect the size of those payloads
        passthrough_payload_sizes = [p.ByteSize() for p in passthrough_payloads]

        # 3. For any payload at or below threshold, pass through
        # 4. For remaining payloads, apply selector to determine driver
        # 5. Group payloads by driver; if no driver, pass through
        driver_context = ExternalStorageDriverContext()
        payloads_by_driver: dict[
            ExternalStorageDriver,
            list[
                tuple[
                    int,
                    temporalio.api.common.v1.Payload,
                    temporalio.api.common.v1.Payload,
                    int,
                ]
            ],
        ] = {}
        passthrough_indices: set[int] = set()

        for i, (original_payload, passthrough_payload, size) in enumerate(
            zip(payloads, passthrough_payloads, passthrough_payload_sizes)
        ):
            # Check threshold first
            if self._threshold > 0 and size <= self._threshold:
                passthrough_indices.add(i)
                continue

            # Apply selector
            driver = self._selector(driver_context, original_payload)
            if driver is None:
                passthrough_indices.add(i)
            else:
                if driver not in payloads_by_driver:
                    payloads_by_driver[driver] = []
                # Store both original and passthrough payloads
                payloads_by_driver[driver].append(
                    (i, original_payload, passthrough_payload, size)
                )

        # Initialize result array
        result: list[temporalio.api.common.v1.Payload | None] = [None] * len(payloads)

        # Set passthrough payloads
        for i in passthrough_indices:
            result[i] = passthrough_payloads[i]

        # 6-10. Process each driver's payloads in parallel
        async def process_driver_payloads(
            driver: ExternalStorageDriver,
            indexed_payloads: list[
                tuple[
                    int,
                    temporalio.api.common.v1.Payload,
                    temporalio.api.common.v1.Payload,
                    int,
                ]
            ],
        ) -> None:
            # 6. Encode using external middleware
            if self._external_middleware is not self._inner_middlware:
                # Use original payloads and encode with external middleware
                original_payloads = [orig for _, orig, _, _ in indexed_payloads]
                driver_payloads = await self._external_middleware.encode(
                    original_payloads
                )
            else:
                # Use already-encoded passthrough payloads (next middleware)
                driver_payloads = [pt for _, _, pt, _ in indexed_payloads]

            # 7. Store payloads using driver
            claims = await driver.store(driver_payloads)

            # 8. Create external payload references from claims
            reference_payloads: list[temporalio.api.common.v1.Payload] = []
            for claim in claims:
                reference = _ExternalStorageReference(
                    driver_name=driver.name(),
                    storage_claim=claim,
                )
                reference_payload = PayloadConverter.default.to_payload(reference)
                reference_payloads.append(reference_payload)

            # 9. Encode all references in one pass using next middleware
            encoded_references = await self._inner_middlware.encode(reference_payloads)

            # Mark encoded references with external payload size and place in result array
            for (original_index, _, _, original_size), encoded_ref in zip(
                indexed_payloads, encoded_references
            ):
                # Mark as externally stored with original size
                external_detail = encoded_ref.external_payloads.add()
                external_detail.size_bytes = original_size
                result[original_index] = encoded_ref

        # Execute all driver operations in parallel
        if payloads_by_driver:
            import asyncio

            await asyncio.gather(
                *[
                    process_driver_payloads(driver, indexed_payloads)
                    for driver, indexed_payloads in payloads_by_driver.items()
                ]
            )

        # 10. Return combination of passthrough and claim payloads
        return [p for p in result if p is not None]

    async def decode(
        self,
        payloads: Sequence[temporalio.api.common.v1.Payload],
    ) -> list[temporalio.api.common.v1.Payload]:
        """Decode payloads, retrieving externally stored ones.

        Args:
            payloads: Payloads to process (may contain external references).

        Returns:
            List of original payloads (with external references retrieved).
        """
        # 1. Identify which payloads are external references vs passthrough
        external_indices: list[int] = []
        passthrough_indices: list[int] = []

        for i, payload in enumerate(payloads):
            # Check if externally stored using external_payloads field
            if payload.external_payloads:
                external_indices.append(i)
            else:
                passthrough_indices.append(i)

        # Initialize result array
        result: list[temporalio.api.common.v1.Payload | None] = [None] * len(payloads)

        # 2. Decode external reference payloads using next middleware
        if external_indices:
            external_payloads = [payloads[i] for i in external_indices]
            decoded_references = await self._inner_middlware.decode(external_payloads)

            # 3. Deserialize references and group by driver
            claims_by_driver: dict[str, list[tuple[int, ExternalStorageClaim]]] = {}
            for i, decoded_ref in zip(external_indices, decoded_references):
                reference = PayloadConverter.default.from_payload(
                    decoded_ref, _ExternalStorageReference
                )

                # 4. Group by driver
                if reference.driver_name not in claims_by_driver:
                    claims_by_driver[reference.driver_name] = []
                claims_by_driver[reference.driver_name].append(
                    (i, reference.storage_claim)
                )

            # 5-7. Retrieve payloads from drivers in parallel
            async def retrieve_from_driver(
                driver_name: str,
                indexed_claims: list[tuple[int, ExternalStorageClaim]],
            ) -> None:
                driver = self._driver_map.get(driver_name)
                if not driver:
                    raise ValueError(f"Driver not found: {driver_name}")

                # 5. Retrieve claims from driver
                claims = [c for _, c in indexed_claims]
                retrieved_payloads = await driver.retrieve(claims)

                # 6. Decode using external middleware if different
                if self._external_middleware is not self._inner_middlware:
                    retrieved_payloads = await self._external_middleware.decode(
                        retrieved_payloads
                    )

                # 7. Decode using next middleware
                final_payloads = await self._inner_middlware.decode(retrieved_payloads)

                # Place in result array
                for (original_index, _), final_payload in zip(
                    indexed_claims, final_payloads
                ):
                    result[original_index] = final_payload

            # Execute all driver operations in parallel
            import asyncio

            await asyncio.gather(
                *[
                    retrieve_from_driver(driver_name, indexed_claims)
                    for driver_name, indexed_claims in claims_by_driver.items()
                ]
            )

        # Process passthrough payloads
        if passthrough_indices:
            passthrough_payload_list = [payloads[i] for i in passthrough_indices]
            decoded_passthrough = await self._inner_middlware.decode(
                passthrough_payload_list
            )
            for i, decoded in zip(passthrough_indices, decoded_passthrough):
                result[i] = decoded

        # Return final result (all Nones should be replaced)
        return [p for p in result if p is not None]
