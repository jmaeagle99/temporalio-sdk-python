"""Simple local filesystem storage driver for demo purposes.

Stores payloads as files under a configurable directory so the demo can run
without any AWS credentials or S3 bucket.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Sequence
from pathlib import Path

from temporalio.api.common.v1 import Payload
from temporalio.converter import (
    StorageDriver,
    StorageDriverClaim,
    StorageDriverRetrieveContext,
    StorageDriverStoreContext,
)

_DRIVER_TYPE = "local.fsdriver"


class FilesystemStorageDriver(StorageDriver):
    """Stores Temporal payloads as files on the local filesystem.

    Payloads are written to ``<storage_dir>/<sha256-hash>.bin``.  Identical
    payloads share the same file (content-addressed, like the S3 driver).

    This driver is intended for local development and demos only — it is not
    suitable for production use (no replication, no distributed access).
    """

    def __init__(
        self,
        storage_dir: str | os.PathLike[str] = "/tmp/temporal-extstore",
        driver_name: str = _DRIVER_TYPE,
    ) -> None:
        self._root = Path(storage_dir)
        self._root.mkdir(parents=True, exist_ok=True)
        self._driver_name = driver_name

    def name(self) -> str:
        return self._driver_name

    def type(self) -> str:
        return _DRIVER_TYPE

    async def store(
        self,
        context: StorageDriverStoreContext,  # noqa: ARG002
        payloads: Sequence[Payload],
    ) -> list[StorageDriverClaim]:
        claims: list[StorageDriverClaim] = []
        for payload in payloads:
            payload_bytes = payload.SerializeToString()
            digest = hashlib.sha256(payload_bytes).hexdigest()
            path = self._root / f"{digest}.bin"
            if not path.exists():
                path.write_bytes(payload_bytes)
            claims.append(
                StorageDriverClaim(
                    claim_data={"path": str(path), "sha256": digest},
                )
            )
        return claims

    async def retrieve(
        self,
        context: StorageDriverRetrieveContext,  # noqa: ARG002
        claims: Sequence[StorageDriverClaim],
    ) -> list[Payload]:
        payloads: list[Payload] = []
        for claim in claims:
            path = claim.claim_data["path"]
            payload_bytes = Path(path).read_bytes()

            expected = claim.claim_data.get("sha256")
            if expected:
                actual = hashlib.sha256(payload_bytes).hexdigest()
                if actual != expected:
                    raise ValueError(
                        f"FilesystemStorageDriver integrity check failed [{path}]: "
                        f"expected {expected}, got {actual}"
                    )

            payload = Payload()
            payload.ParseFromString(payload_bytes)
            payloads.append(payload)
        return payloads
