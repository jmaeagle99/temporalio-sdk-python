"""Payload size limit configuration."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PayloadLimitsConfig:
    """Configuration for when payload sizes exceed limits."""

    memo_size_warning: int = 2 * 1024
    """The limit (in bytes) at which a memo size warning is logged."""

    payload_size_warning: int = 512 * 1024
    """The limit (in bytes) at which a payload size warning is logged."""
