import dataclasses

import temporalio
import temporalio.converter
from temporalio.converter import PayloadLimit
from temporalio.worker._payloads import _WorkerPayloadLimits


def assert_payload_limit(limit: PayloadLimit, disabled: bool, size: int):
    assert disabled == limit.disabled
    assert size == limit.size


def test_worker_payload_limits_all_specified():
    """When all limits are specified, worker limit default application should not change any limit values."""

    # Arrange
    expected_error_limit = PayloadLimit(size=83, disabled=True)
    expected_warning_limit = PayloadLimit(size=37, disabled=False)
    expected_payload_limits = temporalio.converter.PayloadLimitsConfig(
        payload_upload_error_limit=expected_error_limit,
        payload_upload_warning_limit=expected_warning_limit,
    )
    expected_converter = dataclasses.replace(
        temporalio.converter.default(),
        payload_limits=expected_payload_limits,
    )

    worker_limits = _WorkerPayloadLimits(upload_size_error_limit=74)

    # Act
    actual_converter = worker_limits.apply_as_defaults(expected_converter)

    # Assert
    assert actual_converter.payload_limits is not None
    assert actual_converter.payload_limits.payload_upload_error_limit is not None
    assert actual_converter.payload_limits.payload_upload_warning_limit is not None
    assert_payload_limit(
        actual_converter.payload_limits.payload_upload_error_limit,
        expected_error_limit.disabled,
        expected_error_limit.size,
    )
    assert_payload_limit(
        actual_converter.payload_limits.payload_upload_warning_limit,
        expected_warning_limit.disabled,
        expected_warning_limit.size,
    )


def test_worker_payload_limits_error_unspecified():
    """When warning limit is set, but error limit is unset, the effective converter should have a error limit equal to that of the worker error limit."""

    # Arrange
    expected_warning_limit = PayloadLimit(size=37, disabled=False)
    expected_payload_limits = temporalio.converter.PayloadLimitsConfig(
        payload_upload_warning_limit=expected_warning_limit,
    )
    expected_converter = dataclasses.replace(
        temporalio.converter.default(),
        payload_limits=expected_payload_limits,
    )

    worker_error_limit = 74
    worker_limits = _WorkerPayloadLimits(upload_size_error_limit=worker_error_limit)

    # Act
    actual_converter = worker_limits.apply_as_defaults(expected_converter)

    # Assert
    assert actual_converter.payload_limits is not None
    assert actual_converter.payload_limits.payload_upload_error_limit is not None
    assert actual_converter.payload_limits.payload_upload_warning_limit is not None
    assert_payload_limit(
        actual_converter.payload_limits.payload_upload_error_limit,
        disabled=False,
        size=worker_error_limit,
    )
    assert_payload_limit(
        actual_converter.payload_limits.payload_upload_warning_limit,
        expected_warning_limit.disabled,
        expected_warning_limit.size,
    )


def test_worker_payload_limits_warning_unspecified():
    """When error limit is set, but warning limit is unset, the effective converter should have a warning limit of 25% of the error limit."""

    # Arrange
    expected_error_limit = PayloadLimit(size=37, disabled=False)
    expected_payload_limits = temporalio.converter.PayloadLimitsConfig(
        payload_upload_error_limit=expected_error_limit,
    )
    expected_converter = dataclasses.replace(
        temporalio.converter.default(),
        payload_limits=expected_payload_limits,
    )

    worker_error_limit = 74
    worker_limits = _WorkerPayloadLimits(upload_size_error_limit=worker_error_limit)

    # Act
    actual_converter = worker_limits.apply_as_defaults(expected_converter)

    # Assert
    assert actual_converter.payload_limits is not None
    assert actual_converter.payload_limits.payload_upload_error_limit is not None
    assert actual_converter.payload_limits.payload_upload_warning_limit is not None
    assert_payload_limit(
        actual_converter.payload_limits.payload_upload_error_limit,
        expected_error_limit.disabled,
        expected_error_limit.size,
    )
    assert_payload_limit(
        actual_converter.payload_limits.payload_upload_warning_limit,
        disabled=False,
        size=int(0.25 * expected_error_limit.size),
    )


def test_worker_payload_limits_all_unspecified():
    """When all limits are unset, the effective converter should have a warning limit of 25% of the error limit from the worker limits."""

    # Arrange
    expected_payload_limits = temporalio.converter.PayloadLimitsConfig()
    expected_converter = dataclasses.replace(
        temporalio.converter.default(),
        payload_limits=expected_payload_limits,
    )

    worker_error_limit = 74
    worker_limits = _WorkerPayloadLimits(upload_size_error_limit=worker_error_limit)

    # Act
    actual_converter = worker_limits.apply_as_defaults(expected_converter)

    # Assert
    assert actual_converter.payload_limits is not None
    assert actual_converter.payload_limits.payload_upload_error_limit is not None
    assert actual_converter.payload_limits.payload_upload_warning_limit is not None
    assert_payload_limit(
        actual_converter.payload_limits.payload_upload_error_limit,
        disabled=False,
        size=worker_error_limit,
    )
    assert_payload_limit(
        actual_converter.payload_limits.payload_upload_warning_limit,
        disabled=False,
        size=int(0.25 * worker_error_limit),
    )


def test_worker_payload_limits_error_disabled():
    """When error limit is disabled, the effective converter should have a warning limit equal to 25% of the worker error limit."""

    # Arrange
    expected_error_limit = PayloadLimit(size=40, disabled=True)
    expected_payload_limits = temporalio.converter.PayloadLimitsConfig(
        payload_upload_error_limit=expected_error_limit,
    )
    expected_converter = dataclasses.replace(
        temporalio.converter.default(),
        payload_limits=expected_payload_limits,
    )

    worker_error_limit = 100
    worker_limits = _WorkerPayloadLimits(upload_size_error_limit=worker_error_limit)

    # Act
    actual_converter = worker_limits.apply_as_defaults(expected_converter)

    # Assert
    assert actual_converter.payload_limits is not None
    assert actual_converter.payload_limits.payload_upload_error_limit is not None
    assert actual_converter.payload_limits.payload_upload_warning_limit is not None
    assert_payload_limit(
        actual_converter.payload_limits.payload_upload_error_limit,
        disabled=expected_error_limit.disabled,
        size=expected_error_limit.size,
    )
    assert_payload_limit(
        actual_converter.payload_limits.payload_upload_warning_limit,
        disabled=False,
        size=int(0.25 * worker_error_limit),
    )
