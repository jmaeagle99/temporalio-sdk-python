import dataclasses
from dataclasses import dataclass

from temporalio.converter import DataConverter, PayloadLimit, PayloadLimitsConfig


@dataclass(frozen=True)
class _WorkerPayloadLimits:
    upload_size_error_limit: int | None

    def apply_as_defaults(self, data_converter: DataConverter) -> DataConverter:
        worker_upload_size_error_limit = (
            self.upload_size_error_limit or 2 * 1024 * 1024
        )  # 2 MiB fallback

        # Use data converter error limit or fallback to worker payload error limit
        payload_upload_error_limit = (
            data_converter.payload_limits.payload_upload_error_limit
            if data_converter.payload_limits
            and data_converter.payload_limits.payload_upload_error_limit
            else PayloadLimit(
                disabled=False,
                size=worker_upload_size_error_limit,
            )
        )

        # Use data converter warning limit or fallback to 25% of error limit
        payload_upload_warning_limit = (
            data_converter.payload_limits.payload_upload_warning_limit
            if data_converter.payload_limits
            and data_converter.payload_limits.payload_upload_warning_limit
            else PayloadLimit(
                disabled=False,
                size=int(
                    0.25
                    * (
                        data_converter.payload_limits.payload_upload_error_limit.size
                        if data_converter.payload_limits
                        and data_converter.payload_limits.payload_upload_error_limit
                        and not data_converter.payload_limits.payload_upload_error_limit.disabled
                        else worker_upload_size_error_limit
                    )
                ),
            )
        )

        data_converter = dataclasses.replace(
            data_converter,
            payload_limits=PayloadLimitsConfig(
                payload_upload_error_limit=payload_upload_error_limit,
                payload_upload_warning_limit=payload_upload_warning_limit,
            ),
        )

        return data_converter
