import json
import logging
import sys

from datetime import (
    datetime,
    timezone,
)


class JsonFormatter(
    logging.Formatter
):

    EXTRA_FIELDS = (
        "event",
        "pipeline",
        "metric_name",
        "metric_value",
        "duration_seconds",
        "source_age_seconds",
        "input_path",
        "output_path",
    )

    def format(
        self,
        record: logging.LogRecord,
    ) -> str:

        payload = {
            "timestamp": (
                datetime.now(
                    timezone.utc
                ).isoformat()
            ),
            "level": record.levelname,
            "logger": record.name,
            "message": (
                record.getMessage()
            ),
        }

        for field in self.EXTRA_FIELDS:

            if hasattr(
                record,
                field,
            ):
                payload[field] = (
                    getattr(
                        record,
                        field,
                    )
                )

        if record.exc_info:

            payload["exception"] = (
                self.formatException(
                    record.exc_info
                )
            )

        return json.dumps(
            payload,
            default=str,
        )


def configure_logging(
    level: int = logging.INFO,
) -> None:

    handler = (
        logging.StreamHandler(
            sys.stdout
        )
    )

    handler.setFormatter(
        JsonFormatter()
    )

    root_logger = (
        logging.getLogger()
    )

    root_logger.handlers.clear()

    root_logger.addHandler(
        handler
    )

    root_logger.setLevel(
        level
    )


def get_logger(
    name: str,
) -> logging.Logger:

    return logging.getLogger(
        name
    )