"""Centralized logging configuration for PyCoreMark components.

TODO(Member 1): Enhance logging formatting with distributed trace identifiers.
"""

import logging
import sys


def setup_logging(level: str = "INFO") -> None:
    """Configure root logger with consistent formatting across the cluster.

    Args:
        level: String log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    log_format = (
        "[%(asctime)s] [%(levelname)s] [%(name)s] "
        "[pid:%(process)d]: %(message)s"
    )
    logging.basicConfig(
        level=numeric_level,
        format=log_format,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    """Retrieve a configured logger instance with the specified component name.

    Args:
        name: Name of the component or module requesting the logger.

    Returns:
        Configured logging.Logger instance.
    """
    return logging.getLogger(f"pycoremark.{name}")

