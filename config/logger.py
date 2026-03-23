"""
config/logger.py
────────────────
Centralised rotating-file + console logger.
Used by every script in the project.
"""

import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(
    name: str,
    log_file: str = "retail_analytics.log",
    level: int = logging.INFO,
) -> logging.Logger:
    """
    Return a named logger that writes to:
      • A rotating file  →  OUTPUT_DIR/<log_file>  (5 MB max, 3 backups)
      • stdout/stderr    →  console

    Safe to call multiple times for the same *name* — handlers are not
    duplicated on re-import.
    """
    output_dir = os.getenv("OUTPUT_DIR", "./output")
    os.makedirs(output_dir, exist_ok=True)

    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger = logging.getLogger(name)
    if logger.handlers:          # already configured → skip
        return logger

    logger.setLevel(level)

    fh = RotatingFileHandler(
        os.path.join(output_dir, log_file),
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger
