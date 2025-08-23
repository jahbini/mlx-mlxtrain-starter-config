# mlxtrain/__init__.py
"""
mlxtrain package

This package holds shared helpers and utilities for your scripts
and notebooks (e.g., config loading, logging, JSON I/O).
"""

__version__ = "0.1.0"

from . import utils

# (Optional) re-export some common helpers for convenience:
from .utils import (
    load_config,
    dump_json,
    set_logger,
    parse_args,
    apply_overrides,
)
