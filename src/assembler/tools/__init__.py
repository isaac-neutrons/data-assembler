"""
Tools module for file detection and linking.

Provides utilities to detect file types, extract identifiers (run number, IPTS),
and find related files across a dataset.

Module structure:
- types.py: FileType enum, FileInfo and RelatedFiles dataclasses
- detection.py: File type detection and identifier extraction functions
- finder.py: FileFinder class for locating related files
"""

# Re-export types
# Re-export detection functions
from .detection import (
    PATTERNS,
    detect_file,
    detect_file_type,
    extract_instrument,
    extract_ipts,
    extract_run_number,
)

# Re-export finder
from .finder import FileFinder
from .types import (
    FileInfo,
    FileType,
    RelatedFiles,
)

__all__ = [
    # Types
    "FileType",
    "FileInfo",
    "RelatedFiles",
    # Detection functions
    "PATTERNS",
    "detect_file_type",
    "detect_file",
    "extract_run_number",
    "extract_ipts",
    "extract_instrument",
    # Finder
    "FileFinder",
]
