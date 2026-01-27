"""
Tools for file detection and linking.

Main exports:
- FileType: Enum of supported file types
- FileFinder: Utility class to locate related files

For detection functions and data types, import from specific modules::

    from assembler.tools.types import FileInfo, RelatedFiles
    from assembler.tools.detection import detect_file, extract_run_number
"""

from .finder import FileFinder
from .types import FileType

__all__ = [
    "FileType",
    "FileFinder",
]
