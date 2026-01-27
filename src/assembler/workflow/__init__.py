"""
Workflow module for data assembly.

Main exports:
- DataAssembler: Orchestrates the assembly process
- AssemblyResult: Container for assembled records

For record builders (advanced use), import from specific modules::

    from assembler.workflow.record_builders import build_reflectivity_record
"""

from .assembler import DataAssembler
from .result import AssemblyResult

__all__ = [
    "AssemblyResult",
    "DataAssembler",
]
