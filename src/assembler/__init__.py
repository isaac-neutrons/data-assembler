"""
Data Assembler - Automated reflectivity data ingestion workflow.

This package provides tools to ingest reflectometry data from multiple sources
and assemble them into a structured format for the scientific data lakehouse.

Submodules:
- assembler.parsers: File parsers (ReducedParser, ParquetParser, ModelParser)
- assembler.workflow: Assembly orchestration (DataAssembler, AssemblyResult)
- assembler.writers: Output writers (ParquetWriter, JSONWriter)
- assembler.tools: File detection utilities (FileType, FileFinder)
- assembler.instruments: Instrument-specific handlers (REF_L)

Example::

    from assembler.parsers import ReducedParser, ParquetParser, ModelParser
    from assembler.workflow import DataAssembler
    from assembler.writers import ParquetWriter
"""

__version__ = "0.1.0"
