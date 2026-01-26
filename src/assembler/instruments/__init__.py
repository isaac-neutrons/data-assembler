"""
Instrument-specific modules for data assembly.

Each instrument at the neutron facilities has its own conventions for:
- DASLog naming (e.g., temperature sensors, motor positions)
- Sample description fields
- Metadata conventions
- Measurement geometry

This module provides instrument-specific handlers that know how to
interpret and extract data from each instrument's output.
"""

from .base import Instrument, InstrumentRegistry
from .ref_l import REF_L

__all__ = [
    "Instrument",
    "InstrumentRegistry",
    "REF_L",
]
