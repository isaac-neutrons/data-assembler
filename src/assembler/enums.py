"""
Enums for measurement metadata.

These enums define the valid values for key measurement attributes.
"""

from enum import Enum


class Facility(str, Enum):
    """Supported neutron/X-ray facilities."""

    SNS = "SNS"
    HFIR = "HFIR"
    LCLS = "LCLS"
    NIST = "NIST"
    OTHER = "OTHER"


class Probe(str, Enum):
    """Radiation probe types."""

    NEUTRONS = "neutrons"
    XRAY = "xray"
    OTHER = "other"


class Technique(str, Enum):
    """Measurement techniques."""

    REFLECTIVITY = "reflectivity"
    SANS = "SANS"
    EIS = "EIS"
    OTHER = "other"
