"""
File detection utilities.

Functions for detecting file types and extracting identifiers
(run numbers, IPTS, instrument) from file paths and content.
"""

import re
from pathlib import Path
from typing import Optional

from .types import FileInfo, FileType

# Regex patterns for extracting identifiers
PATTERNS = {
    # Run number patterns
    "run_from_refl_filename": re.compile(r"REFL_(\d+)", re.IGNORECASE),
    "run_from_ref_l_filename": re.compile(r"REF_L_(\d+)", re.IGNORECASE),
    "run_from_generic": re.compile(r"[_-](\d{5,7})[_.-]"),
    # IPTS patterns
    "ipts_from_path": re.compile(r"(IPTS-\d+)", re.IGNORECASE),
    "ipts_from_content": re.compile(r"IPTS[- ]?(\d+)", re.IGNORECASE),
    # Instrument patterns
    "instrument_ref_l": re.compile(r"REF[_-]?L", re.IGNORECASE),
}


def detect_file_type(file_path: str | Path) -> FileType:
    """
    Detect the type of a data file based on extension and content.

    Examines file extension first, then content for ambiguous types
    (JSON, TXT).

    Args:
        file_path: Path to the file

    Returns:
        FileType enum value

    Example:
        >>> detect_file_type("/data/REF_L_218386.txt")
        FileType.REDUCED
        >>> detect_file_type("/data/metadata.parquet")
        FileType.PARQUET
    """
    path = Path(file_path)

    if not path.exists():
        return FileType.UNKNOWN

    suffix = path.suffix.lower()
    name = path.name.lower()

    # Check by extension
    if suffix == ".parquet":
        return FileType.PARQUET
    elif suffix in (".h5", ".hdf5") or name.endswith(".nxs.h5"):
        return FileType.RAW_HDF5
    elif suffix == ".json":
        return _detect_json_type(path)
    elif suffix == ".txt":
        return _detect_text_type(path)

    return FileType.UNKNOWN


def _detect_json_type(path: Path) -> FileType:
    """Detect if a JSON file is a refl1d model."""
    try:
        # Read first few KB to check for model indicators
        with open(path, "r") as f:
            content = f.read(4096)

        # Look for bumps/refl1d indicators
        if '"$schema"' in content and "bumps" in content.lower():
            return FileType.MODEL
        if '"sample"' in content and '"layers"' in content:
            return FileType.MODEL
        if "refl1d" in content.lower():
            return FileType.MODEL

    except Exception:
        pass

    return FileType.UNKNOWN


def _detect_text_type(path: Path) -> FileType:
    """Detect if a text file is reduced reflectivity data."""
    try:
        with open(path, "r") as f:
            lines = [f.readline() for _ in range(20)]
            content = "".join(lines)

        # Look for reduced data indicators
        indicators = [
            "# Experiment" in content or "# IPTS" in content,
            "Q [1/Angstrom]" in content or "Q(1/A)" in content.replace(" ", ""),
            "Reduction" in content,
            "combined_data" in path.name.lower(),
            "REFL_" in path.name.upper(),
        ]

        if sum(indicators) >= 2:
            return FileType.REDUCED

    except Exception:
        pass

    return FileType.UNKNOWN


def extract_run_number(file_path: str | Path) -> Optional[int]:
    """
    Extract run number from a file path or name.

    Tries multiple patterns in order of specificity:
    1. REFL_NNNNNN pattern
    2. REF_L_NNNNNN pattern
    3. Generic _NNNNN_ pattern (5-7 digits)

    Args:
        file_path: Path to the file

    Returns:
        Run number as integer, or None if not found

    Example:
        >>> extract_run_number("/data/REF_L_218386_combined.txt")
        218386
    """
    path = Path(file_path)
    name = path.name

    # Try specific patterns first
    for pattern_name in ["run_from_refl_filename", "run_from_ref_l_filename"]:
        match = PATTERNS[pattern_name].search(name)
        if match:
            return int(match.group(1))

    # Try generic pattern
    match = PATTERNS["run_from_generic"].search(name)
    if match:
        return int(match.group(1))

    # Try in full path
    match = PATTERNS["run_from_generic"].search(str(path))
    if match:
        return int(match.group(1))

    return None


def extract_ipts(file_path: str | Path, content: Optional[str] = None) -> Optional[str]:
    """
    Extract IPTS number from file path or content.

    Args:
        file_path: Path to the file
        content: Optional file content to search

    Returns:
        IPTS string (e.g., "IPTS-34347"), or None if not found

    Example:
        >>> extract_ipts("/SNS/REF_L/IPTS-34347/data/file.txt")
        "IPTS-34347"
    """
    path_str = str(file_path)

    # Try from path first
    match = PATTERNS["ipts_from_path"].search(path_str)
    if match:
        return match.group(1).upper()

    # Try from content if provided
    if content:
        match = PATTERNS["ipts_from_content"].search(content)
        if match:
            return f"IPTS-{match.group(1)}"

    return None


def extract_instrument(file_path: str | Path) -> Optional[str]:
    """
    Extract instrument name from file path.

    Args:
        file_path: Path to the file

    Returns:
        Instrument name (e.g., "REF_L"), or None if not found

    Example:
        >>> extract_instrument("/SNS/REF_L/data/run.h5")
        "REF_L"
    """
    path_str = str(file_path)

    if PATTERNS["instrument_ref_l"].search(path_str):
        return "REF_L"

    return None


def detect_file(file_path: str | Path) -> FileInfo:
    """
    Detect file type and extract all available identifiers.

    This is the main entry point for file detection. It combines
    type detection with identifier extraction.

    Args:
        file_path: Path to the file

    Returns:
        FileInfo with type and identifiers

    Example:
        >>> info = detect_file("/data/REF_L_218386_combined.txt")
        >>> info.file_type
        FileType.REDUCED
        >>> info.run_number
        218386
    """
    path = Path(file_path)

    file_type = detect_file_type(path)
    run_number = extract_run_number(path)
    ipts = extract_ipts(path)
    instrument = extract_instrument(path)

    # For reduced files, try to get IPTS from content
    if file_type == FileType.REDUCED and ipts is None:
        try:
            with open(path, "r") as f:
                content = f.read(2048)
            ipts = extract_ipts(path, content)
        except Exception:
            pass

    return FileInfo(
        path=str(path.absolute()),
        file_type=file_type,
        run_number=run_number,
        ipts=ipts,
        instrument=instrument,
    )
