"""
Parser for reduced reflectivity data text files.

Parses the text file format output by the SNS reduction software,
extracting header metadata and Q/R/dR/dQ data columns.
"""

import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from dateutil import parser as date_parser


@dataclass
class ReductionRun:
    """Information about a single data run in the reduction."""

    two_theta: float


@dataclass
class ReducedData:
    """
    Complete parsed data from a reduced reflectivity text file.

    Contains both header metadata and the actual data columns.
    """

    # File info
    file_path: str

    # Header metadata
    experiment_id: Optional[str] = None  # IPTS number
    run_number: Optional[int] = None
    reduction_version: Optional[str] = None
    run_title: Optional[str] = None
    run_start_time: Optional[datetime] = None
    reduction_time: Optional[datetime] = None

    # Run info (for multi-angle datasets)
    runs: list[ReductionRun] = field(default_factory=list)

    # Data columns
    q: list[float] = field(default_factory=list)  # Q in Å⁻¹
    r: list[float] = field(default_factory=list)  # Reflectivity
    dr: list[float] = field(default_factory=list)  # dR uncertainty
    dq: list[float] = field(default_factory=list)  # dQ resolution (FWHM)

    @property
    def num_points(self) -> int:
        """Number of data points."""
        return len(self.q)

    @property
    def q_range(self) -> tuple[float, float]:
        """Q range (min, max)."""
        if not self.q:
            return (0.0, 0.0)
        return (min(self.q), max(self.q))

    @property
    def primary_run(self) -> Optional[int]:
        """Get the primary run number (first data run)."""
        if self.runs:
            return self.runs[0].data_run
        return self.run_number


class ReducedParser:
    """
    Parser for reduced reflectivity text files.

    Handles the standard SNS reduction output format with:
    - Comment lines starting with #
    - Header section with metadata
    - Data section with Q, R, dR, dQ columns

    Usage:
        parser = ReducedParser()
        data = parser.parse("/path/to/REFL_218386_combined_data_auto.txt")

        print(f"Run {data.run_number}: {data.num_points} points")
        print(f"Q range: {data.q_range}")
    """

    # Regex patterns for header parsing
    # Note: patterns with $ need MULTILINE to match end-of-line, not just end-of-string
    PATTERNS = {
        "experiment": re.compile(r"Experiment\s+(IPTS-\d+)\s+Run\s+(\d+)", re.IGNORECASE),
        "reduction": re.compile(r"Reduction\s+(.+)$", re.IGNORECASE | re.MULTILINE),
        "run_title": re.compile(r"Run title:\s*(.+)$", re.IGNORECASE | re.MULTILINE),
        "run_start": re.compile(r"Run start time:\s*(.+)$", re.IGNORECASE | re.MULTILINE),
        "reduction_time": re.compile(r"Reduction time:\s*(.+)$", re.IGNORECASE | re.MULTILINE),
    }

    # Pattern for run info table - extracts two_theta (3rd column)
    RUN_TABLE_PATTERN = re.compile(
        r"^\s*\d+\s+\d+\s+([\d.eE+-]+)",
        re.MULTILINE,
    )

    def __init__(self):
        """Initialize the parser."""
        pass

    def parse(self, file_path: str | Path) -> ReducedData:
        """
        Parse a reduced reflectivity text file.

        Args:
            file_path: Path to the text file

        Returns:
            ReducedData with parsed header and data

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file format is invalid
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(file_path, "r") as f:
            content = f.read()

        return self.parse_content(content, str(file_path))

    def parse_content(self, content: str, file_path: str = "") -> ReducedData:
        """
        Parse reduced data from string content.

        Args:
            content: File content as string
            file_path: Optional file path for reference

        Returns:
            ReducedData with parsed header and data
        """
        lines = content.strip().split("\n")

        result = ReducedData(file_path=file_path)

        # Separate header and data
        header_lines = []
        data_lines = []
        in_data = False

        for line in lines:
            stripped = line.strip()

            if not stripped:
                continue

            if stripped.startswith("#"):
                header_lines.append(stripped[1:].strip())
            else:
                in_data = True
                data_lines.append(stripped)

        # Parse header
        self._parse_header(header_lines, result)

        # Parse data
        self._parse_data(data_lines, result)

        return result

    def _parse_header(self, header_lines: list[str], result: ReducedData) -> None:
        """Parse header comment lines."""
        full_header = "\n".join(header_lines)

        # Extract experiment ID and run number
        match = self.PATTERNS["experiment"].search(full_header)
        if match:
            result.experiment_id = match.group(1)
            result.run_number = int(match.group(2))

        # Extract reduction version
        match = self.PATTERNS["reduction"].search(full_header)
        if match:
            result.reduction_version = match.group(1).strip()

        # Extract run title
        match = self.PATTERNS["run_title"].search(full_header)
        if match:
            result.run_title = match.group(1).strip()

        # Extract run start time
        match = self.PATTERNS["run_start"].search(full_header)
        if match:
            try:
                result.run_start_time = date_parser.parse(match.group(1).strip())
            except (ValueError, TypeError):
                pass

        # Extract reduction time
        match = self.PATTERNS["reduction_time"].search(full_header)
        if match:
            try:
                result.reduction_time = date_parser.parse(match.group(1).strip())
            except (ValueError, TypeError):
                pass

        # Extract run info table (two_theta values)
        for match in self.RUN_TABLE_PATTERN.finditer(full_header):
            result.runs.append(
                ReductionRun(
                    two_theta=float(match.group(1)),
                )
            )

    def _parse_data(self, data_lines: list[str], result: ReducedData) -> None:
        """Parse data columns."""
        for line in data_lines:
            parts = line.split()

            if len(parts) < 4:
                continue

            try:
                q = float(parts[0])
                r = float(parts[1])
                dr = float(parts[2])
                dq = float(parts[3])

                result.q.append(q)
                result.r.append(r)
                result.dr.append(dr)
                result.dq.append(dq)
            except (ValueError, IndexError):
                # Skip malformed lines
                continue


def extract_run_number_from_filename(filename: str) -> Optional[int]:
    """
    Extract run number from a reduced data filename.

    Handles patterns like:
    - REFL_218386_combined_data_auto.txt
    - REF_L_218386_combined.txt
    - refl_218386.txt

    Args:
        filename: Filename to parse

    Returns:
        Run number or None if not found
    """
    # Pattern: REFL_NNNNNN or REF_L_NNNNNN
    match = re.search(r"REF[L_]*_?(\d{5,7})", filename, re.IGNORECASE)
    if match:
        return int(match.group(1))

    # Generic pattern: any 6-digit number
    match = re.search(r"(\d{6})", filename)
    if match:
        return int(match.group(1))

    return None
