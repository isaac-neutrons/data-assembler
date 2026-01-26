"""
Type definitions for file detection and linking.

Contains enums and dataclasses for representing file information.
"""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional


class FileType(str, Enum):
    """
    Types of data files in the ingestion workflow.

    Attributes:
        REDUCED: Reduced reflectivity text file (.txt)
        RAW_HDF5: Raw HDF5/NeXus file (.h5, .nxs.h5)
        PARQUET: Parquet file from nexus-processor (.parquet)
        MODEL: Refl1d/bumps model JSON (.json)
        UNKNOWN: Unrecognized file type
    """

    REDUCED = "reduced"
    RAW_HDF5 = "raw_hdf5"
    PARQUET = "parquet"
    MODEL = "model"
    UNKNOWN = "unknown"


@dataclass
class FileInfo:
    """
    Information extracted from a detected file.

    Attributes:
        path: Absolute path to the file
        file_type: Detected FileType
        run_number: Extracted run number (if found)
        ipts: Extracted IPTS identifier (if found)
        instrument: Extracted instrument name (if found)
    """

    path: str
    file_type: FileType
    run_number: Optional[int] = None
    ipts: Optional[str] = None
    instrument: Optional[str] = None

    @property
    def filename(self) -> str:
        """Get the filename without path."""
        return Path(self.path).name

    @property
    def exists(self) -> bool:
        """Check if the file exists."""
        return Path(self.path).exists()


@dataclass
class RelatedFiles:
    """
    Collection of related files for a single run.

    Groups all files associated with a specific run number,
    making it easy to find matching reduced data, parquet metadata,
    and model files.

    Attributes:
        run_number: The run number these files belong to
        ipts: IPTS identifier (if known)
        reduced_file: Path to reduced reflectivity data
        raw_file: Path to raw HDF5/NeXus file
        model_file: Path to refl1d/bumps model JSON
        metadata_parquet: Path to metadata parquet
        sample_parquet: Path to sample parquet
        users_parquet: Path to users parquet
        daslogs_parquet: Path to daslogs parquet
        instrument_parquet: Path to instrument parquet
        other_parquet: List of other parquet files
    """

    run_number: int
    ipts: Optional[str] = None

    # Primary data files
    reduced_file: Optional[str] = None
    raw_file: Optional[str] = None
    model_file: Optional[str] = None

    # Parquet files from nexus-processor
    metadata_parquet: Optional[str] = None
    sample_parquet: Optional[str] = None
    users_parquet: Optional[str] = None
    daslogs_parquet: Optional[str] = None
    instrument_parquet: Optional[str] = None

    # Additional parquet files
    other_parquet: list[str] = field(default_factory=list)

    def has_parquet_metadata(self) -> bool:
        """Check if metadata parquet is available."""
        return self.metadata_parquet is not None

    def has_model(self) -> bool:
        """Check if a model file is available."""
        return self.model_file is not None

    def has_reduced(self) -> bool:
        """Check if reduced data file is available."""
        return self.reduced_file is not None

    def parquet_dir(self) -> Optional[str]:
        """
        Get the directory containing parquet files.

        Returns the parent directory of the first available parquet file.
        """
        for parquet in [
            self.metadata_parquet,
            self.sample_parquet,
            self.daslogs_parquet,
        ]:
            if parquet:
                return str(Path(parquet).parent)
        return None

    def completeness_score(self) -> float:
        """
        Calculate how complete the file set is (0.0 to 1.0).

        Weights:
        - reduced_file: 30%
        - metadata_parquet: 25%
        - sample_parquet: 15%
        - model_file: 15%
        - daslogs_parquet: 10%
        - users_parquet: 5%

        Returns:
            Float between 0.0 and 1.0 indicating completeness
        """
        score = 0.0
        if self.reduced_file:
            score += 0.30
        if self.metadata_parquet:
            score += 0.25
        if self.sample_parquet:
            score += 0.15
        if self.model_file:
            score += 0.15
        if self.daslogs_parquet:
            score += 0.10
        if self.users_parquet:
            score += 0.05
        return score

    def available_files(self) -> list[str]:
        """Get list of all available file paths."""
        files = []
        for attr in [
            "reduced_file",
            "raw_file",
            "model_file",
            "metadata_parquet",
            "sample_parquet",
            "users_parquet",
            "daslogs_parquet",
            "instrument_parquet",
        ]:
            path = getattr(self, attr)
            if path:
                files.append(path)
        files.extend(self.other_parquet)
        return files
