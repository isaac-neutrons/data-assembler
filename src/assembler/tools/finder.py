"""
File finder for locating related files across directories.

Provides the FileFinder class for discovering all files related
to a specific run number.
"""

from pathlib import Path
from typing import Optional

from .detection import detect_file
from .types import FileInfo, FileType, RelatedFiles


class FileFinder:
    """
    Find related files for a given run across configured search paths.

    Searches multiple directories to find all files associated with
    a run number, including reduced data, parquet metadata, and models.

    Example:
        finder = FileFinder(["/data/reduced", "/data/parquet", "/data/models"])
        related = finder.find_related_files(run_number=218386)

        if related.has_reduced():
            print(f"Found reduced: {related.reduced_file}")
        if related.has_parquet_metadata():
            print(f"Found metadata: {related.metadata_parquet}")
    """

    def __init__(self, search_paths: list[str | Path]):
        """
        Initialize the file finder.

        Args:
            search_paths: List of directories to search for related files
        """
        self.search_paths = [Path(p) for p in search_paths]

    def find_related_files(
        self,
        run_number: int,
        ipts: Optional[str] = None,
        recursive: bool = True,
    ) -> RelatedFiles:
        """
        Find all related files for a run.

        Searches all configured paths for files containing the run number
        and assigns them to appropriate slots in RelatedFiles.

        Args:
            run_number: The run number to search for
            ipts: Optional IPTS to help narrow search
            recursive: Whether to search subdirectories (default True)

        Returns:
            RelatedFiles with paths to discovered files
        """
        related = RelatedFiles(run_number=run_number, ipts=ipts)
        run_str = str(run_number)

        for search_path in self.search_paths:
            if not search_path.exists():
                continue

            # Get files to check
            if recursive:
                files = list(search_path.rglob("*"))
            else:
                files = list(search_path.glob("*"))

            for file_path in files:
                if not file_path.is_file():
                    continue

                # Check if this file is related to our run
                if run_str not in file_path.name:
                    continue

                file_info = detect_file(file_path)

                # Verify run number matches
                if file_info.run_number != run_number:
                    continue

                # Assign to appropriate slot
                self._assign_file(related, file_info)

        return related

    def _assign_file(self, related: RelatedFiles, file_info: FileInfo) -> None:
        """Assign a file to the appropriate slot in RelatedFiles."""
        path = file_info.path

        if file_info.file_type == FileType.REDUCED:
            if related.reduced_file is None:
                related.reduced_file = path

        elif file_info.file_type == FileType.RAW_HDF5:
            if related.raw_file is None:
                related.raw_file = path

        elif file_info.file_type == FileType.MODEL:
            if related.model_file is None:
                related.model_file = path

        elif file_info.file_type == FileType.PARQUET:
            self._assign_parquet(related, path)

    def _assign_parquet(self, related: RelatedFiles, path: str) -> None:
        """Assign a parquet file to the appropriate slot."""
        name = Path(path).name.lower()

        if "metadata" in name:
            if related.metadata_parquet is None:
                related.metadata_parquet = path
        elif "sample" in name:
            if related.sample_parquet is None:
                related.sample_parquet = path
        elif "user" in name:
            if related.users_parquet is None:
                related.users_parquet = path
        elif "daslog" in name:
            if related.daslogs_parquet is None:
                related.daslogs_parquet = path
        elif "instrument" in name:
            if related.instrument_parquet is None:
                related.instrument_parquet = path
        else:
            related.other_parquet.append(path)

    def find_from_file(self, file_path: str | Path) -> RelatedFiles:
        """
        Find related files starting from a known file.

        Extracts run number and IPTS from the file, then searches
        for related files.

        Args:
            file_path: Path to a known file (reduced, model, etc.)

        Returns:
            RelatedFiles with paths to discovered files

        Raises:
            ValueError: If run number cannot be extracted from file
        """
        file_info = detect_file(file_path)

        if file_info.run_number is None:
            raise ValueError(f"Could not extract run number from {file_path}")

        related = self.find_related_files(
            run_number=file_info.run_number,
            ipts=file_info.ipts,
        )

        return related

    def add_search_path(self, path: str | Path) -> None:
        """
        Add a search path to the finder.

        Args:
            path: Directory path to add
        """
        self.search_paths.append(Path(path))

    def search_path_count(self) -> int:
        """Get the number of configured search paths."""
        return len(self.search_paths)
