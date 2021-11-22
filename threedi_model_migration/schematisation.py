from .file import File
from .file import Raster
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List
from typing import Optional


__all__ = ["Schematisation", "SchemaRevision"]


SQLITE_COMPRESSION_RATIO = 7


@dataclass
class SchemaRevision:
    sqlite_path: Path  # relative to repository dir
    settings_name: str

    # copy from RepoRevision:
    revision_nr: int
    revision_hash: str
    last_update: datetime
    commit_msg: str
    commit_user: str

    # files
    sqlite: File
    rasters: List[Raster]

    def __repr__(self):
        return f"SchemaRevision({self.revision_nr})"


@dataclass
class Schematisation:
    slug: str  # matches repository slug
    sqlite_name: str  # the newest of its revisions
    settings_id: int
    settings_name: str  # the newest of its revisions
    file_count: Optional[int] = None
    total_size_mb: Optional[int] = None
    revisions: Optional[List[SchemaRevision]] = None

    @property
    def concat_name(self):
        return f"{self.slug}-{self.sqlite_name}-{self.settings_name}"

    def summarize_files(self):
        unique_sqlites = set()
        unique_rasters = set()
        for revision in self.revisions:
            unique_sqlites.add(revision.sqlite)
            unique_rasters |= set(revision.rasters)

        self.file_count = len(unique_sqlites) + len(unique_rasters)
        size_sqlites = sum(x.size for x in unique_sqlites) / SQLITE_COMPRESSION_RATIO
        size_rasters = sum(x.size for x in unique_rasters)
        self.total_size_mb = int((size_sqlites + size_rasters) / (1024 ** 2))

    def __repr__(self):
        return f"Schematisation({self.slug}, sqlite_name={self.sqlite_name}, settings_name={self.settings_name})"
