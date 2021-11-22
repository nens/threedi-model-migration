from .file import File
from .file import Raster
from .metadata import SchemaMeta
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List
from typing import Optional
from typing import Set
from typing import Union


__all__ = ["Schematisation", "SchemaRevision"]


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
    metadata: Optional[SchemaMeta] = None
    revisions: Optional[List[SchemaRevision]] = None

    @property
    def concat_name(self):
        return f"{self.slug}-{self.sqlite_name}-{self.settings_name}"

    def get_files(self) -> Set[Union[File, Raster]]:
        result = set()
        for revision in self.revisions:
            result.add(revision.sqlite)
            for raster in revision.rasters:
                result.add(raster)

        return result

    def __repr__(self):
        return f"Schematisation({self.slug}, sqlite_name={self.sqlite_name}, settings_name={self.settings_name})"
