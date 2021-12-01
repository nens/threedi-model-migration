from .file import File
from .file import Raster
from .metadata import SchemaMeta
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List
from typing import Optional
from typing import Set


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
    repo_slug: str
    sqlite_name: str  # the newest of its revisions
    settings_id: int
    settings_name: str  # the newest of its revisions
    metadata: Optional[SchemaMeta] = None
    revisions: Optional[List[SchemaRevision]] = None

    name: str = None  # max 256 characters, unique within org, generated here

    def __post_init__(self):
        self.name = f"{self.repo_slug} - {self.sqlite_name} - {self.settings_id} {self.settings_name}"[
            :256
        ]

    def get_files(self) -> Set[File]:
        result = set()
        for revision in self.revisions:
            if revision.sqlite is not None:
                result.add(revision.sqlite)
            for raster in revision.rasters:
                result.add(raster.as_file())

        return result

    def __repr__(self):
        return f"Schematisation({self.slug}, sqlite_name={self.sqlite_name}, settings_name={self.settings_name})"

    def create_report(self):
        return {
            "repository_slug": self.slug,
            "sqlite_name": self.sqlite_name,
            "settings_id": self.settings_id,
            "settings_name": self.settings_name,
            "owner": self.metadata.owner,
            "created": self.metadata.created,
            "last_update": self.revisions[0].last_update,
            "revision_count": len(self.revisions),
            "first_rev_nr": self.revisions[-1].revision_nr,
            "last_rev_nr": self.revisions[0].revision_nr,
        }
