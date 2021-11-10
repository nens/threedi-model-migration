from .repository import RepoSettings
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator
from typing import List
from typing import Tuple


@dataclass
class Schematisation:
    name: str  # matches repository name
    sqlite_name: str  # the newest of its revisions
    settings_id: int
    settings_name: str  # the newest of its revisions

    @property
    def concat_name(self):
        return f"{self.name}-{self.sqlite_name}-{self.settings_name}"

    def __repr__(self):
        return f"Schematisation(name={self.name}, sqlite_name={self.sqlite_name}, settings_name={self.settings_name})"


@dataclass
class SchemaRevision:
    schematisation: Schematisation
    sqlite_path: Path  # relative to repository dir
    settings_name: str

    # copy from RepoRevision:
    revision_nr: int
    revision_hash: str
    last_update: datetime
    commit_msg: str
    commit_user: str

    def __repr__(self):
        first_line = self.commit_msg.split("\n")[0]
        return f"SchemaRevision(revision_hash={self.revision_hash[:8]}, commit_msg={first_line})"


def repository_to_schematisations(
    settings_iter=Iterator[RepoSettings],
) -> Iterator[Tuple[Schematisation, List[SchemaRevision]]]:
    schematisations = {}
    for settings in settings_iter:
        sqlite = settings.sqlite
        revision = sqlite.revision
        repository = revision.repository
        unique_id = (repository.name, str(sqlite.sqlite_path), settings.settings_id)
        if unique_id not in schematisations:
            schematisations[unique_id] = (
                Schematisation(
                    name=repository.name,
                    sqlite_name=str(sqlite.sqlite_path),
                    settings_id=settings.settings_id,
                    settings_name=settings.settings_name,
                ),
                [],
            )
        schematisations[unique_id][1].append(
            SchemaRevision(
                schematisation=schematisations[unique_id][0],
                sqlite_path=sqlite.sqlite_path,
                settings_name=settings.settings_name,
                revision_nr=revision.revision_nr,
                revision_hash=revision.revision_hash,
                last_update=revision.last_update,
                commit_msg=revision.commit_msg,
                commit_user=revision.commit_user,
            )
        )

    return list(schematisations.values())
