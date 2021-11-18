from . import hg
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import logging
import sqlite3


logger = logging.getLogger(__name__)

DEFAULT_REMOTE = "https://hg.lizard.net"


@dataclass
class RepoSettings:
    settings_id: int
    settings_name: str

    def __repr__(self):
        return f"RepoSettings(id={self.settings_id}, name={self.settings_name})"


@dataclass
class RepoSqlite:
    sqlite_path: Path  # relative path within repository
    settings: Optional[List[RepoSettings]] = None

    def get_settings(
        self,
        repository: Optional["Repository"] = None,
        revision: Optional["RepoRevision"] = None,
    ) -> List[RepoSettings]:
        if self.settings is None:
            if repository is None or revision is None:
                raise ValueError("Provide the repository and revision when inspecting")
            repository.checkout(revision.revision_hash)
            full_path = repository.path / self.sqlite_path

            con = sqlite3.connect(full_path)
            try:
                with con:
                    cursor = con.execute(
                        "SELECT id, name FROM v2_global_settings ORDER BY id"
                    )
                records = cursor.fetchall()
            except sqlite3.OperationalError as e:
                logger.warning(f"{self} OperationalError {e}")
                return []
            finally:
                con.close()

            self.settings = [RepoSettings(*record) for record in records]

        return self.settings

    def __repr__(self):
        return f"RepoSqlite({self.sqlite_path})"


@dataclass
class RepoRevision:
    revision_nr: int
    revision_hash: str
    last_update: datetime
    commit_msg: str
    commit_user: str
    sqlites: Optional[List[RepoSqlite]] = None

    def get_sqlites(
        self, repository: Optional["Repository"] = None
    ) -> List[RepoSqlite]:
        """Return a list of sqlites in this revision"""
        if self.sqlites is None:
            if repository is None:
                raise ValueError("Provide the repository when inspecting")
            repository.checkout(self.revision_hash)
            base = repository.path.resolve()
            glob = base.glob("*.sqlite")
            self.sqlites = [
                RepoSqlite(sqlite_path=path.relative_to(base)) for path in sorted(glob)
            ]

        return self.sqlites

    @classmethod
    def from_log(cls, revision_nr, **fields):
        return cls(revision_nr=revision_nr + 1, **fields)  # like in model databank

    def __repr__(self):
        return f"RepoRevision({self.revision_nr})"


@dataclass
class Repository:
    base_path: Path
    slug: str
    revisions: Optional[List[RepoRevision]] = None

    @property
    def path(self):
        return self.base_path / self.slug

    @property
    def remote_full(self):
        return self.remote + "/" + self.slug

    def download(self, remote):
        """Get the latest commits from the remote (calls hg clone / pull and lfpull)"""
        if self.path.exists():
            logger.info(f"Pulling from {remote}...")
            hg.pull(self.path, remote)
            logger.info("Done.")
        else:
            logger.info(f"Cloning from {remote}...")
            hg.clone(self.path, remote)
            logger.info("Done.")
        logger.info("Pulling largefiles...")
        hg.pull_all_largefiles(self.path, remote)
        logger.info("Done.")

    def get_revisions(
        self, last_update: Optional[datetime] = None
    ) -> List[RepoRevision]:
        """Return a list of revisions, ordered newest first (calls hg log)

        Optionally filter by last_update. If supplied, only revisions newer than that
        date are considered.
        """
        if self.revisions is None or last_update is not None:
            revisions = []
            for record in hg.log(self.path):
                revision = RepoRevision.from_log(**record)
                if last_update is not None:
                    truncated_revision_last_update = revision.last_update.replace(
                        hour=0, minute=0, second=0, microsecond=0, tzinfo=None
                    )
                    if truncated_revision_last_update < last_update:
                        continue

                revisions.append(revision)

            self.revisions = revisions
        return self.revisions

    def checkout(self, revision_hash: str):
        """Update the working directory to given revision hash (calls hg update)"""
        try:
            int(revision_hash)
        except ValueError:
            pass
        else:
            revision_hash -= 1  # model databank does +1 on revision_nr display
        hg.update(self.path, revision_hash)
        logger.info(f"Updated working directory to revision {revision_hash}.")

    def inspect(
        self, last_update: Optional[datetime] = None
    ) -> Iterator[Tuple[RepoRevision, RepoSqlite, RepoSettings]]:
        """Iterate over all unique (revision, sqlite, global_setting) combinations.

        As a side effect, the results are cached on this object.

        Optionally filter by last_update. If supplied, only revisions newer than that
        date are considered.
        """
        for revision in self.get_revisions(last_update=last_update):
            for sqlite in revision.get_sqlites(repository=self):
                for settings in sqlite.get_settings(repository=self, revision=revision):
                    yield revision, sqlite, settings
        # go back to tip
        self.checkout("tip")
