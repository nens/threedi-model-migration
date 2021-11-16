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
    sqlite: "RepoSqlite"

    def __repr__(self):
        return f"RepoSettings(id={self.settings_id}, name={self.settings_name})"


@dataclass
class RepoSqlite:
    sqlite_path: Path  # relative path within repository
    revision: "RepoRevision"

    @property
    def settings(self) -> List[RepoSettings]:
        self.revision.repository.checkout(self.revision.revision_hash)
        full_path = self.revision.repository.path / self.sqlite_path

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

        return [RepoSettings(*record, sqlite=self) for record in records]

    def __repr__(self):
        return f"RepoSqlite({self.sqlite_path})"


@dataclass
class RepoRevision:
    repository: "Repository"
    revision_nr: int
    revision_hash: str
    last_update: datetime
    commit_msg: str
    commit_user: str

    @property
    def sqlites(self) -> List["RepoSqlite"]:
        """Return a list of sqlites in this revision"""
        self.repository.checkout(self.revision_hash)
        base = self.repository.path.resolve()
        glob = base.glob("*.sqlite")
        return [
            RepoSqlite(revision=self, sqlite_path=path.relative_to(base))
            for path in sorted(glob)
        ]

    def __repr__(self):
        first_line = self.commit_msg.split("\n")[0]
        return f"RepoRevision(revision_hash={self.revision_hash[:8]}, commit_msg={first_line})"


class Repository:
    def __init__(self, base_path: Path, slug: str, remote: str = DEFAULT_REMOTE):
        self.base_path = base_path
        self.slug = slug
        if remote.endswith("/"):
            remote = remote[:-1]
        self.remote = remote

    @property
    def path(self):
        return self.base_path / self.slug

    @property
    def remote_full(self):
        return self.remote + "/" + self.slug

    def download(self):
        """Get the latest commits from the remote (calls hg clone / pull and lfpull)"""
        if self.remote is None:
            raise ValueError("Cannot download because remote is not set")
        if self.path.exists():
            logger.info(f"Pulling from {self.remote_full}...")
            hg.pull(self.path, self.remote_full)
            logger.info("Done.")
        else:
            logger.info(f"Cloning from {self.remote_full}...")
            hg.clone(self.path, self.remote_full)
            logger.info("Done.")
        logger.info("Pulling largefiles...")
        hg.pull_all_largefiles(self.path)
        logger.info("Done.")

    @property
    def revisions(self) -> List["RepoRevision"]:
        """Return a list of revisions, ordered newest first (calls hg log)"""
        return [RepoRevision(repository=self, **x) for x in hg.log(self.path)]

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

        Optionally filter by last_update. If supplied, only revisions newer than that
        date are considered.
        """
        for revision in self.revisions:
            if last_update is not None:
                truncated_revision_last_update = revision.last_update.replace(
                    hour=0, minute=0, second=0, microsecond=0, tzinfo=None
                )
                if truncated_revision_last_update < last_update:
                    continue

            for sqlite in revision.sqlites:
                for settings in sqlite.settings:
                    yield revision, sqlite, settings
        # go back to tip
        self.checkout("tip")
