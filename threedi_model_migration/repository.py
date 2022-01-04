from . import hg
from .file import compare_paths
from .file import File
from .file import Raster
from .file import RasterOptions
from .sql import RASTER_SQL_MAP
from .sql import select
from .sql import SETTINGS_SQL
from .sql import VERSION_SQL
from copy import copy
from datetime import datetime
from pathlib import Path
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import dataclasses
import logging
import shutil
import sqlite3


__all__ = ["Repository", "RepoSettings", "RepoRevision", "RepoSqlite"]

logger = logging.getLogger(__name__)

DEFAULT_REMOTE = "https://hg.lizard.net"


@dataclasses.dataclass
class RepoSettings:
    settings_id: int
    settings_name: str
    rasters: List[Raster] = ()

    @classmethod
    def from_record(cls, record):
        rasters = []
        for raster_path, raster_option in zip(record[2:], RasterOptions):
            if not raster_path:
                continue
            rasters.append(Raster(raster_type=raster_option.name, path=raster_path))
        return cls(record[0], record[1], rasters)

    def __repr__(self):
        return f"RepoSettings(id={self.settings_id}, name={self.settings_name})"


@dataclasses.dataclass
class RepoSqlite:
    sqlite_path: Path  # relative path within repository
    settings: Optional[List[RepoSettings]] = None
    version: Optional[int] = None

    def set_version(self, repository: "Repository"):
        """Extract the latest migration id. Assumes the file is present."""
        full_path = repository.path / self.sqlite_path
        try:
            ((self.version,),) = select(full_path, VERSION_SQL)
        except (sqlite3.OperationalError, sqlite3.DatabaseError, ValueError):
            logger.warning(f"No version found in {self}")

    def get_settings(
        self,
        do_checkout=True,
        repository: Optional["Repository"] = None,
        revision: Optional["RepoRevision"] = None,
    ) -> List[RepoSettings]:
        if self.settings is None:
            if repository is None or revision is None:
                raise ValueError("Provide the repository and revision when inspecting")
            if do_checkout:
                repository.checkout(revision.revision_hash)
            full_path = repository.path / self.sqlite_path

            try:
                records = select(full_path, SETTINGS_SQL)
            except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
                logger.warning(f"{self} error {e}")
                records = []

            if len(records) == 0:
                self.settings = []
                return self.settings

            # Pragmatic fix: in earlier sqlite schemas, some tables / columns
            # may not exist. Do each query separately and wrap in try..except.
            records = [list(record) for record in records]
            for option in RasterOptions:
                try:
                    paths = select(full_path, RASTER_SQL_MAP[option])
                except sqlite3.OperationalError:
                    paths = [(None,)] * len(records)

                for record, path in zip(records, paths):
                    raster_path = path[0]
                    if raster_path:
                        raster_path = self.sqlite_path.parent / Path(raster_path)
                    record.append(raster_path)

            self.settings = [RepoSettings.from_record(record) for record in records]

        return self.settings

    def __repr__(self):
        return f"RepoSqlite({self.sqlite_path})"


@dataclasses.dataclass
class RepoRevision:
    revision_nr: int
    revision_hash: str
    last_update: datetime
    commit_msg: str
    commit_user: str
    changes: List[File] = ()
    sqlites: Optional[List[RepoSqlite]] = None

    def get_sqlites(
        self, do_checkout=True, repository: Optional["Repository"] = None
    ) -> List[RepoSqlite]:
        """Return a list of sqlites in this revision"""
        if self.sqlites is None:
            if repository is None:
                raise ValueError("Provide the repository when inspecting")
            if do_checkout:
                repository.checkout(self.revision_hash)
            base = repository.path.resolve()
            glob = [path.relative_to(base) for path in base.glob("**/*.sqlite")]
            self.sqlites = [
                RepoSqlite(sqlite_path=path)
                for path in sorted(glob)
                if path.parts[0] != ".hglf"
            ]
            for sqlite in self.sqlites:
                sqlite.set_version(repository=repository)
            # also compute hashes now we have the checkout
            for file in self.changes:
                file.compute_md5(base_path=base)

        return self.sqlites

    @classmethod
    def from_log(cls, revision_nr, **fields):
        fields["changes"] = [File(x) for x in fields["changes"]]
        return cls(revision_nr=revision_nr, **fields)  # like in model databank

    def __repr__(self):
        return f"RepoRevision({self.revision_nr})"


@dataclasses.dataclass
class Repository:
    base_path: Path
    slug: str
    revisions: Optional[List[RepoRevision]] = None

    @property
    def path(self):
        return self.base_path / self.slug

    def __repr__(self):
        return f"Repository({self.slug})"

    @property
    def remote_full(self):
        return self.remote + "/" + self.slug

    def download(self, remote, ifnewer=False, lfclear=False):
        """Get the latest commits from the remote (calls hg clone / pull and lfpull)

        Returns whether the download was actually done.
        """
        if ifnewer and self.revisions:
            logger.info(f"Requesting last revision hash from {remote}...")
            last_revision_hash = hg.identify_tip(remote)
            if self.revisions[0].revision_hash == last_revision_hash:
                # nothing to do
                logger.info("Skipping download as revision hash is not newer.")
                return False
            else:
                logger.info(f"Detected a newer revision hash {last_revision_hash}.")

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
        if lfclear:
            logger.info("Clearing largefiles usercache...")
            hg.clear_largefiles_cache()

        return True

    def identify_tip(self, remote):
        """Returns the hash of the latest revision at the remote"""
        return hg.identify_tip(remote)

    def delete(self):
        if self.path.exists():
            shutil.rmtree(self.path)

    def get_revisions(
        self, last_update: Optional[datetime] = None
    ) -> List[RepoRevision]:
        """Return a list of revisions, ordered newest first (calls hg log)

        Optionally filter by last_update. If supplied, only revisions newer than that
        date are considered.
        """
        new_revs = [RepoRevision.from_log(**x) for x in hg.log(self.path)]
        existing_hashes = set(r.revision_hash for r in (self.revisions or []))
        if existing_hashes:
            # Merge with what is already present
            for new_rev in new_revs:
                if new_rev.revision_hash not in existing_hashes:
                    self.revisions.append(new_rev)
            self.revisions = sorted(self.revisions, key=lambda x: -x.revision_nr)
            rev_nrs = [rev.revision_nr for rev in self.revisions]
            if len(rev_nrs) != len(set(rev_nrs)):
                raise RuntimeError(f"{self} has non-unique revisions numbers!")
        else:
            self.revisions = new_revs

        # filter on last_update (only for return value)
        if last_update is not None:
            revisions = [
                r
                for r in self.revisions
                if r.last_update.replace(tzinfo=None) >= last_update
            ]
            # patch the 'changes' of the oldest commit when filtering on last_update
            # so that all files are present in the Repository
            if len(revisions) > 0:
                revisions[0] = copy(revisions[0])
                revisions[0].changes = [
                    File(x) for x in hg.files(self.path, revisions[0].revision_hash)
                ]
            return revisions
        else:
            return self.revisions

    def checkout(self, hash_or_nr: str):
        """Update the working directory to given revision hash (calls hg update)"""
        try:
            hash_or_nr = int(hash_or_nr)
        except ValueError:
            pass
        hg.update(self.path, hash_or_nr)
        logger.info(f"Updated working directory to revision {hash_or_nr}.")

    def inspect(
        self, last_update: Optional[datetime] = None
    ) -> Iterator[Tuple[RepoRevision, RepoSqlite, RepoSettings]]:
        """Iterate over all unique (revision, sqlite, global_setting) combinations.

        As a side effect, the results are cached on this object.

        Optionally filter by last_update. If supplied, only revisions newer than that
        date are considered.
        """
        for revision in self.get_revisions(last_update=last_update):
            revision_has_sqlites = revision.sqlites is not None
            for sqlite in revision.get_sqlites(repository=self, do_checkout=True):
                # Workaround: If the revision had sqlites, this means they are inspected
                # We patch sqlite.settings to [] if it is None to suppress re-inspection
                if revision_has_sqlites and sqlite.settings is None:
                    sqlite.settings = []
                for settings in sqlite.get_settings(
                    repository=self, revision=revision, do_checkout=False
                ):
                    yield revision, sqlite, settings
        # go back to tip
        self.checkout("tip")

    def get_file(self, revision_nr: int, path: Path) -> Tuple[int, File]:
        """Inspect revisions <revision_nr> and earlier to get a File"""
        for revision in self.revisions:
            if revision.revision_nr > revision_nr:
                continue
            for file in revision.changes:
                if compare_paths(file.path, path):
                    return revision.revision_nr, file

        return None, None
