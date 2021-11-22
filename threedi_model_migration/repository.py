from . import hg
from .file import File
from .file import Raster
from .file import RasterOptions
from datetime import datetime
from pathlib import Path
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import dataclasses
import logging
import sqlite3


__all__ = ["Repository", "RepoSettings", "RepoRevision", "RepoSqlite"]

logger = logging.getLogger(__name__)

DEFAULT_REMOTE = "https://hg.lizard.net"
RASTER_SQL_MAP = {
    RasterOptions.dem_raw_file: "v2_global_settings.dem_file",
    RasterOptions.equilibrium_infiltration_rate_file: "v2_groundwater.equilibrium_infiltration_rate_file",
    RasterOptions.frict_coef_file: "v2_global_settings.frict_coef_file",
    RasterOptions.initial_groundwater_level_file: "v2_global_settings.initial_groundwater_level_file",
    RasterOptions.initial_waterlevel_file: "v2_global_settings.initial_waterlevel_file",
    RasterOptions.groundwater_hydro_connectivity_file: "v2_groundwater.groundwater_hydro_connectivity_file",
    RasterOptions.groundwater_impervious_layer_level_file: "v2_groundwater.groundwater_impervious_layer_level_file",
    RasterOptions.infiltration_decay_period_file: "v2_groundwater.infiltration_decay_period_file",
    RasterOptions.initial_infiltration_rate_file: "v2_groundwater.initial_infiltration_rate_file",
    RasterOptions.leakage_file: "v2_groundwater.leakage_file",
    RasterOptions.phreatic_storage_capacity_file: "v2_groundwater.phreatic_storage_capacity_file",
    RasterOptions.hydraulic_conductivity_file: "v2_interflow.hydraulic_conductivity_file",
    RasterOptions.porosity_file: "v2_interflow.porosity_file",
    RasterOptions.infiltration_rate_file: "v2_simple_infiltration.infiltration_rate_file",
    RasterOptions.max_infiltration_capacity_file: "v2_simple_infiltration.max_infiltration_capacity_file",
    RasterOptions.interception_file: "v2_global_settings.interception_file",
}
SQL = (
    "SELECT v2_global_settings.id, v2_global_settings.name,"
    + ", ".join(RASTER_SQL_MAP[name] for name in RasterOptions)
    + """
FROM v2_global_settings
LEFT JOIN v2_interflow ON v2_interflow.id = v2_global_settings.interflow_settings_id
LEFT JOIN v2_simple_infiltration ON v2_simple_infiltration.id = v2_global_settings.simple_infiltration_settings_id
LEFT JOIN v2_groundwater ON v2_groundwater.id = v2_global_settings.groundwater_settings_id
ORDER BY v2_global_settings.id
"""
)


@dataclasses.dataclass
class RepoSettings:
    settings_id: int
    settings_name: str
    rasters: List[Raster] = ()

    @classmethod
    def from_record(cls, record, repository: Optional["Repository"] = None):
        rasters = []
        for raster_path, raster_option in zip(record[2:], RasterOptions):
            if not raster_path:
                continue
            raster_path = Path(raster_path)
            fullpath = repository.path / raster_path
            if not fullpath.exists():
                logger.warn(f"Referenced raster {raster_path} does not exist.")
            rasters.append(Raster(raster_type=raster_option, path=raster_path))
        return cls(record[0], record[1], rasters)

    def __repr__(self):
        return f"RepoSettings(id={self.settings_id}, name={self.settings_name})"


@dataclasses.dataclass
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
                    cursor = con.execute(SQL)
                records = cursor.fetchall()
            except sqlite3.OperationalError as e:
                logger.warning(f"{self} OperationalError {e}")
                return []
            finally:
                con.close()

            self.settings = [
                RepoSettings.from_record(record, repository=repository)
                for record in records
            ]

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
            # also compute hashes now we have the checkout
            for file in self.changes:
                file.compute_md5(base_path=base)

        return self.sqlites

    @classmethod
    def from_log(cls, revision_nr, **fields):
        fields["changes"] = [File(x) for x in fields["changes"]]
        return cls(revision_nr=revision_nr + 1, **fields)  # like in model databank

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
                        break
                revisions.append(revision)

            # patch the 'changes' if filtering on last_update so that all files are
            # present in the repository
            if last_update is not None and len(revisions) > 0:
                revisions[-1].changes = [
                    File(x) for x in hg.files(self.path, revisions[-1].revision_hash)
                ]
            self.revisions = revisions
        return self.revisions

    def checkout(self, hash_or_nr: str):
        """Update the working directory to given revision hash (calls hg update)"""
        try:
            hash_or_nr = int(hash_or_nr)
        except ValueError:
            pass
        else:
            hash_or_nr -= 1  # model databank does +1 on revision_nr display
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
            for sqlite in revision.get_sqlites(repository=self):
                for settings in sqlite.get_settings(repository=self, revision=revision):
                    yield revision, sqlite, settings
        # go back to tip
        self.checkout("tip")

    @classmethod
    def from_dict(cls, dct):
        class_fields = {f.name for f in dataclasses.fields(cls)}
        kwargs = {}
        for key, value in dct.items():
            if key not in class_fields:
                continue
            if key == "revisions" and value is not None:
                value = [RepoRevision.from_dict(subvalue) for subvalue in value]
            kwargs[key] = value
        return cls(**kwargs)
