from .file import Raster
from .metadata import InpyMeta
from .metadata import SchemaMeta
from .repository import Repository
from .schematisation import SchemaRevision
from .schematisation import Schematisation
from collections import defaultdict
from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional

import logging


logger = logging.getLogger(__name__)


def raster_lookup(
    repository: Repository, revision_nr: int, sqlite_path: Path, raster: Raster
):
    revision, file = repository.get_file(revision_nr, raster.path)
    if file is None:
        # it could be that the raster path was not relative to the sqlite, but
        # relative to the repo.
        other_path = raster.path.relative_to(sqlite_path.parent)
        revision, file = repository.get_file(revision_nr, other_path)
        if file is None:
            logger.warning(f"{raster} not present in {repository} #0-{revision_nr}.")
            return
    return revision, Raster(
        file.path, file.size, file.md5, raster_type=raster.raster_type
    )


def repository_to_schematisations(
    repository: Repository,
    metadata: Optional[Dict[str, SchemaMeta]] = None,
    inpy_data: Optional[Dict[str, InpyMeta]] = None,
    org_lut: Optional[Dict[str, str]] = None,
) -> List[Schematisation]:
    """Apply logic to convert a repository to several schematisations

    Supplied RepoSettings should belong to only 1 repository.
    """
    if metadata:
        _metadata = metadata.get(repository.slug)
    else:
        _metadata = None

    # schemas is a list of schematisations
    combinations = defaultdict(list)

    # partition into unique (sqlite_path, settings_id) combinations
    for revision in sorted(repository.revisions, key=lambda x: -x.revision_nr):
        for sqlite in revision.sqlites or []:
            for settings in sqlite.settings or []:
                key = (sqlite.sqlite_path, settings.settings_id)
                combinations[key].append((revision, sqlite, settings))

    schemas = []
    for _combinations in combinations.values():
        _, last_sqlite, last_settings = _combinations[0]
        schematisation = Schematisation(
            repo_slug=repository.slug,
            sqlite_name=str(last_sqlite.sqlite_path).split(".sqlite")[0],
            settings_id=last_settings.settings_id,
            settings_name=last_settings.settings_name,
            revisions=[],
            metadata=_metadata,
        )

        # append the revision for each
        for (revision, sqlite, settings) in _combinations:
            sqlite_revision_nr, sqlite_file = repository.get_file(
                revision.revision_nr, sqlite.sqlite_path
            )
            if sqlite_file is None:
                raise FileNotFoundError(f"Sqlite {sqlite.sqlite_path} does not exist")
            rasters = [
                raster_lookup(repository, revision.revision_nr, sqlite.sqlite_path, x)
                for x in settings.rasters
            ]
            rasters = [x for x in rasters if x is not None]
            if sqlite_revision_nr != revision.revision_nr and not any(
                x[0] == revision.revision_nr for x in rasters
            ):
                logger.info(
                    f"Skipped rev #{revision.revision_nr} in '{schematisation}'."
                )
                continue

            schematisation.revisions.append(
                SchemaRevision(
                    sqlite_path=sqlite.sqlite_path,
                    settings_name=settings.settings_name,
                    revision_nr=revision.revision_nr,
                    revision_hash=revision.revision_hash,
                    last_update=revision.last_update,
                    commit_msg=revision.commit_msg,
                    commit_user=revision.commit_user,
                    sqlite=sqlite_file,
                    rasters=[x[1] for x in rasters if x is not None],
                )
            )

        if len(schematisation.revisions) == 0:
            raise RuntimeError(f"Schematisation {schematisation.name} has 0 revisions!")
        rev_nrs = [rev.revision_nr for rev in schematisation.revisions]
        if len(rev_nrs) != len(set(rev_nrs)):
            raise RuntimeError(
                f"Schematisation {schematisation.name} has non-unique revisions!"
            )

        schemas.append(schematisation)

    # check schematisation slug uniqueness
    slugs = [s.slug for s in schemas]
    if len(slugs) != len(set(slugs)):
        raise RuntimeError("Non-unique schematisation slugs!")

    # extract unique files
    files_in_schema = set()
    for schematisation in schemas:
        files_in_schema |= schematisation.get_files()

    # list files omitted from schematisations
    files_omitted = {}
    for revision in repository.revisions:
        omitted = set(revision.changes) - files_in_schema
        if len(omitted) > 0:
            files_omitted[str(revision.revision_nr)] = list(omitted)

    # insert data from inpy
    if inpy_data is not None and repository.slug in inpy_data:
        n_threedimodels = inpy_data[repository.slug].n_threedimodels
        n_inp_success = inpy_data[repository.slug].n_inp_success
    elif inpy_data is not None:
        n_threedimodels = n_inp_success = 0
    else:
        n_threedimodels = n_inp_success = None

    # insert org name
    if org_lut is not None and _metadata is not None:
        org_name = org_lut.get(_metadata.owner)
    else:
        org_name = None

    return {
        "count": len(schemas),
        "file_count": len(files_in_schema),
        "file_size_mb": int(sum(x.size for x in files_in_schema) / (1024 ** 2)),
        "files_omitted": files_omitted,
        "repository_slug": repository.slug,
        "repository_meta": _metadata,
        "n_threedimodels": n_threedimodels,
        "n_inp_success": n_inp_success,
        "org_name": org_name,
        "schematisations": schemas,
    }
