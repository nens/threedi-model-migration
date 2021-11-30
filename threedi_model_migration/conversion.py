from .file import Raster
from .metadata import InpyMeta
from .metadata import SchemaMeta
from .repository import RepoSettings
from .repository import Repository
from .repository import RepoSqlite
from .schematisation import SchemaRevision
from .schematisation import Schematisation
from typing import Dict
from typing import List

import logging


logger = logging.getLogger(__name__)


def _unique_id(sqlite: RepoSqlite, settings: RepoSettings):
    return (str(sqlite.sqlite_path), settings.settings_id)


def raster_lookup(repository: Repository, revision_nr: int, raster: Raster):
    file = repository.get_file(revision_nr, raster.path)
    if file is None:
        return
    return Raster(file.path, file.size, file.md5, raster_type=raster.raster_type)


def repository_to_schematisations(
    repository: Repository,
    metadata: Dict[str, SchemaMeta] = None,
    inpy_data: Dict[str, InpyMeta] = None,
    org_lut: Dict[str, str] = None,
) -> List[Schematisation]:
    """Apply logic to convert a repository to several schematisations

    Supplied RepoSettings should belong to only 1 repository.
    """
    if metadata:
        _metadata = metadata.get(repository.slug)
    else:
        _metadata = None

    # schemas is a list of schematisations
    schemas = []

    # keep track only of the unique (sqlite_path,settings_id) combinations of the
    # previously processed (newer)
    previous_rev = {}  # unique_id -> index into schemas
    for revision in sorted(repository.revisions, key=lambda x: -x.revision_nr):
        combinations = [
            (sqlite, settings)
            for sqlite in revision.sqlites
            for settings in (sqlite.settings or [])
        ]

        # match settings-sqlite combinations with previous (newer) revision
        unique_ids = [_unique_id(*x) for x in combinations]
        targets = [previous_rev.pop(x, None) for x in unique_ids]
        unmatched_ids = [x for (x, y) in zip(unique_ids, targets) if y is None]

        # extra logic to fix incomplete matches:
        if len(unmatched_ids) > 0:
            # situation: 1 sqlite is renamed (still multiple settings allowed!)
            n_unmatched_sqlites = len(set([x[0] for x in unmatched_ids]))
            n_unmatched_sqlites_prev = len(set([x[0] for x in previous_rev.keys()]))
            if n_unmatched_sqlites == 1 and n_unmatched_sqlites_prev == 1:
                # rewrite 'previous_rev' keys to account for the rename
                sqlite_name = unmatched_ids[0][0]
                previous_rev = {
                    (sqlite_name, k[1]): v for (k, v) in previous_rev.items()
                }
                # insert the new target ids
                for i, unique_id in enumerate(unique_ids):
                    if targets[i] is None:
                        targets[i] = previous_rev.pop(unique_id, None)

        # create schematisations if necessary
        for i, (sqlite, settings) in enumerate(combinations):
            if targets[i] is None:
                schematisation = Schematisation(
                    repo_slug=repository.slug,
                    sqlite_name=str(sqlite.sqlite_path).split(".sqlite")[0],
                    settings_id=settings.settings_id,
                    settings_name=settings.settings_name,
                    revisions=[],
                    metadata=_metadata,
                )
                schemas.append(schematisation)
                targets[i] = len(schemas) - 1

        # append the revision for each
        for (sqlite, settings), target in zip(combinations, targets):
            _sqlite = repository.get_file(revision.revision_nr, sqlite.sqlite_path)
            if _sqlite is None:
                raise FileNotFoundError(f"Sqlite {sqlite.sqlite_path} does not exist")
            rasters = [
                raster_lookup(repository, revision.revision_nr, x)
                for x in settings.rasters
            ]
            schemas[target].revisions.append(
                SchemaRevision(
                    sqlite_path=sqlite.sqlite_path,
                    settings_name=settings.settings_name,
                    revision_nr=revision.revision_nr,
                    revision_hash=revision.revision_hash,
                    last_update=revision.last_update,
                    commit_msg=revision.commit_msg,
                    commit_user=revision.commit_user,
                    sqlite=_sqlite,
                    rasters=[x for x in rasters if x is not None],
                )
            )

        # update previous_rev
        previous_rev = {uid: target for (uid, target) in zip(unique_ids, targets)}

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

    # check schematisation name uniqueness
    names = [s.name for s in schemas]
    if len(names) != len(set(names)):
        raise RuntimeError("Non-unique schematisation names!")

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
