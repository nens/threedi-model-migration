from .file import Raster
from .repository import RepoSettings
from .repository import Repository
from .repository import RepoSqlite
from .schematisation import SchemaRevision
from .schematisation import Schematisation
from typing import List


def _unique_id(sqlite: RepoSqlite, settings: RepoSettings):
    return (str(sqlite.sqlite_path), settings.settings_id)


def raster_lookup(repository: Repository, revision_nr: int, raster: Raster):
    file = repository.get_file(revision_nr, raster.path)
    return Raster(file.path, file.size, file.md5, raster_type=raster.raster_type)


def repository_to_schematisations(repository: Repository) -> List[Schematisation]:
    """Apply logic to convert a repository to several schematisations

    Supplied RepoSettings should belong to only 1 repository.
    """
    # the result is a list of schematisations
    result = []

    # keep track only of the unique (sqlite_path,settings_id) combinations of the
    # previously processed (newer)
    previous_rev = {}  # unique_id -> index into result
    for revision in sorted(repository.revisions, key=lambda x: -x.revision_nr):
        combinations = [
            (sqlite, settings)
            for sqlite in revision.sqlites
            for settings in sqlite.settings
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
                    slug=repository.slug,
                    sqlite_name=str(sqlite.sqlite_path),
                    settings_id=settings.settings_id,
                    settings_name=settings.settings_name,
                    revisions=[],
                )
                result.append(schematisation)
                targets[i] = len(result) - 1

        # append the revision for each
        for (sqlite, settings), target in zip(combinations, targets):
            result[target].revisions.append(
                SchemaRevision(
                    sqlite_path=sqlite.sqlite_path,
                    settings_name=settings.settings_name,
                    revision_nr=revision.revision_nr,
                    revision_hash=revision.revision_hash,
                    last_update=revision.last_update,
                    commit_msg=revision.commit_msg,
                    commit_user=revision.commit_user,
                    sqlite=repository.get_file(
                        revision.revision_nr, sqlite.sqlite_path
                    ),
                    rasters=[
                        raster_lookup(repository, revision.revision_nr, x)
                        for x in settings.rasters
                    ],
                )
            )

        # update previous_rev
        previous_rev = {uid: target for (uid, target) in zip(unique_ids, targets)}

    return result
