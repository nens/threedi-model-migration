from .repository import RepoSettings
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator
from typing import List
from typing import Tuple


@dataclass
class Schematisation:
    slug: str  # matches repository slug
    sqlite_name: str  # the newest of its revisions
    settings_id: int
    settings_name: str  # the newest of its revisions

    @property
    def concat_name(self):
        return f"{self.slug}-{self.sqlite_name}-{self.settings_name}"

    def __repr__(self):
        return f"Schematisation(slug={self.slug}, sqlite_name={self.sqlite_name}, settings_name={self.settings_name})"


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


def _settings_unique_id(settings: RepoSettings):
    return (str(settings.sqlite.sqlite_path), settings.settings_id)


def repository_to_schematisations(
    settings_iter=Iterator[RepoSettings],
) -> List[Tuple[Schematisation, List[SchemaRevision]]]:
    """Apply logic to convert a repository to several schematisations

    Supplied RepoSettings should belong to only 1 repository.
    """
    # the result is a list of schematisations
    result = []

    # group per revision_nr and sort
    repo_slug = None
    per_revision = defaultdict(list)
    for settings in settings_iter:
        if repo_slug is None:
            repo_slug = settings.sqlite.revision.repository.slug
        else:
            assert repo_slug == settings.sqlite.revision.repository.slug
        per_revision[settings.sqlite.revision.revision_nr].append(settings)

    # special case: empty list provided
    if repo_slug is None:
        return result

    # iterate over all revisions
    revision_nrs = sorted(per_revision.keys(), reverse=True)

    # keep track only of the unique (sqlite_path,settings_id) combinations of the
    # previously processed (newer)
    previous_rev = {}  # unique_id -> index into result
    for n in revision_nrs:
        # match settings-sqlite combinations with previous (newer) revision
        unique_ids = [_settings_unique_id(x) for x in per_revision[n]]
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
        for i, settings in enumerate(per_revision[n]):
            if targets[i] is None:
                schematisation = Schematisation(
                    slug=repo_slug,
                    sqlite_name=str(settings.sqlite.sqlite_path),
                    settings_id=settings.settings_id,
                    settings_name=settings.settings_name,
                )
                result.append((schematisation, []))
                targets[i] = len(result) - 1

        # append the revision for each
        for settings, target in zip(per_revision[n], targets):
            result[target][1].append(
                SchemaRevision(
                    schematisation=result[target][0],
                    sqlite_path=settings.sqlite.sqlite_path,
                    settings_name=settings.settings_name,
                    revision_nr=settings.sqlite.revision.revision_nr,
                    revision_hash=settings.sqlite.revision.revision_hash,
                    last_update=settings.sqlite.revision.last_update,
                    commit_msg=settings.sqlite.revision.commit_msg,
                    commit_user=settings.sqlite.revision.commit_user,
                )
            )

        # update previous_rev
        previous_rev = {uid: target for (uid, target) in zip(unique_ids, targets)}

    return result
