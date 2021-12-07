"""Main module."""
from . import api_utils
from .conversion import repository_to_schematisations
from .json_utils import custom_json_object_hook
from .json_utils import custom_json_serializer
from .metadata import load_inpy
from .metadata import load_modeldatabank
from .metadata import load_symlinks
from .repository import DEFAULT_REMOTE
from .repository import Repository
from .schematisation import SchemaMeta
from .schematisation import Schematisation
from datetime import datetime
from enum import Enum
from pathlib import Path
from threedi_api_client import ThreediApi
from threedi_api_client.openapi import V3BetaApi
from typing import Dict
from typing import List
from typing import Optional
from typing import TextIO
from typing import Union
from uuid import UUID

import csv
import dataclasses
import json
import logging
import shutil


logger = logging.getLogger(__name__)

INSPECTION_RELPATH = "_inspection"

INSPECT_CSV_FIELDNAMES = [
    "revision_nr",
    "revision_hash",
    "last_update",
    "sqlite_path",
    "settings_id",
    "settings_name",
]


class InspectMode(Enum):
    always = "always"  # discard the inspection file
    incremental = "incremental"  # retrieve the HEAD and inspect the new commits
    if_necessary = "if-necessary"  # if the inspect file is missing
    never = "never"  # assume inspection file is there


class PushMode(Enum):
    full = "full"  # push all revisions; error if API has a different commit with same number
    overwrite = "overwrite"  # always delete the API schematisation (for testing mostly)
    incremental = "incremental"  # push newer revisions, increase revision number if necessary
    never = "never"


class RepositoryNotFound(FileNotFoundError):
    pass


def download(
    base_path: Path,
    slug: str,
    remote: str = DEFAULT_REMOTE,
    uuid: bool = False,
    metadata: Optional[Dict] = None,
    lfclear: bool = False,
    ifnewer: bool = False,
):
    """Clone or pull a repository.

    Args:
        base_path: A local working directory to clone into.
        slug: The name of the repository.
        remote: The remote URL (https://hg.lizard.net) or path.
        uuid: Whether to use a uuid as remote repository name (instead of 'name')
        metadata: Metadata (models.lizard.net dump)
        ifnewer: First check the remote tip and only clone if it is newer
    """
    if remote.endswith("/"):
        remote = remote[:-1]

    if uuid:
        if metadata is None:
            raise ValueError("Please supply metadata_path")
        remote_name = str(metadata[slug].repo_uuid)
    else:
        remote_name = slug

    if ifnewer:
        with (base_path / INSPECTION_RELPATH / f"{slug}.json").open("r") as f:
            repository = json.load(f, object_hook=custom_json_object_hook)
        repository.base_path = base_path
    else:
        repository = Repository(base_path, slug)

    return repository.download(remote + "/" + remote_name, ifnewer, lfclear)


def delete(base_path: Path, slug: str):
    """Delete a repository.

    Args:
        base_path: A local working directory that contains the repository.
        slug: The name of the repository.
    """
    repository = Repository(base_path, slug)
    repository.delete()


def _needs_local_repo(base_path, slug, inspect_mode):
    inspect_mode = InspectMode(inspect_mode)
    inspection_file_path = base_path / INSPECTION_RELPATH / f"{slug}.json"

    if inspect_mode in (InspectMode.always, InspectMode.incremental):
        return True
    if inspect_mode is InspectMode.if_necessary:
        return not inspection_file_path.exists()
    elif inspect_mode is InspectMode.never:
        return False


def inspect(
    base_path: Path,
    slug: str,
    inspect_mode: Union[InspectMode, str],
    last_update: Optional[datetime] = None,
    out: Optional[TextIO] = None,
):
    """Inspect a repository and write results to JSON.

    Args:
        base_path: A local working directory that contains the repository.
        slug: The name of the repository.
        inspect_mode: Whether to inspect
        last_update: Only consider revisions starting on this date
        stdout: Optionally write progress to this stream.
    """
    if not _needs_local_repo(base_path, slug, inspect_mode):
        return

    # Check if repository is present
    repository = Repository(base_path, slug)
    if not repository.path.exists():
        raise FileNotFoundError(f"Repository {slug} not present")

    inspect_mode = InspectMode(inspect_mode)
    inspection_file_path = base_path / INSPECTION_RELPATH / f"{slug}.json"

    if inspect_mode is InspectMode.incremental:
        with inspection_file_path.open("r") as f:
            repository = json.load(f, object_hook=custom_json_object_hook)
        repository.base_path = base_path
    else:
        repository = Repository(base_path, slug)

    if out is not None:
        writer = csv.DictWriter(out, fieldnames=INSPECT_CSV_FIELDNAMES)
        writer.writeheader()

    for revision, sqlite, settings in repository.inspect(last_update):
        record = {
            **dataclasses.asdict(revision),
            **dataclasses.asdict(sqlite),
            **dataclasses.asdict(settings),
        }
        record.pop("sqlites")
        record.pop("settings")
        if out is not None:
            writer.writerow({x: record[x] for x in INSPECT_CSV_FIELDNAMES})

    (base_path / INSPECTION_RELPATH).mkdir(exist_ok=True)
    with (base_path / INSPECTION_RELPATH / f"{repository.slug}.json").open("w") as f:
        json.dump(
            repository,
            f,
            indent=4,
            default=custom_json_serializer,
        )


def plan(
    base_path: Path,
    slug: str,
    metadata_path: Optional[Path] = None,
    inpy_path: Optional[Path] = None,
    quiet: bool = True,
):
    """Create a migration plan and write results to JSON.

    Args:
        base_path: A local working directory
        slug: The name of the repository.
        metadata_path: The path of a metadata file (models.lizard.net db dump)
        inpy_path: The path of an inpy metadata file (inpy db dump)
        quiet: Whether to print a summary.
    """
    inspection_path = base_path / INSPECTION_RELPATH
    metadata = load_modeldatabank(metadata_path) if metadata_path else None
    inpy_data, org_lut = load_inpy(inpy_path) if inpy_path else (None, None)

    with (inspection_path / f"{slug}.json").open("r") as f:
        repository = json.load(f, object_hook=custom_json_object_hook)

    assert repository.slug == slug

    result = repository_to_schematisations(repository, metadata, inpy_data, org_lut)
    if not quiet:
        print(f"Schematisation count: {result['count']}")

        for schematisation in result["schematisations"]:
            revisions = schematisation.revisions
            rev_rng = f"{revisions[-1].revision_nr}-{revisions[0].revision_nr}"
            print(f"{schematisation.name}: {rev_rng}")

        print(
            f"File count: {result['file_count']}, Estimated size: {result['file_size_mb']} MB"
        )

    with (inspection_path / f"{slug}.plan.json").open("w") as f:
        json.dump(
            result,
            f,
            indent=4,
            default=custom_json_serializer,
        )


def batch(
    base_path,
    metadata,
    inpy_data,
    env_file,
    lfclear,
    org_lut,
    slug,
    remote,
    uuid,
    last_update,
    inspect_mode,
    push_mode,
):
    repository = Repository(base_path, slug)
    inspect_mode = InspectMode(inspect_mode or "always")
    inspection_path = base_path / INSPECTION_RELPATH

    # Check if we need to download / pull & Inspect if necessary
    if _needs_local_repo(base_path, slug, inspect_mode):
        logger.info(f"Downloading {slug}...")
        needs_inspection = download(
            base_path,
            slug,
            remote,
            uuid,
            metadata,
            lfclear,
            ifnewer=inspect_mode is InspectMode.incremental,
        )
        if needs_inspection:
            logger.info(f"Inspecting {slug}...")
            inspect(base_path, slug, inspect_mode, last_update)

    inspection_file_path = inspection_path / f"{slug}.json"
    if inspection_file_path.exists():
        with inspection_file_path.open("r") as f:
            repository = json.load(f, object_hook=custom_json_object_hook)
    else:
        return  # skip

    # copy of application.plan()
    logger.info(f"Planning {slug}...")
    result = repository_to_schematisations(repository, metadata, inpy_data, org_lut)
    with (inspection_path / f"{repository.slug}.plan.json").open("w") as f:
        json.dump(
            result,
            f,
            indent=4,
            default=custom_json_serializer,
        )

    for _ in range(2):
        try:
            push(
                base_path,
                slug,
                push_mode,
                env_file=env_file,
                last_update=last_update,
            )
        except FileNotFoundError:
            # Try again, after downloading the repo
            download(
                base_path,
                slug,
                remote,
                uuid,
                metadata,
                lfclear,
            )
        else:
            break

    logger.info(f"Done processing {slug}.")


def report(base_path: Path):
    """Aggregate all plans into 1 repository and 1 schematisation CSV"""
    inspection_path = base_path / INSPECTION_RELPATH

    REPOSITORY_CSV_FIELDNAMES = [
        "repository_slug",
        "owner",
        "created",
        "last_update",
        "schematisation_count",
        "file_count",
        "file_size_mb",
        "n_threedimodels",
        "n_inp_success",
    ]

    SCHEMATISATION_CSV_FIELDNAMES = [
        "name",
        "owner",
        "created",
        "last_update",
        "revision_count",
        "first_rev_nr",
        "last_rev_nr",
    ]

    with Path("repositories.csv").open("w", errors="surrogateescape") as f1, Path(
        "schematisations.csv"
    ).open("w", errors="surrogateescape") as f2:
        writer1 = csv.DictWriter(f1, fieldnames=REPOSITORY_CSV_FIELDNAMES)
        writer1.writeheader()
        writer2 = csv.DictWriter(f2, fieldnames=SCHEMATISATION_CSV_FIELDNAMES)
        writer2.writeheader()

        for path in inspection_path.glob("*.plan.json"):
            with path.open("r") as f:
                plan = json.load(f, object_hook=custom_json_object_hook)

            schemas: List[Schematisation] = plan["schematisations"]
            if len(schemas) > 0:
                last_update = max(s.revisions[0].last_update for s in schemas)
            else:
                last_update = None

            metadata: SchemaMeta = plan["repository_meta"]
            record = {
                "repository_slug": plan["repository_slug"],
                "owner": plan["org_name"] or getattr(metadata, "owner", None),
                "created": getattr(metadata, "created", None),
                "last_update": last_update,
                "schematisation_count": plan["count"],
                "file_count": plan["file_count"],
                "file_size_mb": plan["file_size_mb"],
                "n_threedimodels": plan["n_threedimodels"],
                "n_inp_success": plan["n_inp_success"],
            }

            writer1.writerow(record)
            for schematisation in schemas:
                md = schematisation.metadata
                record = {
                    "name": schematisation.name,
                    "owner": plan["org_name"] or getattr(md, "owner", None),
                    "created": getattr(md, "created", None),
                    "last_update": schematisation.revisions[0].last_update,
                    "revision_count": len(schematisation.revisions),
                    "first_rev_nr": schematisation.revisions[-1].revision_nr,
                    "last_rev_nr": schematisation.revisions[0].revision_nr,
                }
                writer2.writerow(record)


def push(
    base_path: Path,
    slug: str,
    mode: Union[PushMode, str],
    env_file: Optional[Path] = None,
    last_update: Optional[datetime] = None,
):
    """Aggregate all plans into 1 repository and 1 schematisation CSV"""
    mode = PushMode(mode)
    if mode is PushMode.never:
        return

    repository = Repository(base_path, slug)
    repository_exists = repository.path.exists()
    plan_path = base_path / INSPECTION_RELPATH / f"{slug}.plan.json"
    with plan_path.open("r") as f:
        plan = json.load(f, object_hook=custom_json_object_hook)

    schematisations: List[Schematisation] = plan["schematisations"]
    with ThreediApi(env_file=env_file, version="v3-beta", asynchronous=False) as api:
        api: V3BetaApi
        for schematisation in schematisations:
            revisions = schematisation.revisions
            if last_update is not None:
                revisions = [
                    x
                    for x in revisions
                    if x.last_update.replace(tzinfo=None) >= last_update
                ]
                if len(revisions) == 0:
                    continue

            oa_schema, created = api_utils.get_or_create_schematisation(
                api, schematisation, overwrite=(mode == PushMode.overwrite)
            )
            logger.info(f"Got schematisation with pk='{oa_schema.id}'.")

            set_revision_nr = True
            if mode == PushMode.incremental and not created:
                latest, nr = api_utils.get_latest_revision(api, oa_schema.id, revisions)
                if latest is not None:
                    revisions = [
                        x for x in revisions if x.revision_nr > latest.revision_nr
                    ]
                if len(revisions) == 0:
                    continue

                # Don't try to set the (API) revision number if it already has a newer
                # one. In that case; leave it to the API. Revision numbers will
                # not match.
                if nr is not None and revisions[0].revision_nr <= nr:
                    set_revision_nr = False

            for revision in sorted(revisions, key=lambda x: x.revision_nr):
                oa_rev, created = api_utils.get_or_create_revision(
                    api, oa_schema.id, revision, set_revision_nr=set_revision_nr
                )
                if not created:
                    continue
                if not repository_exists:
                    raise FileNotFoundError(
                        f"{repository} does not exist locally, please download it first."
                    )
                repository.checkout(revision.revision_hash)
                api_utils.upload_sqlite(
                    api, oa_rev.id, oa_schema.id, repository.path, revision.sqlite
                )
                for raster in revision.rasters:
                    api_utils.upload_raster(
                        api, oa_rev.id, oa_schema.id, repository.path, raster
                    )
                api_utils.commit_revision(api, oa_rev.id, oa_schema.id, revision)


def patch_uuids(
    base_path: Path,
    symlinks_path: Path,
    metadata_path: Optional[Path],
    inpy_path: Optional[Path],
):
    """Patch inspection data (repositories and plans) that have a UUID as slug."""
    inspection_path = base_path / INSPECTION_RELPATH
    symlinks = load_symlinks(symlinks_path)
    metadata = load_modeldatabank(metadata_path) if metadata_path else None
    inpy_data, org_lut = load_inpy(inpy_path) if inpy_path else (None, None)

    for path in inspection_path.glob("*???????-????-????-????????????*.json"):
        with path.open("r") as f:
            obj = json.load(f, object_hook=custom_json_object_hook)

        if isinstance(obj, Repository):
            uuid = obj.slug
        else:
            uuid = obj["repository_slug"]

        try:
            uuid = UUID(uuid)
        except ValueError:
            continue  # no uuid: skip

        try:
            slug = symlinks[uuid]
        except KeyError:
            logger.warning(f"Unknown repository: {uuid}")
            continue

        if isinstance(obj, Repository):
            obj.slug = slug
        else:
            if metadata is not None:
                _metadata = metadata.get(slug)
            else:
                _metadata = None

            # Insert data from Inpy
            if inpy_data is not None and slug in inpy_data:
                n_threedimodels = inpy_data[slug].n_threedimodels
                n_inp_success = inpy_data[slug].n_inp_success
            elif inpy_data is not None:
                n_threedimodels = n_inp_success = 0
            else:
                n_threedimodels = n_inp_success = None

            # insert org name
            if org_lut is not None and _metadata is not None:
                org_name = org_lut.get(_metadata.owner)
            else:
                org_name = None

            # patch the plan
            obj["repository_slug"] = slug
            obj["repository_meta"] = _metadata
            obj["org_name"] = org_name
            obj["n_threedimodels"] = n_threedimodels
            obj["n_inp_success"] = n_inp_success
            for schematisation in obj["schematisations"]:
                schematisation.slug = slug
                schematisation.metadata = _metadata

        shutil.copyfile(path, str(path) + ".bak")
        with path.open("w") as f:
            json.dump(
                obj,
                f,
                indent=4,
                default=custom_json_serializer,
            )
        logger.info(f"Patched repository {slug} (formerly: {uuid})")
