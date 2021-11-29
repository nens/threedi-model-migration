"""Main module."""
from .conversion import repository_to_schematisations
from .json_utils import custom_json_object_hook
from .json_utils import custom_json_serializer
from .metadata import load_inpy
from .metadata import load_modeldatabank
from .repository import DEFAULT_REMOTE
from .repository import Repository
from .schematisation import SchemaMeta
from datetime import datetime
from pathlib import Path
from typing import Optional
from typing import TextIO

import csv
import dataclasses
import json
import logging


logger = logging.getLogger(__name__)


INSPECT_CSV_FIELDNAMES = [
    "revision_nr",
    "revision_hash",
    "last_update",
    "sqlite_path",
    "settings_id",
    "settings_name",
]


def download(
    base_path: Path,
    slug: str,
    remote: str = DEFAULT_REMOTE,
    uuid: bool = False,
    metadata_path: Optional[Path] = None,
    lfclear: bool = False,
):
    """Clone or pull a repository.

    Args:
        base_path: A local working directory to clone into.
        slug: The name of the repository.
        remote: The remote URL (https://hg.lizard.net) or path.
        uuid: Whether to use a uuid as remote repository name (instead of 'name')
        metadata_path: The path of a metadata file (models.lizard.net db dump)
    """
    repository = Repository(base_path, slug)
    if uuid:
        if not metadata_path:
            raise ValueError("Please supply metadata_path")
        metadata = load_modeldatabank(metadata_path)
        remote_name = str(metadata[repository.slug].repo_uuid)
    else:
        remote_name = repository.slug

    if remote.endswith("/"):
        remote = remote[:-1]

    repository.download(remote + "/" + remote_name, lfclear)


def delete(base_path: Path, slug: str):
    """Delete a repository.

    Args:
        base_path: A local working directory that contains the repository.
        slug: The name of the repository.
    """
    repository = Repository(base_path, slug)
    repository.delete()


def inspect(
    base_path: Path,
    inspection_path: Path,
    slug: str,
    last_update: Optional[datetime] = None,
    out: Optional[TextIO] = None,
):
    """Inspect a repository and write results to JSON.

    Args:
        base_path: A local working directory that contains the repository.
        inspection_path: A local directory to write the inspection file into.
        slug: The name of the repository.
        last_update: Only consider revisions starting on this date
        stdout: Optionally write progress to this stream.
    """
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

    inspection_path.mkdir(exist_ok=True)
    with (inspection_path / f"{repository.slug}.json").open("w") as f:
        json.dump(
            repository,
            f,
            indent=4,
            default=custom_json_serializer,
        )


def plan(
    inspection_path: Path,
    slug: str,
    metadata_path: Optional[Path] = None,
    inpy_path: Optional[Path] = None,
    quiet: bool = True,
):
    """Create a migration plan and write results to JSON.

    Args:
        inspection_path: A local directory to read the inspection file and write the
            migration file into.
        slug: The name of the repository.
        metadata_path: The path of a metadata file (models.lizard.net db dump)
        inpy_path: The path of an inpy metadata file (inpy db dump)
        quiet: Whether to print a summary.
    """
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
            print(f"{schematisation.concat_name}: {rev_rng}")

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


def download_inspect_plan(
    base_path,
    inspection_path,
    metadata,
    inpy_data,
    lfclear,
    org_lut,
    slug,
    remote,
    uuid,
    last_update,
    inspect_mode,
):
    repository = Repository(base_path, slug)
    _inspection_path = inspection_path / f"{slug}.json"

    # Download & Inspect if necessary
    if inspect_mode == "always" or (
        inspect_mode == "if-necessary" and not _inspection_path.exists()
    ):
        logger.info(f"Downloading {slug}...")
        # COPY FROM download
        if uuid:
            remote_name = str(metadata[repository.slug].repo_uuid)
        else:
            remote_name = repository.slug

        if remote.endswith("/"):
            remote = remote[:-1]

        repository.download(remote + "/" + remote_name, lfclear)

        # COPY FROM inspect
        logger.info(f"Inspecting {slug}...")
        for revision, sqlite, settings in repository.inspect(last_update):
            record = {
                **dataclasses.asdict(revision),
                **dataclasses.asdict(sqlite),
                **dataclasses.asdict(settings),
            }
            record.pop("sqlites")
            record.pop("settings")

        inspection_path.mkdir(exist_ok=True)
        with _inspection_path.open("w") as f:
            json.dump(
                repository,
                f,
                indent=4,
                default=custom_json_serializer,
            )
    elif _inspection_path.exists():
        with _inspection_path.open("r") as f:
            repository = json.load(f, object_hook=custom_json_object_hook)
    else:
        return  # skip

    # COPY FROM plan
    logger.info(f"Planning {slug}...")
    result = repository_to_schematisations(repository, metadata, inpy_data, org_lut)
    with (inspection_path / f"{repository.slug}.plan.json").open("w") as f:
        json.dump(
            result,
            f,
            indent=4,
            default=custom_json_serializer,
        )
    logger.info(f"Done processing {slug}.")


def report(inspection_path: Path):
    """Aggregate all plans into 1 repository and 1 schematisation CSV"""

    REPOSITORY_CSV_FIELDNAMES = [
        "repository_slug",
        "owner",
        "created",
        "last_update",
        "version",
        "schematisation_count",
        "file_count",
        "file_size_mb",
        "n_threedimodels",
        "n_inp_success",
    ]

    SCHEMATISATION_CSV_FIELDNAMES = [
        "repository_slug",
        "sqlite_name",
        "settings_id",
        "settings_name",
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

            metadata: SchemaMeta = plan["repository_meta"]
            record = {
                "repository_slug": plan["repository_slug"],
                "owner": plan["org_name"] or getattr(metadata, "owner", None),
                "created": getattr(metadata, "created", None),
                "last_update": getattr(metadata, "last_update", None),
                "version": getattr(metadata, "meta", {}).get("version"),
                "schematisation_count": plan["count"],
                "file_count": plan["file_count"],
                "file_size_mb": plan["file_size_mb"],
                "n_threedimodels": plan["n_threedimodels"],
                "n_inp_success": plan["n_inp_success"],
            }

            writer1.writerow(record)
            for schematisation in plan["schematisations"]:
                md = schematisation.metadata
                record = {
                    "repository_slug": schematisation.slug,
                    "sqlite_name": schematisation.sqlite_name,
                    "settings_id": schematisation.settings_id,
                    "settings_name": schematisation.settings_name,
                    "owner": plan["org_name"] or getattr(md, "owner", None),
                    "created": getattr(md, "created", None),
                    "last_update": schematisation.revisions[0].last_update,
                    "revision_count": len(schematisation.revisions),
                    "first_rev_nr": schematisation.revisions[-1].revision_nr,
                    "last_rev_nr": schematisation.revisions[0].revision_nr,
                }
                writer2.writerow(record)
