"""Main module."""
from .conversion import repository_to_schematisations
from .json_utils import custom_json_object_hook
from .json_utils import custom_json_serializer
from .metadata import load_metadata
from .repository import DEFAULT_REMOTE
from .repository import Repository
from datetime import datetime
from pathlib import Path
from typing import Optional
from typing import TextIO

import csv
import dataclasses
import json


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
        metadata = load_metadata(metadata_path)
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
    quiet: bool = True,
):
    """Create a migration plan and write results to JSON.

    Args:
        inspection_path: A local directory to read the inspection file and write the
            migration file into.
        slug: The name of the repository.
        metadata_path: The path of a metadata file (models.lizard.net db dump)
        quiet: Whether to print a summary.
    """
    metadata = load_metadata(metadata_path) if metadata_path else None

    with (inspection_path / f"{slug}.json").open("r") as f:
        repository = json.load(f, object_hook=custom_json_object_hook)

    assert repository.slug == slug

    result = repository_to_schematisations(repository, metadata)
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
    base_path, inspection_path, metadata, slug, remote, uuid, indent, last_update, cache
):
    repository = Repository(base_path, slug)
    _inspection_path = inspection_path / f"{slug}.json"

    # Download & Inspect if necessary
    if not cache or not _inspection_path.exists():
        # COPY FROM download
        if uuid:
            remote_name = str(metadata[repository.slug].repo_uuid)
        else:
            remote_name = repository.slug

        if remote.endswith("/"):
            remote = remote[:-1]

        repository.download(remote + "/" + remote_name, lfclear=True)

        # COPY FROM inspect
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
                indent=indent,
                default=custom_json_serializer,
            )
    else:
        with _inspection_path.open("r") as f:
            repository = json.load(f, object_hook=custom_json_object_hook)

    # COPY FROM plan
    result = repository_to_schematisations(repository, metadata)
    with (inspection_path / f"{repository.slug}.plan.json").open("w") as f:
        json.dump(
            result,
            f,
            indent=indent,
            default=custom_json_serializer,
        )
