"""Console script for threedi_model_migration."""
from .application import download_inspect_plan
from .conversion import repository_to_schematisations
from .json_utils import custom_json_object_hook
from .json_utils import custom_json_serializer
from .metadata import load_metadata
from .repository import DEFAULT_REMOTE
from .repository import Repository

import click
import csv
import dataclasses
import json
import logging
import pathlib
import re
import shutil
import sys


logger = logging.getLogger(__name__)


@click.group()
@click.option(
    "-b",
    "--base_path",
    type=click.Path(exists=True, readable=True, path_type=pathlib.Path),
    help="Local root that contains the repositories",
)
@click.option(
    "-n",
    "--name",
    type=str,
    required=True,
    help="The name of the repository (directory within path)",
)
@click.option(
    "-m",
    "--metadata_path",
    type=click.Path(exists=True, readable=True, path_type=pathlib.Path),
    help="An optional path to a database dump of the modeldatabank",
)
@click.option(
    "-v",
    "--verbosity",
    type=int,
    default=1,
    help="Logging verbosity (0: error, 1: warning, 2: info, 3: debug)",
)
@click.pass_context
def main(ctx, base_path, name, metadata_path, verbosity):
    """Console script for threedi_model_migration."""
    if not base_path:
        base_path = pathlib.Path.cwd()

    ctx.ensure_object(dict)
    ctx.obj["base_path"] = base_path
    ctx.obj["repository"] = Repository(base_path, name)
    ctx.obj["inspection_path"] = base_path / "_inspection"
    ctx.obj["metadata_path"] = metadata_path

    # setup logging
    LOGGING_LUT = [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG]
    logger = logging.getLogger("threedi_model_migration")
    logger.setLevel(LOGGING_LUT[verbosity])
    ch = logging.StreamHandler()
    ch.setLevel(LOGGING_LUT[verbosity])
    formatter = logging.Formatter("%(asctime)s: %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)


@main.command()
@click.option(
    "-r",
    "--remote",
    type=str,
    default=DEFAULT_REMOTE,
    help="Remote domain that contains the repositories to download",
)
@click.option(
    "-u/-nu",
    "--uuid/--not-uuid",
    type=bool,
    default=False,
    help="Whether to use UUIDs instead of repository slugs in the remote",
)
@click.pass_context
def download(ctx, remote, uuid):
    """Clones / pulls a repository"""
    repository = ctx.obj["repository"]
    if uuid:
        if not ctx.obj["metadata_path"]:
            raise ValueError("Please supply metadata_path")
        metadata = load_metadata(ctx.obj["metadata_path"])
        remote_name = str(metadata[repository.slug].repo_uuid)
    else:
        remote_name = repository.slug

    if remote.endswith("/"):
        remote = remote[:-1]

    repository.download(remote + "/" + remote_name)


@main.command()
@click.pass_context
def delete(ctx):
    """Clones / pulls a repository"""
    repository = ctx.obj["repository"]
    shutil.rmtree(repository.path)


@main.command()
@click.option(
    "-i",
    "--indent",
    type=int,
    default=4,
    help="The indentation in case of JSON output",
)
@click.option(
    "-l",
    "--last_update",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Revisions older than this are filtered",
)
@click.option(
    "-q/-nq",
    "--quiet/--not-quiet",
    type=bool,
    default=False,
)
@click.pass_context
def inspect(ctx, indent, last_update, quiet):
    """Inspects revisions, sqlites, and global settings in a repository"""
    repository = ctx.obj["repository"]
    inspection_path = ctx.obj["inspection_path"]

    INSPECT_CSV_FIELDNAMES = [
        "revision_nr",
        "revision_hash",
        "last_update",
        "sqlite_path",
        "settings_id",
        "settings_name",
    ]

    if not quiet:
        stdout = click.get_text_stream("stdout")
        writer = csv.DictWriter(stdout, fieldnames=INSPECT_CSV_FIELDNAMES)
        writer.writeheader()

    for revision, sqlite, settings in repository.inspect(last_update):
        record = {
            **dataclasses.asdict(revision),
            **dataclasses.asdict(sqlite),
            **dataclasses.asdict(settings),
        }
        record.pop("sqlites")
        record.pop("settings")
        if not quiet:
            writer.writerow({x: record[x] for x in INSPECT_CSV_FIELDNAMES})

    inspection_path.mkdir(exist_ok=True)
    with (inspection_path / f"{repository.slug}.json").open("w") as f:
        json.dump(
            repository,
            f,
            indent=indent,
            default=custom_json_serializer,
        )


@main.command()
@click.option(
    "-i",
    "--indent",
    type=int,
    default=4,
    help="The indentation in case of JSON output",
)
@click.option(
    "-q/-nq",
    "--quiet/--not-quiet",
    type=bool,
    default=False,
)
@click.pass_context
def plan(ctx, indent, quiet):
    """Plans schematisation migration for given inspect result"""
    repository_slug = ctx.obj["repository"].slug
    inspection_path = ctx.obj["inspection_path"]
    if ctx.obj["metadata_path"]:
        metadata = load_metadata(ctx.obj["metadata_path"])
    else:
        metadata = None

    with (inspection_path / f"{repository_slug}.json").open("r") as f:
        repository = json.load(f, object_hook=custom_json_object_hook)

    assert repository.slug == repository_slug

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

    with (inspection_path / f"{repository.slug}.plan.json").open("w") as f:
        json.dump(
            result,
            f,
            indent=indent,
            default=custom_json_serializer,
        )


@main.command()
@click.argument(
    "revision_hash",
    type=str,
    required=True,
)
@click.pass_context
def checkout(ctx, revision_hash):
    """Lists revisions in a repository"""
    repository = ctx.obj["repository"]
    repository.checkout(revision_hash)


@main.command()
@click.option(
    "-r",
    "--remote",
    type=str,
    default=DEFAULT_REMOTE,
    help="Remote domain that contains the repositories to download",
)
@click.option(
    "-u/-nu",
    "--uuid/--not-uuid",
    type=bool,
    default=False,
    help="Whether to use UUIDs instead of repository slugs in the remote",
)
@click.option(
    "-i",
    "--indent",
    type=int,
    default=4,
    help="The indentation in case of JSON output",
)
@click.option(
    "-l",
    "--last_update",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Revisions older than this are filtered",
)
@click.option(
    "-c/-nc",
    "--cache/--no-cache",
    type=bool,
    default=True,
    help="Whether to redo the inspect if the inspection already exists",
)
@click.option(
    "-f",
    "--filters",
    type=str,
    help="A regex to match repository slug against",
)
@click.pass_context
def batch(ctx, remote, uuid, indent, last_update, cache, filters):
    """Downloads, inspects, and plans all repositories from the metadata file"""
    base_path = ctx.obj["base_path"]
    inspection_path = ctx.obj["inspection_path"]
    if not ctx.obj["metadata_path"]:
        raise ValueError("Please supply metadata_path")
    metadata = load_metadata(ctx.obj["metadata_path"])

    # sort newest first
    sorted_metadata = sorted(
        metadata.values(), key=lambda x: x.last_update, reverse=True
    )

    for _metadata in sorted_metadata:
        slug = _metadata.slug
        if filters and not re.match(filters, slug):
            continue
        try:
            download_inspect_plan(
                base_path,
                inspection_path,
                metadata,
                _metadata.slug,
                remote,
                uuid,
                indent,
                last_update,
                cache,
            )
        except Exception as e:
            logger.warning(f"Could not process {_metadata.slug}: {e}")
        finally:
            # Always cleanup FROM delete
            repository = Repository(base_path, slug)
            if repository.path.exists():
                shutil.rmtree(repository.path)


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
