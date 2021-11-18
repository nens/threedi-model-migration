"""Console script for threedi_model_migration."""
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
import sys


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
@click.pass_context
def plan(ctx, indent):
    """Plans schematisation migration for given inspect result"""
    repository_slug = ctx.obj["repository"].slug
    inspection_path = ctx.obj["inspection_path"]

    with (inspection_path / f"{repository_slug}.json").open("r") as f:
        repository = json.load(f, object_hook=custom_json_object_hook)

    assert repository.slug == repository_slug

    for schematisation in repository_to_schematisations(repository):
        revisions = schematisation.revisions
        rev_rng = f"{revisions[-1].revision_nr}-{revisions[0].revision_nr}"
        print(f"{schematisation.concat_name}: {rev_rng}")

    with (inspection_path / f"{repository.slug}.plan.json").open("w") as f:
        json.dump(
            schematisation,
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


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
