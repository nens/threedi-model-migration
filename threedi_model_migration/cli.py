"""Console script for threedi_model_migration."""
from .repository import DEFAULT_REMOTE
from .repository import Repository
from dataclasses import asdict

import click
import csv
import datetime
import json
import logging
import pathlib
import sys


INSPECT_CSV_FIELDNAMES = [
    "revision_nr",
    "revision_hash",
    "last_update",
    "sqlite_path",
    "settings_id",
    "settings_name",
]


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
    "-r",
    "--remote",
    type=str,
    default=DEFAULT_REMOTE,
    help="Remote domain that contains the repositories to download",
)
@click.option(
    "-v",
    "--verbosity",
    type=int,
    default=1,
    help="Logging verbosity (0: error, 1: warning, 2: info, 3: debug)",
)
@click.pass_context
def main(ctx, base_path, name, remote, verbosity):
    """Console script for threedi_model_migration."""
    if not base_path:
        base_path = pathlib.Path.cwd()

    ctx.ensure_object(dict)
    ctx.obj["repository"] = Repository(base_path, name, remote)

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
@click.pass_context
def download(ctx):
    """Clones / pulls a repository"""
    repository = ctx.obj["repository"]
    repository.download()


def default_json_serializer(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    elif isinstance(o, pathlib.Path):
        return str(o)


@main.command()
@click.option(
    "-i",
    "--indent",
    type=int,
    default=None,
    help="The indentation in case of JSON output",
)
@click.option(
    "-d",
    "--dialect",
    type=str,
    default="excel",
    help="The dialect of the csv output",
)
@click.option(
    "-l",
    "--last_update",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Revisions older than this are filtered",
)
@click.option(
    "-t",
    "--target_path",
    type=click.Path(exists=False, writable=True, path_type=pathlib.Path),
    help="Optional path to save output JSON file into",
)
@click.option(
    "-q/-nq",
    "--quiet/--not-quiet",
    type=bool,
    default=False,
)
@click.pass_context
def inspect(ctx, indent, dialect, last_update, target_path, quiet):
    """Inspects revisions, sqlites, and global settings in a repository"""
    # convert last_update and localize
    repository = ctx.obj["repository"]

    if not quiet:
        stdout = click.get_text_stream("stdout")
        writer = csv.DictWriter(
            stdout, fieldnames=INSPECT_CSV_FIELDNAMES, dialect=dialect
        )
        writer.writeheader()

    result = []
    for revision, sqlite, settings in repository.inspect(last_update):
        record = {**asdict(revision), **asdict(sqlite), **asdict(settings)}
        record.pop("repository")
        record.pop("revision")
        record.pop("sqlite")
        if not quiet:
            writer.writerow({x: record[x] for x in INSPECT_CSV_FIELDNAMES})
        result.append(record)

    if target_path is not None:
        with target_path.open("w") as f:
            json.dump(
                {"repository": repository.name, "combinations": result},
                f,
                indent=indent,
                default=default_json_serializer,
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
