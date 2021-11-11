"""Console script for threedi_model_migration."""
from .repository import DEFAULT_REMOTE
from .repository import RepoRevision
from .repository import RepoSettings
from .repository import Repository
from .repository import RepoSqlite
from .schematisation import repository_to_schematisations
from dataclasses import asdict
from datetime import datetime

import click
import csv
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
    ctx.obj["inspection_path"] = base_path / "_inspection"

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
    if isinstance(o, datetime):
        return o.isoformat()
    elif isinstance(o, pathlib.Path):
        return str(o)


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

    if not quiet:
        stdout = click.get_text_stream("stdout")
        writer = csv.DictWriter(stdout, fieldnames=INSPECT_CSV_FIELDNAMES)
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

    inspection_path.mkdir(exist_ok=True)
    with (inspection_path / f"{repository.name}.json").open("w") as f:
        json.dump(
            {"repository": repository.name, "combinations": result},
            f,
            indent=indent,
            default=default_json_serializer,
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
    repository = ctx.obj["repository"]
    inspection_path = ctx.obj["inspection_path"]

    with (inspection_path / f"{repository.name}.json").open("r") as f:
        inspection = json.load(f)

    assert inspection["repository"] == repository.name

    settings = [
        RepoSettings(
            settings_id=x["settings_id"],
            settings_name=x["settings_name"],
            sqlite=RepoSqlite(
                sqlite_path=pathlib.Path(x["sqlite_path"]),
                revision=RepoRevision(
                    repository=repository,
                    revision_nr=x["revision_nr"],
                    revision_hash=x["revision_hash"],
                    last_update=datetime.fromisoformat(x["last_update"]),
                    commit_msg=x["commit_msg"],
                    commit_user=x["commit_user"],
                ),
            ),
        )
        for x in inspection["combinations"]
    ]

    result = []
    for schematisation, revisions in repository_to_schematisations(settings):
        rev_rng = f"{revisions[-1].revision_nr}-{revisions[0].revision_nr}"
        print(f"{schematisation.concat_name}: {rev_rng}")
        record = asdict(schematisation)
        record["revisions"] = [asdict(revision) for revision in revisions]
        for revision in record["revisions"]:
            del revision["schematisation"]
        result.append(record)

    with (inspection_path / f"{repository.name}.plan.json").open("w") as f:
        json.dump(
            {"repository": repository.name, "schematisations": result},
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
