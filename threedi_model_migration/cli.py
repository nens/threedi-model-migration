"""Console script for threedi_model_migration."""
from .repository import DEFAULT_REMOTE
from .repository import Repository
from .repository import RepositoryRevision
from dataclasses import asdict
from dataclasses import fields

import click
import csv
import datetime
import json
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
    "-r",
    "--remote",
    type=str,
    default=DEFAULT_REMOTE,
    help="Remote domain that contains the repositories to download",
)
@click.pass_context
def main(ctx, base_path, name, remote):
    """Console script for threedi_model_migration."""
    if not base_path:
        base_path = pathlib.Path.cwd()

    ctx.ensure_object(dict)
    ctx.obj["repository"] = Repository(base_path, name, remote)


@main.command()
@click.pass_context
def download(ctx):
    """Clones / pulls a repository"""
    repository = ctx.obj["repository"]
    repository.download()


def default_json_serializer(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()


@main.command()
@click.option(
    "-f",
    "--format",
    type=click.Choice(["json", "csv"], case_sensitive=False),
    default="csv",
    required=True,
    help="What format to output",
)
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
    help="The dialect in case of csv output",
)
@click.pass_context
def ls(ctx, format, indent, dialect):
    """Lists revisions in a repository"""
    repository = ctx.obj["repository"]
    result_dct = [asdict(revision) for revision in repository.revisions]
    stdout_text = click.get_text_stream("stdout")
    if format == "json":
        json.dump(
            result_dct, stdout_text, indent=indent, default=default_json_serializer
        )
    elif format == "csv":
        fieldnames = [x.name for x in fields(RepositoryRevision)]
        writer = csv.DictWriter(stdout_text, fieldnames=fieldnames, dialect=dialect)
        writer.writeheader()
        for revision in result_dct:
            writer.writerow(revision)


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
