"""Console script for threedi_model_migration."""
from . import application
from .metadata import load_inpy
from .metadata import load_modeldatabank
from .repository import DEFAULT_REMOTE

import click
import logging
import pathlib
import re
import sys


logger = logging.getLogger(__name__)


@click.group()
@click.option(
    "-b",
    "--base_path",
    type=click.Path(exists=True, readable=True, path_type=pathlib.Path),
    default=pathlib.Path.cwd,
    help="Local root that contains the repositories",
)
@click.option(
    "-m",
    "--metadata_path",
    type=click.Path(exists=True, readable=True, path_type=pathlib.Path),
    help="An optional path to a database dump of the modeldatabank, required when using --uuid",
)
@click.option(
    "-i",
    "--inpy_path",
    type=click.Path(exists=True, readable=True, path_type=pathlib.Path),
    help="An optional path to a database dump of inpy",
)
@click.option(
    "-v",
    "--verbosity",
    type=int,
    default=1,
    help="Logging verbosity (0: error, 1: warning, 2: info, 3: debug)",
)
@click.pass_context
def main(ctx, base_path, metadata_path, inpy_path, verbosity):
    """Console script for threedi_model_migration."""
    ctx.ensure_object(dict)
    ctx.obj["base_path"] = base_path
    ctx.obj["inspection_path"] = base_path / "_inspection"
    ctx.obj["metadata_path"] = metadata_path
    ctx.obj["inpy_path"] = inpy_path

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
@click.argument(
    "slug",
    type=str,
)
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
    "--lfclear/--no-lfclear",
    type=bool,
    default=False,
    help="Whether to clear the largefiles usercache ($HOME/.cache/largefiles) afterwards",
)
@click.pass_context
def download(ctx, slug, remote, uuid, lfclear):
    """Clones / pulls a repository"""
    application.download(
        ctx.obj["base_path"],
        slug,
        remote,
        uuid,
        ctx.obj["metadata_path"],
        lfclear,
    )


@main.command()
@click.argument(
    "slug",
    type=str,
)
@click.pass_context
def delete(ctx, slug):
    """Removes a repository"""
    application.delete(ctx.obj["base_path"], slug)


@main.command()
@click.argument(
    "slug",
    type=str,
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
def inspect(ctx, slug, last_update, quiet):
    """Inspects revisions, sqlites, and global settings in a repository"""
    application.inspect(
        ctx.obj["base_path"],
        ctx.obj["inspection_path"],
        slug,
        last_update,
        click.get_text_stream("stdout") if not quiet else None,
    )


@main.command()
@click.argument(
    "slug",
    type=str,
)
@click.option(
    "-q/-nq",
    "--quiet/--not-quiet",
    type=bool,
    default=False,
)
@click.pass_context
def plan(ctx, slug, quiet):
    """Plans schematisation migration for given inspect result"""
    application.plan(
        ctx.obj["inspection_path"],
        slug,
        ctx.obj["metadata_path"],
        ctx.obj["inpy_path"],
        quiet,
    )


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
    "-l",
    "--last_update",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Revisions older than this are filtered",
)
@click.option(
    "-i",
    "--inspect_mode",
    type=click.Choice(["never", "if-necessary", "always"], case_sensitive=False),
    help="Controls whether the heavy clone & inspect tasks are done",
)
@click.option(
    "-f",
    "--filters",
    type=str,
    help="A regex to match repository slug against",
)
@click.pass_context
def batch(ctx, remote, uuid, last_update, inspect_mode, filters):
    """Downloads, inspects, and plans all repositories from the metadata file"""
    base_path = ctx.obj["base_path"]
    inspection_path = ctx.obj["inspection_path"]
    if not ctx.obj["metadata_path"]:
        raise ValueError("Please supply metadata_path")
    metadata = load_modeldatabank(ctx.obj["metadata_path"])
    if ctx.obj["inpy_path"]:
        inpy_data, org_lut = load_inpy(ctx.obj["inpy_path"])
    else:
        inpy_data = org_lut = None

    # sort newest first
    sorted_metadata = sorted(
        metadata.values(), key=lambda x: x.last_update, reverse=True
    )

    for _metadata in sorted_metadata:
        slug = _metadata.slug
        if filters and not re.match(filters, slug):
            continue
        try:
            application.download_inspect_plan(
                base_path,
                inspection_path,
                metadata,
                inpy_data,
                org_lut,
                slug,
                remote,
                uuid,
                last_update,
                inspect_mode,
            )
        except Exception as e:
            logger.warning(f"Could not process {_metadata.slug}: {e}")
            raise
        finally:
            # Always cleanup
            application.delete(base_path, slug)


@main.command()
@click.pass_context
def report(ctx):
    """Aggregate all plans into a two CSV files"""
    inspection_path = ctx.obj["inspection_path"]
    application.report(inspection_path)


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
