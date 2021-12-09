"""Console script for threedi_model_migration."""
from . import application
from .metadata import load_inpy
from .metadata import load_modeldatabank
from .repository import DEFAULT_REMOTE

import click
import fnmatch
import json
import logging
import pathlib
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
    "-e",
    "--env_file",
    type=click.Path(exists=True, readable=True, path_type=pathlib.Path),
    help="An env file containing API host, user, password",
)
@click.option(
    "-u",
    "--user_mapping_path",
    type=click.Path(exists=True, readable=True, path_type=pathlib.Path),
    help="An optional path to a json mapping Mercurial users to API usernames",
)
@click.option(
    "-s",
    "--sentry_dsn",
    type=str,
    help="An optional DSN for logging warnings and exception to Sentry",
)
@click.option(
    "--lfclear",
    type=bool,
    default=False,
    help="Specify to clear the largefiles usercache after a download",
)
@click.option(
    "-v",
    "--verbosity",
    type=int,
    default=1,
    help="Logging verbosity (0: error, 1: warning, 2: info, 3: debug)",
)
@click.pass_context
def main(
    ctx,
    base_path,
    metadata_path,
    inpy_path,
    user_mapping_path,
    sentry_dsn,
    env_file,
    lfclear,
    verbosity,
):
    """Console script for threedi_model_migration."""
    ctx.ensure_object(dict)
    ctx.obj["base_path"] = base_path
    ctx.obj["metadata_path"] = metadata_path
    ctx.obj["inpy_path"] = inpy_path
    ctx.obj["env_file"] = env_file
    ctx.obj["user_mapping_path"] = user_mapping_path
    ctx.obj["lfclear"] = lfclear
    if sentry_dsn:
        import sentry_sdk
        sentry_sdk.init(sentry_dsn)

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
    "-i/-ni",
    "--ifnewer/--not-ifnewer",
    type=bool,
    default=False,
    help="Whether to skip the download if the inspection file is up to date",
)
@click.pass_context
def download(ctx, slug, remote, uuid, ifnewer):
    """Clones / pulls a repository"""
    if uuid and not ctx.obj["metadata_path"]:
        raise ValueError("Please supply metadata_path")
    if uuid:
        metadata = load_modeldatabank(ctx.obj["metadata_path"])
    else:
        metadata = None
    application.download(
        ctx.obj["base_path"],
        slug,
        remote,
        uuid,
        metadata,
        ctx.obj["lfclear"],
        ifnewer,
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
    "-m",
    "--mode",
    type=click.Choice([x.value for x in application.InspectMode], case_sensitive=False),
    default=application.InspectMode.always.value,
    help="Controls when to inspect",
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
def inspect(ctx, slug, inspect_mode, last_update, quiet):
    """Inspects revisions, sqlites, and global settings in a repository"""
    if not quiet:
        out = click.get_text_stream("stdout", errors="surrogateescape")
    else:
        out = None
    application.inspect(
        ctx.obj["base_path"],
        slug,
        inspect_mode,
        last_update,
        out,
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
        ctx.obj["base_path"],
        slug,
        ctx.obj["metadata_path"],
        ctx.obj["inpy_path"],
        ctx.obj["user_mapping_path"],
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
    type=click.Choice([x.value for x in application.InspectMode], case_sensitive=False),
    default=application.InspectMode.if_necessary.value,
    help="Controls when to inspect",
)
@click.option(
    "-p",
    "--push_mode",
    type=click.Choice([x.value for x in application.PushMode], case_sensitive=False),
    default=application.PushMode.never.value,
    help="Controls when to push",
)
@click.option(
    "-I",
    "--include",
    type=str,
    multiple=True,
    help="Pattern(s) to only process specific repository slugs",
)
@click.option(
    "-X",
    "--exclude",
    type=str,
    multiple=True,
    help="Pattern(s) to exclude specific repository slugs (takes precedence over include)",
)
@click.option(
    "-o",
    "--owner_blacklist_path",
    type=click.Path(exists=True, readable=True, path_type=pathlib.Path),
    help="An optional path to a file listing unique_ids of organisations to ignore",
)
@click.pass_context
def batch(
    ctx,
    remote,
    uuid,
    last_update,
    inspect_mode,
    push_mode,
    include,
    exclude,
    owner_blacklist_path,
):
    """Downloads, inspects, and plans all repositories from the metadata file"""
    base_path = ctx.obj["base_path"]
    lfclear = ctx.obj["lfclear"]
    env_file = ctx.obj["env_file"]
    if not ctx.obj["metadata_path"]:
        raise ValueError("Please supply metadata_path")
    metadata = load_modeldatabank(ctx.obj["metadata_path"], owner_blacklist_path)
    if ctx.obj["inpy_path"]:
        inpy_data, org_lut = load_inpy(ctx.obj["inpy_path"])
    else:
        inpy_data = org_lut = None
    if ctx.obj["user_mapping_path"]:
        with ctx.obj["user_mapping_path"].open("r") as f:
            user_lut = json.load(f)
    else:
        user_lut = None

    # sort newest first
    sorted_metadata = sorted(metadata.values(), key=lambda x: x.created, reverse=True)

    for _metadata in sorted_metadata:
        slug = _metadata.slug
        if include and not any(fnmatch.fnmatch(slug, x) for x in include):
            continue
        if exclude and any(fnmatch.fnmatch(slug, x) for x in exclude):
            continue
        try:
            application.batch(
                base_path,
                metadata,
                inpy_data,
                env_file,
                lfclear,
                org_lut,
                user_lut,
                slug,
                remote,
                uuid,
                last_update,
                inspect_mode,
                push_mode,
            )
        except Exception as e:
            logger.exception(f"Could not process {_metadata.slug}: {e}")
        finally:
            # Always cleanup
            application.delete(base_path, slug)


@main.command()
@click.argument(
    "url",
    type=str,
)
@click.option(
    "-q",
    "--queue",
    type=str,
    default="migration",
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
    "-l",
    "--last_update",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Revisions older than this are filtered",
)
@click.option(
    "-i",
    "--inspect_mode",
    type=click.Choice([x.value for x in application.InspectMode], case_sensitive=False),
    default=application.InspectMode.incremental.value,
    help="Controls when to inspect",
)
@click.option(
    "-p",
    "--push_mode",
    type=click.Choice([x.value for x in application.PushMode], case_sensitive=False),
    default=application.PushMode.incremental.value,
    help="Controls when to push",
)
@click.option(
    "-o",
    "--owner_blacklist_path",
    type=click.Path(exists=True, readable=True, path_type=pathlib.Path),
    help="An optional path to a file listing unique_ids of organisations to ignore",
)
@click.pass_context
def consume(
    ctx,
    url,
    queue,
    remote,
    uuid,
    last_update,
    inspect_mode,
    push_mode,
    owner_blacklist_path,
):
    """Downloads, inspects, and plans all repositories from the metadata file"""
    base_path = ctx.obj["base_path"]
    lfclear = ctx.obj["lfclear"]
    env_file = ctx.obj["env_file"]
    if not ctx.obj["metadata_path"]:
        raise ValueError("Please supply metadata_path")
    metadata = load_modeldatabank(ctx.obj["metadata_path"], owner_blacklist_path)
    if ctx.obj["inpy_path"]:
        inpy_data, org_lut = load_inpy(ctx.obj["inpy_path"])
    else:
        inpy_data = org_lut = None
    if ctx.obj["user_mapping_path"]:
        with ctx.obj["user_mapping_path"].open("r") as f:
            user_lut = json.load(f)
    else:
        user_lut = None

    def wrapped_batch_func(slug):
        if slug not in metadata:
            logger.warning(
                f"Skipping '{slug}': unknown slug or blacklisted organisation'"
            )
            return
        try:
            application.batch(
                base_path,
                metadata,
                inpy_data,
                env_file,
                lfclear,
                org_lut,
                user_lut,
                slug,
                remote,
                uuid,
                last_update,
                inspect_mode,
                push_mode,
            )
        finally:
            # Always cleanup
            application.delete(base_path, slug)

    application.consume_amqp(url, queue, wrapped_batch_func)


@main.command()
@click.pass_context
def report(ctx):
    """Aggregate all plans into a two CSV files"""
    application.report(ctx.obj["base_path"])


@main.command()
@click.argument(
    "slug",
    type=str,
)
@click.option(
    "-m",
    "--mode",
    type=click.Choice([x.value for x in application.PushMode], case_sensitive=False),
    help="Controls which revisions are pushed",
    default=application.PushMode.full.value,
)
@click.option(
    "-l",
    "--last_update",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Revisions older than this are not pushed",
)
@click.pass_context
def push(ctx, slug, mode, last_update):
    """Push a complete repository to the API"""
    application.push(ctx.obj["base_path"], slug, mode, ctx.obj["env_file"], last_update)


@main.command()
@click.argument(
    "symlinks_path",
    type=click.Path(exists=True, readable=True, path_type=pathlib.Path),
    required=True,
)
@click.pass_context
def patch_uuids(ctx, symlinks_path):
    """Patch inspection data that have a UUID as slug."""
    application.patch_uuids(
        ctx.obj["base_path"],
        symlinks_path,
        ctx.obj["metadata_path"],
        ctx.obj["inpy_path"],
    )


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
