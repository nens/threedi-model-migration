from .file import Raster
from .file import RasterOptions
from .schematisation import SchemaRevision
from .schematisation import Schematisation
from .text_utils import make_utf8
from .zip_utils import deterministic_zip
from enum import Enum
from pathlib import Path
from tempfile import SpooledTemporaryFile
from threedi_api_client.files import upload_file
from threedi_api_client.files import upload_fileobj
from threedi_api_client.openapi import Commit as OACommit
from threedi_api_client.openapi import CreateRevision as OACreateRevision
from threedi_api_client.openapi import RasterCreate as OARaster
from threedi_api_client.openapi import Schematisation as OASchematisation
from threedi_api_client.openapi import SchematisationRevision as OARevision
from threedi_api_client.openapi import SqliteFileUpload as OASqlite
from threedi_api_client.openapi import Upload as OAUpload
from threedi_api_client.openapi import V3BetaApi
from threedi_api_client.openapi.exceptions import ApiException
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import hashlib
import json
import logging
import time
import urllib3


UPLOAD_TIMEOUT = urllib3.Timeout(connect=60, read=600)

logger = logging.getLogger(__name__)


class PushMode(Enum):
    full = "full"  # push all revisions; error if API has a different commit with same number
    overwrite = "overwrite"  # always delete the API schematisation (for testing mostly)
    incremental = (
        "incremental"  # push newer revisions, increase revision number if necessary
    )
    never = "never"


class NoSchematisation(Exception):
    pass


def get_or_create_schematisation(
    api: V3BetaApi,
    schematisation: Schematisation,
    mode: PushMode = PushMode.full,
) -> OASchematisation:
    if mode is PushMode.never:
        raise ValueError("Invalid push mode 'never'")
    resp = api.schematisations_list(
        slug=schematisation.slug, owner__unique_id=schematisation.metadata.owner
    )
    if resp.count == 1 and mode in {PushMode.full, PushMode.incremental}:
        logger.info(
            f"Schematisation '{schematisation.slug}' already exists, skipping creation."
        )
        return resp.results[0], False
    elif resp.count == 1 and mode is PushMode.overwrite:
        logger.info(
            f"Schematisation '{make_utf8(schematisation.name)}' already exists, deleting..."
        )
        delete_schematisation(api, resp.results[0].id)
    elif resp.count == 0 and mode is PushMode.incremental:
        raise NoSchematisation(
            f"Cannot incrementally update '{schematisation.slug}' as it does not exist."
        )

    logger.info(f"Creating schematisation '{schematisation.slug}'...")
    obj = OASchematisation(
        owner=schematisation.metadata.owner,
        name=make_utf8(schematisation.name),
        slug=schematisation.slug,
        tags=["models.lizard.net"],
        meta={
            "repository": schematisation.repo_slug,
            "sqlite_path": make_utf8(str(schematisation.sqlite_path)),
            "settings_id": schematisation.settings_id,
            "settings_name": make_utf8(schematisation.settings_name),
            **schematisation.metadata.meta,
        },
        created_by=schematisation.metadata.created_by,
        created=schematisation.metadata.created,
    )
    for _ in range(2):
        try:
            resp = api.schematisations_create(obj)
        except ApiException as e:
            if e.status == 400:
                errors = json.loads(e.body)
                if len(errors.get("created_by", [])) == 1:
                    logger.info(errors["created_by"])
                    obj.created_by = None
                    continue  # try again
            raise e
        break
    return resp, True


def delete_schematisation(api: V3BetaApi, schema_id: int):
    # first delete the revisions
    while True:
        resp = api.schematisations_revisions_list(schema_id)
        if len(resp.results) == 0:
            break

        for oa_revision in resp.results:
            api.schematisations_revisions_delete(
                oa_revision.id, schema_id, {"number": oa_revision.number}
            )

    # then the schematisation
    api.schematisations_delete(schema_id)


def _match_revision(
    oa_revision: OARevision, revisions: List[SchemaRevision]
) -> SchemaRevision:
    """Match an external (OpenAPI) revision with an internal (SchemaRevision)

    The revision with the commit date is returned.

    Revisions should be sorted new to old ('last_update' descending)
    """
    for revision in revisions:
        if oa_revision.commit_date > revision.last_update:
            break
        if oa_revision.commit_date == revision.last_update:
            return revision


def get_latest_revision(
    api: V3BetaApi, schema_id: int, revisions=List[SchemaRevision]
) -> Tuple[Optional[SchemaRevision], Optional[int]]:
    """Retrieve the (internal) revision object that matches the latest one in the API.

    This is tricky to get right as users may have committed via the API (so that
    revision numbers do not match). This function uses the commit_date to compare.
    """
    logger.info("Getting the latest revision...")

    offset = 0
    latest_revision = None
    latest_revision_nr = None
    while True:
        resp = api.schematisations_revisions_list(
            schema_id, committed=True, limit=10, offset=offset
        )

        for oa_revision in resp.results:
            if latest_revision_nr is None:
                latest_revision_nr = oa_revision.number
            latest_revision = _match_revision(oa_revision, revisions)
            if latest_revision is not None:
                break

        if len(resp.results) == 0 or latest_revision is not None:
            break

        offset += 10

    if latest_revision is not None:
        logger.info(f"The latest revision number is {latest_revision.revision_nr}.")
    else:
        logger.info("No matching revision present.")
    return latest_revision, latest_revision_nr


def get_or_create_revision(
    api: V3BetaApi,
    schema_id: int,
    revision: SchemaRevision,
    set_revision_nr: bool,
) -> OARevision:
    resp = api.schematisations_revisions_list(
        schema_id, commit_date=revision.last_update
    )
    if resp.count == 1:
        logger.info(f"Revision {revision.revision_nr} is already present, skipping.")
        return resp.results[0], False

    logger.info(f"Creating revision {revision.revision_nr}...")
    obj = OACreateRevision(
        empty=True, number=revision.revision_nr if set_revision_nr else None
    )
    resp = api.schematisations_revisions_create(schema_id, obj)
    return resp, True


def upload_sqlite(api: V3BetaApi, rev_id: int, schema_id: int, sqlite_path: Path):
    logger.info(f"Creating {make_utf8(str(sqlite_path.name))}...")

    # Sqlite files are zipped; the md5 sum is that of the zipped file (so: recompute)
    with SpooledTemporaryFile(mode="w+b") as f:
        deterministic_zip(f, [make_utf8(str(sqlite_path))])
        f.seek(0)
        md5 = hashlib.md5(f.read())
        f.seek(0)
        obj = OASqlite(
            filename=make_utf8(sqlite_path.stem) + ".zip",
            md5sum=md5.hexdigest(),
        )
        upload = api.schematisations_revisions_sqlite_upload(rev_id, schema_id, obj)
        if upload.put_url is None:
            logger.info(
                f"Sqlite '{make_utf8(str(sqlite_path.name))}' already existed, skipping upload."
            )
        else:
            logger.info(f"Uploading '{make_utf8(str(sqlite_path.name))}'...")
            upload_fileobj(upload.put_url, f, timeout=UPLOAD_TIMEOUT, md5=md5.digest())


def upload_raster(
    api: V3BetaApi, rev_id: int, schema_id: int, repo_path: Path, raster: Raster
):
    raster_type = RasterOptions(raster.raster_type).value
    logger.info(f"Creating '{raster_type}' raster...")
    obj = OARaster(
        name=make_utf8(raster.path.name)[:60], md5sum=raster.md5, type=raster_type
    )
    resp = api.schematisations_revisions_rasters_create(rev_id, schema_id, obj)
    if resp.file and resp.file.state == "uploaded":
        logger.info(f"Raster '{str(raster.path)}' already existed, skipping upload.")
        return

    logger.info(f"Uploading '{str(raster.path)}'...")
    obj = OAUpload(
        filename=make_utf8(raster.path.name),
    )
    upload = api.schematisations_revisions_rasters_upload(
        resp.id, rev_id, schema_id, obj
    )

    upload_file(
        upload.put_url,
        repo_path / raster.path,
        timeout=UPLOAD_TIMEOUT,
        md5=bytes.fromhex(raster.md5),
    )


def commit_revision(
    api: V3BetaApi,
    rev_id: int,
    schema_id: int,
    revision: SchemaRevision,
    user_lut: Optional[Dict[str, str]] = None,
):
    # First wait for all files to have turned to 'uploaded'
    for wait_time in [0.5, 1.0, 2.0, 10.0, 30.0, 60.0, 120.0, 300.0]:
        oa_revision = api.schematisations_revisions_read(rev_id, schema_id)
        states = [oa_revision.sqlite.file.state]
        states.extend([raster.file.state for raster in oa_revision.rasters])

        if all(state == "uploaded" for state in states):
            break
        elif any(state == "created" for state in states):
            logger.info(
                f"Sleeping {wait_time} seconds to wait for the files to become 'uploaded'..."
            )
            time.sleep(wait_time)
            continue

        # We have non expected states (probably an error in the upload processor)
        file = oa_revision.sqlite.file
        if file.state not in ("uploaded", "created"):
            logger.exception(
                f"File (sqlite) with pk={file.id} has unexpected state "
                f"'{file.state}'. Skipping commit."
            )
            # Attempt cleanup (delete revision twice)
            for _ in range(2):
                try:
                    api.schematisations_revisions_delete(
                        rev_id, schema_id, {"number": oa_revision.number}
                    )
                except Exception:
                    logger.exception(f"Error deleting revision {rev_id}")

        for raster in oa_revision.rasters:
            file = raster.file
            if file.state in ("uploaded", "created"):
                continue
            logger.exception(
                f"File (raster) with pk={file.id} has unexpected state "
                f"'{file.state}'. Omitting raster."
            )
            api.schematisations_revisions_rasters_delete(raster.id, rev_id, schema_id)

        break
    else:
        raise RuntimeError("Some files are still in 'created' state")

    # In the API, the 'user' is just a string and 'commit_user' is an FK to user
    user = revision.commit_user
    obj = OACommit(
        commit_message=make_utf8(revision.commit_msg)
        if revision.commit_msg != ""
        else None,
        commit_date=revision.last_update,
        user=user,
    )
    # Only attempt to set 'commit_user' if the user is in the user look-up table
    if user_lut and user in user_lut:
        obj.commit_user = user_lut[user]

    for _ in range(2):
        try:
            api.schematisations_revisions_commit(rev_id, schema_id, obj)
        except ApiException as e:
            if e.status == 400:
                errors = json.loads(e.body)
                if len(errors.get("commit_user", [])) == 1:
                    logger.warning(errors["commit_user"])
                    obj.commit_user = None
                    continue  # try again if user is not found
            raise e
        break

    logger.info(f"Committed revision {revision.revision_nr}.")


def check_revision(api: V3BetaApi, rev_id: int, schema_id: int):
    api.schematisations_revisions_check(rev_id, schema_id, {})
