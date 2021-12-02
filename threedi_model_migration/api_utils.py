from .file import File
from .file import Raster
from .file import RasterOptions
from .schematisation import SchemaRevision
from .schematisation import Schematisation
from pathlib import Path
from tempfile import SpooledTemporaryFile
from threedi_api_client.files import upload_file
from threedi_api_client.files import upload_fileobj
from threedi_api_client.openapi import Commit as OACommit
from threedi_api_client.openapi import RasterCreate as OARaster
from threedi_api_client.openapi import Schematisation as OASchematisation
from threedi_api_client.openapi import SchematisationRevision as OARevision
from threedi_api_client.openapi import SqliteFileUpload as OASqlite
from threedi_api_client.openapi import Upload as OAUpload
from threedi_api_client.openapi import V3BetaApi
from threedi_api_client.openapi.exceptions import ApiException

import json
import logging
import zipfile


logger = logging.getLogger(__name__)


def get_or_create_schematisation(api: V3BetaApi, schematisation: Schematisation) -> int:
    resp = api.schematisations_list(slug=schematisation.slug)
    if resp.count == 1:
        return resp.results[0].id, False

    obj = OASchematisation(
        owner=schematisation.metadata.owner,
        name=schematisation.name,
        slug=schematisation.slug,
        tags=[],
        meta=schematisation.metadata.meta,
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
                    logger.warning(errors["created_by"])
                    obj.created_by = None
                    continue  # try again
            raise e
        break
    return resp.id, True


def get_or_create_revision(
    api: V3BetaApi, schema_id: int, revision: SchemaRevision
) -> int:
    resp = api.schematisations_revisions_list(
        schema_id, number=revision.revision_nr, committed=True
    )
    if resp.count == 1:
        return resp.results[0].id, False

    obj = OARevision(
        number=revision.revision_nr,
    )
    resp = api.schematisations_revisions_create(schema_id, obj)
    return resp.id, True


def upload_sqlite(
    api: V3BetaApi, rev_id: int, schema_id: int, repo_path: Path, sqlite: File
):
    obj = OASqlite(
        filename=sqlite.path.stem + ".zip",
        md5sum=sqlite.md5,
    )
    upload = api.schematisations_revisions_sqlite_upload(rev_id, schema_id, obj)

    # mode = write/read bytes
    with SpooledTemporaryFile(mode="w+b") as f:
        with zipfile.ZipFile(f, "x") as zip_file:
            zip_file.write((repo_path / sqlite.path).as_posix(), sqlite.path.name)
            zip_file.close()
        f.seek(0)
        upload_fileobj(upload.put_url, f)


def upload_raster(
    api: V3BetaApi, rev_id: int, schema_id: int, repo_path: Path, raster: Raster
):
    raster_type = RasterOptions(raster.raster_type)
    if raster_type is RasterOptions.dem_file:
        raster_type = "dem_raw_file"
    else:
        raster_type = raster_type.value
    obj = OARaster(
        name=raster.path.name,
        md5sum=raster.md5,
        type=raster_type,
    )
    resp = api.schematisations_revisions_rasters_create(rev_id, schema_id, obj)
    if resp.file.state == "uploaded":
        return

    obj = OAUpload(
        filename=raster.path.name,
    )
    upload = api.schematisations_revisions_rasters_upload(
        resp.id, rev_id, schema_id, obj
    )

    upload_file(upload.put_url, raster.path)


def commit_revision(
    api: V3BetaApi, rev_id: int, schema_id: int, revision: SchemaRevision
):
    obj = OACommit(
        commit_message=revision.commit_msg,
        commit_date=revision.last_update,
        user=revision.commit_user,
    )
    return api.schematisations_revisions_commit(rev_id, schema_id, obj)
