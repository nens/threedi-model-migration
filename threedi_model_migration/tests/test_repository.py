from copy import deepcopy
from pathlib import Path
from threedi_model_migration.file import RasterOptions

import logging
import pytest


def test_revisions(repository_inspected):
    revisions = repository_inspected.revisions
    assert len(revisions) == 2

    assert revisions[0].commit_msg == "My second commit"
    assert revisions[0].revision_nr == 1
    assert len(revisions[0].changes) == 1
    assert revisions[0].changes[0].path.name == "db2.sqlite"
    assert revisions[1].commit_msg == "My first commit"
    assert revisions[1].revision_nr == 0
    assert len(revisions[1].changes) == 1
    assert revisions[1].changes[0].path.name == "db1.sqlite"


def test_checkout_newest(repository):
    repository.checkout(1)
    assert (repository.path / "db1.sqlite").exists()
    assert (repository.path / "db2.sqlite").exists()


def test_checkout_oldest(repository):
    repository.checkout(0)
    assert (repository.path / "db1.sqlite").exists()
    assert not (repository.path / "db2.sqlite").exists()


def test_sqlites(repository_inspected):
    sqlites = repository_inspected.revisions[0].sqlites
    assert len(sqlites) == 2

    assert str(sqlites[0].sqlite_path) == "db1.sqlite"
    assert str(sqlites[1].sqlite_path) == "db2.sqlite"


def test_settings(repository_inspected):
    settings = repository_inspected.revisions[0].sqlites[1].settings
    assert len(settings) == 2

    assert settings[0].settings_id == 1
    assert settings[0].settings_name == "default"
    assert len(settings[0].rasters) == 1
    assert settings[0].rasters[0].raster_type == RasterOptions.dem_file.value
    assert settings[0].rasters[0].path == Path("rasters/dem.tif")
    assert settings[1].settings_id == 2
    assert settings[1].settings_name == "groundwater"
    assert len(settings[1].rasters) == 2
    assert settings[1].rasters[0].raster_type == RasterOptions.dem_file.value
    assert settings[1].rasters[0].path == Path("rasters/dem.tif")
    assert (
        settings[1].rasters[1].raster_type
        == RasterOptions.groundwater_impervious_layer_level_file.value
    )
    assert settings[1].rasters[1].path == Path("rasters/x.tif")


def test_inspect(repository_inspected):
    result = list(repository_inspected.inspect())

    assert len(result) == 4

    # newest to oldest
    revision, sqlite, settings = result[0]
    assert revision.revision_nr == 1
    assert sqlite.sqlite_path.name == "db1.sqlite"
    assert settings.settings_id == 1
    revision, sqlite, settings = result[1]
    assert revision.revision_nr == 1
    assert sqlite.sqlite_path.name == "db2.sqlite"
    assert settings.settings_id == 1
    revision, sqlite, settings = result[2]
    assert revision.revision_nr == 1
    assert sqlite.sqlite_path.name == "db2.sqlite"
    assert settings.settings_id == 2
    revision, sqlite, settings = result[3]
    assert revision.revision_nr == 0
    assert sqlite.sqlite_path.name == "db1.sqlite"
    assert settings.settings_id == 1


def test_incremental_inspect(repository_inspected):
    repository = deepcopy(repository_inspected)

    # drop the last revision
    repository.revisions = repository.revisions[1:]
    revision_before = repository.revisions[0]

    # redo the inspection
    result = list(repository.inspect())

    assert len(result) == 4

    # newest to oldest
    revision, sqlite, settings = result[0]
    assert revision.revision_nr == 1
    assert sqlite.sqlite_path.name == "db1.sqlite"
    assert settings.settings_id == 1
    revision, sqlite, settings = result[1]
    assert revision.revision_nr == 1
    assert sqlite.sqlite_path.name == "db2.sqlite"
    assert settings.settings_id == 1
    revision, sqlite, settings = result[2]
    assert revision.revision_nr == 1
    assert sqlite.sqlite_path.name == "db2.sqlite"
    assert settings.settings_id == 2
    revision, sqlite, settings = result[3]
    assert revision is revision_before


@pytest.mark.parametrize(
    "revision_nr,path",
    [
        (2, "db1.sqlite"),
        (2, "db2.sqlite"),
        (1, "db1.sqlite"),
    ],
)
def test_get_file(revision_nr, path, repository_inspected):
    _, file = repository_inspected.get_file(revision_nr, Path(path))
    assert file.path.name == path


@pytest.mark.parametrize(
    "revision_nr,path",
    [
        (0, "db2.sqlite"),
        (1, "db3.sqlite"),
    ],
)
def test_get_file_not_found(revision_nr, path, repository_inspected, caplog):
    caplog.set_level(logging.WARNING)
    assert repository_inspected.get_file(revision_nr, Path(path))[1] is None
    assert len(caplog.record_tuples) == 1
