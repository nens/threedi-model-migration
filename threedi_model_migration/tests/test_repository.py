from pathlib import Path
from threedi_model_migration.file import RasterOptions


def test_revisions(repository_inspected):
    revisions = repository_inspected.revisions
    assert len(revisions) == 2

    assert revisions[0].commit_msg == "My second commit"
    assert revisions[0].revision_nr == 2
    assert revisions[1].commit_msg == "My first commit"
    assert revisions[1].revision_nr == 1


def test_checkout_newest(repository):
    repository.checkout(2)
    assert (repository.path / "db1.sqlite").exists()
    assert (repository.path / "db2.sqlite").exists()


def test_checkout_oldest(repository):
    repository.checkout(1)
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
    assert settings[0].rasters[0].raster_type == RasterOptions.dem_raw_file.value
    assert settings[0].rasters[0].path == Path("rasters/dem.tif")
    assert settings[1].settings_id == 2
    assert settings[1].settings_name == "groundwater"
    assert len(settings[1].rasters) == 2
    assert settings[1].rasters[0].raster_type == RasterOptions.dem_raw_file.value
    assert settings[1].rasters[0].path == Path("rasters/dem.tif")
    assert (
        settings[1].rasters[1].raster_type
        == RasterOptions.groundwater_impervious_layer_level_file.value
    )
    assert settings[1].rasters[1].path == Path("rasters/x.tif")


def test_inspect(repository_inspected):
    result = list(repository_inspected.inspect())

    # newest to oldest
    revision, sqlite, settings = result[0]
    assert revision.revision_nr == 2
    assert sqlite.sqlite_path.name == "db1.sqlite"
    assert settings.settings_id == 1
    revision, sqlite, settings = result[1]
    assert revision.revision_nr == 2
    assert sqlite.sqlite_path.name == "db2.sqlite"
    assert settings.settings_id == 1
    revision, sqlite, settings = result[2]
    assert revision.revision_nr == 2
    assert sqlite.sqlite_path.name == "db2.sqlite"
    assert settings.settings_id == 2
    revision, sqlite, settings = result[3]
    assert revision.revision_nr == 1
    assert sqlite.sqlite_path.name == "db1.sqlite"
    assert settings.settings_id == 1
