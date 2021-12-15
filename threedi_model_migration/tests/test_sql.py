from threedi_model_migration.sql import filter_global_settings
from threedi_model_migration.sql import select
from threedi_model_migration.sql import SETTINGS_SQL

import shutil


def test_filter_global_settings(repository, tmp_path):
    repository.checkout(1)

    shutil.copyfile(repository.path / "db2.sqlite", tmp_path / "db2.sqlite")
    assert len(select(tmp_path / "db2.sqlite", SETTINGS_SQL)) == 2
    filter_global_settings(tmp_path / "db2.sqlite", 1)
    assert select(tmp_path / "db2.sqlite", SETTINGS_SQL) == [(1, "default")]
