from pathlib import Path
from threedi_model_migration import hg
from threedi_model_migration.repository import Repository

import pytest
import sqlite3


DATA_PATH = Path(__file__).parent / "data"

GLOBAL_SETTINGS_SCHEMA = """
v2_global_settings (
    id int,
    name varchar(255),
    dem_file varchar(255),
    frict_coef_file varchar(255),
    interception_file varchar(255),
    initial_waterlevel_file varchar(255),
    initial_groundwater_level_file varchar(255),
    interflow_settings_id int,
    simple_infiltration_settings_id int,
    groundwater_settings_id int
)
"""

INTERFLOW_SCHEMA = """
v2_interflow (
    id int,
    porosity_file varchar(255),
    hydraulic_conductivity_file varchar(255)
)
"""

SIMPLE_INFILTRATION_SCHEMA = """
v2_simple_infiltration (
    id int,
    infiltration_rate_file varchar(255),
    max_infiltration_capacity_file varchar(255)
)
"""

GROUNDWATER_SCHEMA = """
v2_groundwater (
    id int,
    groundwater_impervious_layer_level_file varchar(255),
    phreatic_storage_capacity_file varchar(255),
    equilibrium_infiltration_rate_file varchar(255),
    initial_infiltration_rate_file varchar(255),
    infiltration_decay_period_file varchar(255),
    groundwater_hydro_connectivity_file varchar(255),
    leakage_file varchar(255)
)
"""


@pytest.fixture(scope="session")
def metadata_json_path():
    return DATA_PATH / "metadata.json"


@pytest.fixture(scope="session")
def owner_blacklist_path():
    return DATA_PATH / "owner_blacklist.txt"


@pytest.fixture(scope="session")
def repository(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("repositories")
    repo_path = tmp_path / "testrepo"
    hg.init(repo_path)

    # write a sqlite
    con = sqlite3.connect(repo_path / "db1.sqlite")
    with con:
        con.execute(f"CREATE TABLE {GLOBAL_SETTINGS_SCHEMA}")
        con.execute(f"CREATE TABLE {INTERFLOW_SCHEMA}")
        con.execute(f"CREATE TABLE {SIMPLE_INFILTRATION_SCHEMA}")
        # This asserts that inspect() passes without a groundwater table:
        # con.execute(f"CREATE TABLE {GROUNDWATER_SCHEMA}")
        con.execute(
            "INSERT INTO v2_global_settings (id, name, dem_file) VALUES (1, 'default', 'rasters/dem.tif')"
        )
    con.close()
    hg.add(repo_path, "db1.sqlite")
    hg.commit(repo_path, "db1.sqlite", "My first commit")

    # add another sqlite
    con = sqlite3.connect(repo_path / "db2.sqlite")
    with con:
        con.execute(f"CREATE TABLE {GLOBAL_SETTINGS_SCHEMA}")
        con.execute(f"CREATE TABLE {INTERFLOW_SCHEMA}")
        con.execute(f"CREATE TABLE {SIMPLE_INFILTRATION_SCHEMA}")
        con.execute(f"CREATE TABLE {GROUNDWATER_SCHEMA}")
        con.execute(
            "INSERT INTO v2_global_settings (id, name, dem_file) VALUES (1, 'default', 'rasters/dem.tif')"
        )
        con.execute(
            "INSERT INTO v2_global_settings (id, name, dem_file, groundwater_settings_id) VALUES (2, 'groundwater', 'rasters/dem.tif', 1)"
        )
        con.execute(
            "INSERT INTO v2_groundwater (id, groundwater_impervious_layer_level_file) VALUES (1, 'rasters/x.tif')"
        )
    con.close()
    hg.add(repo_path, "db2.sqlite")
    hg.commit(repo_path, "db2.sqlite", "My second commit")
    return Repository(base_path=tmp_path, slug="testrepo")


@pytest.fixture(scope="session")
def repository_inspected(repository):
    inspected = Repository(base_path=repository.base_path, slug=repository.slug)
    list(inspected.inspect())
    return inspected
