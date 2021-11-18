from pathlib import Path
from threedi_model_migration import hg
from threedi_model_migration.repository import Repository

import pytest
import sqlite3


DATA_PATH = Path(__file__).parent / "data"


@pytest.fixture(scope="session")
def metadata_json_path():
    return DATA_PATH / "metadata.json"


@pytest.fixture(scope="session")
def repository(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("repositories")
    repo_path = tmp_path / "testrepo"
    hg.init(repo_path)

    # write a sqlite
    con = sqlite3.connect(repo_path / "db1.sqlite")
    with con:
        con.execute("CREATE TABLE v2_global_settings (id int, name varchar(255))")
        con.execute("INSERT INTO v2_global_settings VALUES (1, 'default')")
    con.close()
    hg.add(repo_path, "db1.sqlite")
    hg.commit(repo_path, "db1.sqlite", "My first commit")

    # add another sqlite
    con = sqlite3.connect(repo_path / "db2.sqlite")
    with con:
        con.execute("CREATE TABLE v2_global_settings (id int, name varchar(255))")
        con.execute("INSERT INTO v2_global_settings VALUES (1, 'default')")
        con.execute("INSERT INTO v2_global_settings VALUES (2, 'breach')")
    con.close()
    hg.add(repo_path, "db2.sqlite")
    hg.commit(repo_path, "db2.sqlite", "My second commit")
    return Repository(base_path=tmp_path, slug="testrepo")


@pytest.fixture(scope="session")
def repository_inspected(repository):
    inspected = Repository(base_path=repository.base_path, slug=repository.slug)
    list(inspected.inspect())
    return inspected
