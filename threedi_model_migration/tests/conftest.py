from threedi_model_migration import hg
from threedi_model_migration.repository import Repository

import pytest


@pytest.fixture(scope="session")
def repository(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("repositories")
    hg.init(tmp_path / "testrepo")
    with open(tmp_path / "testrepo" / "file.txt", "w") as f:
        f.write("foo")
    hg.add(tmp_path / "testrepo", "file.txt")
    hg.commit(tmp_path / "testrepo", "file.txt", "My first commit")
    with open(tmp_path / "testrepo" / "file.txt", "w") as f:
        f.write("bar")
    hg.commit(tmp_path / "testrepo", "file.txt", "My second commit")
    return Repository(
        base_path=tmp_path, name="testrepo", remote="https://non.existing"
    )
