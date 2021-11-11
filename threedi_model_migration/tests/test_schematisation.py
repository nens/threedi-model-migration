from datetime import datetime
from pathlib import Path
from threedi_model_migration.repository import RepoRevision
from threedi_model_migration.repository import RepoSettings
from threedi_model_migration.repository import Repository
from threedi_model_migration.repository import RepoSqlite
from threedi_model_migration.schematisation import (
    repository_to_schematisations,
)

import pytest


repository = Repository(
    base_path=Path("/tmp"),
    name="testrepo",
    remote="https://non.existing",
)

revision_1 = RepoRevision(
    repository,
    1,
    "abc",
    datetime(2019, 2, 2),
    "My first commit",
    "username",
)

revision_2 = RepoRevision(
    repository,
    2,
    "cba",
    datetime(2021, 8, 10),
    "My second commit",
    "username",
)

revision_3 = RepoRevision(
    repository,
    3,
    "abcd",
    datetime(2021, 11, 11),
    "My third commit",
    "username",
)


@pytest.mark.parametrize(
    "settings,expected_names,expected_nrs",
    [
        # One revision, one sqlite, one settings entry
        (
            [RepoSettings(1, "a", RepoSqlite("db1", revision_1))],
            ["testrepo-db1-a"],
            [[1]],
        ),
        # Two revisions with the same sqlite and settings
        (
            [
                RepoSettings(1, "a", RepoSqlite("db1", revision_2)),
                RepoSettings(1, "a", RepoSqlite("db1", revision_1)),
            ],
            ["testrepo-db1-a"],
            [[2, 1]],
        ),
        # One revision with two sqlites with the same settings
        (
            [
                RepoSettings(1, "a", RepoSqlite("db1", revision_1)),
                RepoSettings(1, "a", RepoSqlite("db2", revision_1)),
            ],
            ["testrepo-db1-a", "testrepo-db2-a"],
            [[1], [1]],
        ),
        # One revision with one sqlites with two settings
        (
            [
                RepoSettings(2, "b", RepoSqlite("db1", revision_1)),
                RepoSettings(1, "a", RepoSqlite("db1", revision_1)),
            ],
            ["testrepo-db1-a", "testrepo-db1-b"],
            [[1], [1]],
        ),
        # Two revisions with two sqlites with the same settings ("sqlite renamed")
        (
            [
                RepoSettings(1, "a", RepoSqlite("db1", revision_1)),
                RepoSettings(1, "a", RepoSqlite("db2", revision_2)),
            ],
            ["testrepo-db1-a", "testrepo-db2-a"],
            [[1], [2]],
        ),
        # Two revisions with one sqlites with different settings ("settings renumbered")
        (
            [
                RepoSettings(1, "a", RepoSqlite("db1", revision_1)),
                RepoSettings(2, "b", RepoSqlite("db1", revision_2)),
            ],
            ["testrepo-db1-a", "testrepo-db1-b"],
            [[1], [2]],
        ),
        # Two revisions with the same sqlite and settings, one sqlite added
        (
            [
                RepoSettings(1, "a", RepoSqlite("db2", revision_2)),
                RepoSettings(1, "a", RepoSqlite("db1", revision_2)),
                RepoSettings(1, "a", RepoSqlite("db1", revision_1)),
            ],
            ["testrepo-db1-a", "testrepo-db2-a"],
            [[2, 1], [2]],
        ),
        # Two revisions with the same sqlite and settings, one settings entry added
        (
            [
                RepoSettings(2, "b", RepoSqlite("db1", revision_2)),
                RepoSettings(1, "a", RepoSqlite("db1", revision_2)),
                RepoSettings(1, "a", RepoSqlite("db1", revision_1)),
            ],
            ["testrepo-db1-a", "testrepo-db1-b"],
            [[2, 1], [2]],
        ),
        # Setting is renamed: it is tracked
        (
            [
                RepoSettings(1, "b", RepoSqlite("db1", revision_2)),
                RepoSettings(1, "a", RepoSqlite("db1", revision_1)),
            ],
            ["testrepo-db1-b"],
            [[2, 1]],
        ),
        # Settings entry skips a revision; it counts as a new one
        (
            [
                RepoSettings(2, "c", RepoSqlite("db1", revision_3)),
                RepoSettings(1, "a", RepoSqlite("db1", revision_3)),
                RepoSettings(1, "a", RepoSqlite("db1", revision_2)),
                RepoSettings(2, "b", RepoSqlite("db1", revision_1)),
                RepoSettings(1, "a", RepoSqlite("db1", revision_1)),
            ],
            ["testrepo-db1-a", "testrepo-db1-b", "testrepo-db1-c"],
            [[3, 2, 1], [1], [3]],
        ),
    ],
)
def test_repo_to_schema(settings, expected_names, expected_nrs):
    actual = repository_to_schematisations(settings)

    # sort by schematisation name
    actual = sorted(actual, key=lambda x: x[0].concat_name)
    assert [x[0].concat_name for x in actual] == expected_names
    assert [[rev.revision_nr for rev in x[1]] for x in actual] == expected_nrs
