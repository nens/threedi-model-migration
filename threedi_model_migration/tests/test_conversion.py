from datetime import datetime
from pathlib import Path
from threedi_model_migration.conversion import repository_to_schematisations
from threedi_model_migration.file import File
from threedi_model_migration.repository import RepoRevision
from threedi_model_migration.repository import RepoSettings
from threedi_model_migration.repository import Repository
from threedi_model_migration.repository import RepoSqlite

import pytest


def gen_repo(*revision_sqlites):
    files = [File(path=f"db{i}", md5=f"abc{i}", size=i * 1024) for i in range(1, 4)]
    revisions = [
        RepoRevision(
            i + 1,
            f"hash{i}",
            datetime(2019, 2, 2 + i),
            f"My {i}nd commit",
            "username",
            sqlites=sqlites,
            changes=files if i == 0 else [],
        )
        for i, sqlites in enumerate(revision_sqlites)
    ]
    return Repository(
        base_path=Path("/tmp"),
        slug="testrepo",
        revisions=revisions,
    )


@pytest.mark.parametrize(
    "repository,expected_names,expected_nrs",
    [
        # One revision, one sqlite, one settings entry
        (
            gen_repo([RepoSqlite("db1", settings=[RepoSettings(1, "a")])]),
            ["testrepo-db1-a"],
            [[1]],
        ),
        # Two revisions with the same sqlite and settings
        (
            gen_repo(
                [RepoSqlite("db1", settings=[RepoSettings(1, "a")])],
                [RepoSqlite("db1", settings=[RepoSettings(1, "a")])],
            ),
            ["testrepo-db1-a"],
            [[2, 1]],
        ),
        # One revision with two sqlites with the same settings
        (
            gen_repo(
                [
                    RepoSqlite("db1", settings=[RepoSettings(1, "a")]),
                    RepoSqlite("db2", settings=[RepoSettings(1, "a")]),
                ],
            ),
            ["testrepo-db1-a", "testrepo-db2-a"],
            [[1], [1]],
        ),
        # One revision with one sqlites with two settings
        (
            gen_repo(
                [
                    RepoSqlite("db1", settings=[RepoSettings(1, "a")]),
                    RepoSqlite("db1", settings=[RepoSettings(2, "b")]),
                ],
            ),
            ["testrepo-db1-a", "testrepo-db1-b"],
            [[1], [1]],
        ),
        # Two revisions with one sqlites with different settings ("settings renumbered")
        (
            gen_repo(
                [RepoSqlite("db1", settings=[RepoSettings(1, "a")])],
                [RepoSqlite("db1", settings=[RepoSettings(2, "b")])],
            ),
            ["testrepo-db1-a", "testrepo-db1-b"],
            [[1], [2]],
        ),
        # Two revisions with the same sqlite and settings, one sqlite added
        (
            gen_repo(
                [RepoSqlite("db1", settings=[RepoSettings(1, "a")])],
                [
                    RepoSqlite("db1", settings=[RepoSettings(1, "a")]),
                    RepoSqlite("db2", settings=[RepoSettings(1, "a")]),
                ],
            ),
            ["testrepo-db1-a", "testrepo-db2-a"],
            [[2, 1], [2]],
        ),
        # Two revisions with the same sqlite and settings, one settings entry added
        (
            gen_repo(
                [RepoSqlite("db1", settings=[RepoSettings(1, "a")])],
                [
                    RepoSqlite("db1", settings=[RepoSettings(1, "a")]),
                    RepoSqlite("db1", settings=[RepoSettings(2, "b")]),
                ],
            ),
            ["testrepo-db1-a", "testrepo-db1-b"],
            [[2, 1], [2]],
        ),
        # Setting is renamed: it is tracked (and the last revision will set the name)
        (
            gen_repo(
                [RepoSqlite("db1", settings=[RepoSettings(1, "a")])],
                [RepoSqlite("db1", settings=[RepoSettings(1, "b")])],
            ),
            ["testrepo-db1-b"],
            [[2, 1]],
        ),
        # Settings entry skips a revision; it counts as a new one
        (
            gen_repo(
                [
                    RepoSqlite("db1", settings=[RepoSettings(1, "a")]),
                    RepoSqlite("db1", settings=[RepoSettings(2, "b")]),
                ],
                [RepoSqlite("db1", settings=[RepoSettings(1, "a")])],
                [
                    RepoSqlite("db1", settings=[RepoSettings(1, "a")]),
                    RepoSqlite("db1", settings=[RepoSettings(2, "c")]),
                ],
            ),
            ["testrepo-db1-a", "testrepo-db1-b", "testrepo-db1-c"],
            [[3, 2, 1], [1], [3]],
        ),
        # Renaming an sqlite is allowed
        (
            gen_repo(
                [RepoSqlite("db1", settings=[RepoSettings(1, "a")])],
                [RepoSqlite("db2", settings=[RepoSettings(1, "a")])],
            ),
            ["testrepo-db2-a"],
            [[2, 1]],
        ),
        # Renaming an sqlite is allowed, but a settings id must remain constant
        (
            gen_repo(
                [RepoSqlite("db1", settings=[RepoSettings(1, "a")])],
                [RepoSqlite("db2", settings=[RepoSettings(2, "b")])],
            ),
            ["testrepo-db1-a", "testrepo-db2-b"],
            [[1], [2]],
        ),
        # Renaming an sqlite is allowed, and a setting can be added at the same time
        (
            gen_repo(
                [RepoSqlite("db1", settings=[RepoSettings(1, "a")])],
                [
                    RepoSqlite("db2", settings=[RepoSettings(1, "a")]),
                    RepoSqlite("db2", settings=[RepoSettings(2, "b")]),
                ],
            ),
            ["testrepo-db2-a", "testrepo-db2-b"],
            [[2, 1], [2]],
        ),
        # Renaming an sqlite is allowed, another sqlite may be present
        (
            gen_repo(
                [
                    RepoSqlite("db1", settings=[RepoSettings(1, "a")]),
                    RepoSqlite("db2", settings=[RepoSettings(1, "a")]),
                ],
                [
                    RepoSqlite("db1", settings=[RepoSettings(1, "a")]),
                    RepoSqlite("db3", settings=[RepoSettings(1, "a")]),
                ],
            ),
            ["testrepo-db1-a", "testrepo-db3-a"],
            [[2, 1], [2, 1]],
        ),
    ],
)
def test_repo_to_schema(repository, expected_names, expected_nrs):
    actual = repository_to_schematisations(repository)["schematisations"]

    # sort by schematisation name
    actual = sorted(actual, key=lambda x: x.concat_name)
    assert [x.concat_name for x in actual] == expected_names
    assert [[rev.revision_nr for rev in x.revisions] for x in actual] == expected_nrs
