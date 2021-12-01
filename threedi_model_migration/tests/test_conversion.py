from datetime import datetime
from pathlib import Path
from threedi_model_migration.conversion import repository_to_schematisations
from threedi_model_migration.file import File
from threedi_model_migration.repository import RepoRevision
from threedi_model_migration.repository import RepoSettings
from threedi_model_migration.repository import Repository
from threedi_model_migration.repository import RepoSqlite

import pytest
import hashlib
import random

def random_md5():
    random_bytes = bytes([random.randrange(0, 256) for _ in range(0, 128)])
    return hashlib.md5(random_bytes).hexdigest()


def random_files(n):
    return [
        File(path=Path(f"db{i}"), md5=random_md5(), size=i * 1024) for i in range(1, n + 1)
    ]

def create_revision(revision_nr, sqlites):
    return RepoRevision(
        revision_nr,
        hashlib.md5(f"hash{revision_nr}".encode()),
        datetime(2019, 2, 2 + revision_nr),
        f"My {revision_nr}nd commit",
        "username",
        sqlites=sqlites,
        changes=random_files(3),
    )


def gen_repo(*revision_sqlites):
    n = len(revision_sqlites)
    revisions = [
        create_revision(n - i - 1, sqlites) for i, sqlites in enumerate(revision_sqlites)
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
            gen_repo([RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")])]),
            ["testrepo-db1-a"],
            [[0]],
        ),
        # Two revisions with the same sqlite and settings
        (
            gen_repo(
                [RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")])],
                [RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")])],
            ),
            ["testrepo-db1-a"],
            [[1, 0]],
        ),
        # One revision with two sqlites with the same settings
        (
            gen_repo(
                [
                    RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")]),
                    RepoSqlite(Path("db2"), settings=[RepoSettings(1, "a")]),
                ],
            ),
            ["testrepo-db1-a", "testrepo-db2-a"],
            [[0], [0]],
        ),
        # One revision with one sqlites with two settings
        (
            gen_repo(
                [
                    RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")]),
                    RepoSqlite(Path("db1"), settings=[RepoSettings(2, "b")]),
                ],
            ),
            ["testrepo-db1-a", "testrepo-db1-b"],
            [[0], [0]],
        ),
        # Two revisions with one sqlites with different settings ("settings renumbered")
        (
            gen_repo(
                [RepoSqlite(Path("db1"), settings=[RepoSettings(2, "b")])],
                [RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")])],
            ),
            ["testrepo-db1-a", "testrepo-db1-b"],
            [[0], [1]],
        ),
        # Two revisions with the same sqlite and settings, one sqlite added
        (
            gen_repo(
                [
                    RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")]),
                    RepoSqlite(Path("db2"), settings=[RepoSettings(1, "a")]),
                ],
                [RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")])],
                
            ),
            ["testrepo-db1-a", "testrepo-db2-a"],
            [[1, 0], [1]],
        ),
        # Two revisions with the same sqlite and settings, one settings entry added
        (
            gen_repo(
                [
                    RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")]),
                    RepoSqlite(Path("db1"), settings=[RepoSettings(2, "b")]),
                ],
                [RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")])],
                
            ),
            ["testrepo-db1-a", "testrepo-db1-b"],
            [[1, 0], [1]],
        ),
        # Setting is renamed: it is tracked (and the last revision will set the name)
        (
            gen_repo(
                [RepoSqlite(Path("db1"), settings=[RepoSettings(1, "b")])],
                [RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")])],
                
            ),
            ["testrepo-db1-b"],
            [[1, 0]],
        ),
        # Settings entry skips a revision; it counts as a new one
        (
            gen_repo(
                [
                    RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")]),
                    RepoSqlite(Path("db1"), settings=[RepoSettings(2, "c")]),
                ],
                [RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")])],
                [
                    RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")]),
                    RepoSqlite(Path("db1"), settings=[RepoSettings(2, "b")]),
                ],
            ),
            ["testrepo-db1-a", "testrepo-db1-b", "testrepo-db1-c"],
            [[2, 1, 0], [0], [2]],
        ),
        # Renaming an sqlite is allowed
        (
            gen_repo(
                [RepoSqlite(Path("db2"), settings=[RepoSettings(1, "a")])],
                [RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")])],
                
            ),
            ["testrepo-db2-a"],
            [[1, 0]],
        ),
        # Renaming an sqlite is allowed, but a settings id must remain constant
        (
            gen_repo(
                [RepoSqlite(Path("db2"), settings=[RepoSettings(2, "b")])],
                [RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")])],
                
            ),
            ["testrepo-db1-a", "testrepo-db2-b"],
            [[0], [1]],
        ),
        # Renaming an sqlite is allowed, and a setting can be added at the same time
        (
            gen_repo(
                [
                    RepoSqlite(Path("db2"), settings=[RepoSettings(1, "a")]),
                    RepoSqlite(Path("db2"), settings=[RepoSettings(2, "b")]),
                ],
                [RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")])],
                
            ),
            ["testrepo-db2-a", "testrepo-db2-b"],
            [[1, 0], [1]],
        ),
        # Renaming an sqlite is allowed, another sqlite may be present
        (
            gen_repo(
                [
                    RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")]),
                    RepoSqlite(Path("db3"), settings=[RepoSettings(1, "a")]),
                ],
                [
                    RepoSqlite(Path("db1"), settings=[RepoSettings(1, "a")]),
                    RepoSqlite(Path("db2"), settings=[RepoSettings(1, "a")]),
                ],
                
            ),
            ["testrepo-db1-a", "testrepo-db3-a"],
            [[1, 0], [1, 0]],
        ),
    ],
)
def test_repo_to_schema(repository, expected_names, expected_nrs):
    actual = repository_to_schematisations(repository)["schematisations"]

    # sort by schematisation name
    actual = sorted(actual, key=lambda x: x.concat_name)
    assert [x.concat_name for x in actual] == expected_names
    assert [[rev.revision_nr for rev in x.revisions] for x in actual] == expected_nrs
