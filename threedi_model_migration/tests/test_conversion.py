from .factories import FileFactory
from .factories import RepoRevisionFactory
from .factories import RepositoryFactory
from pathlib import Path
from threedi_model_migration.conversion import repository_to_schematisations
from threedi_model_migration.repository import RepoSettings as Sett
from threedi_model_migration.repository import RepoSqlite as Sql

import pytest


def gen_repo(*revision_sqlites, files=None):
    if files is None:
        # Every sqlite is present in its associated changeset
        files = [
            [FileFactory(path=sqlite.sqlite_path) for sqlite in sqlites]
            for sqlites in revision_sqlites
        ]
    n = len(revision_sqlites)
    revisions = [
        RepoRevisionFactory(
            revision_nr=n - i - 1,
            sqlites=sqlites,
            changes=_files,
        )
        for i, (sqlites, _files) in enumerate(zip(revision_sqlites, files))
    ]
    return RepositoryFactory(
        slug="testrepo",
        revisions=revisions,
    )


@pytest.mark.parametrize(
    "repository,expected_names,expected_nrs",
    [
        # One revision, one sqlite, one settings entry
        (
            gen_repo([Sql(Path("db1"), settings=[Sett(1, "a")])]),
            ["testrepo-db1-a"],
            [[0]],
        ),
        # Two revisions with the same sqlite and settings
        (
            gen_repo(
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
            ),
            ["testrepo-db1-a"],
            [[1, 0]],
        ),
        # One revision with two sqlites with the same settings
        (
            gen_repo(
                [
                    Sql(Path("db1"), settings=[Sett(1, "a")]),
                    Sql(Path("db2"), settings=[Sett(1, "a")]),
                ]
            ),
            ["testrepo-db1-a", "testrepo-db2-a"],
            [[0], [0]],
        ),
        # One revision with one sqlites with two settings
        (
            gen_repo(
                [
                    Sql(Path("db1"), settings=[Sett(1, "a")]),
                    Sql(Path("db1"), settings=[Sett(2, "b")]),
                ]
            ),
            ["testrepo-db1-a", "testrepo-db1-b"],
            [[0], [0]],
        ),
        # Two revisions with one sqlites with different settings ("settings renumbered")
        (
            gen_repo(
                [Sql(Path("db1"), settings=[Sett(2, "b")])],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
            ),
            ["testrepo-db1-a", "testrepo-db1-b"],
            [[0], [1]],
        ),
        # Two revisions with the same sqlite and settings, one sqlite added
        (
            gen_repo(
                [
                    Sql(Path("db1"), settings=[Sett(1, "a")]),
                    Sql(Path("db2"), settings=[Sett(1, "a")]),
                ],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
            ),
            ["testrepo-db1-a", "testrepo-db2-a"],
            [[1, 0], [1]],
        ),
        # Two revisions with the same sqlite and settings, one settings entry added
        (
            gen_repo(
                [
                    Sql(Path("db1"), settings=[Sett(1, "a")]),
                    Sql(Path("db1"), settings=[Sett(2, "b")]),
                ],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
            ),
            ["testrepo-db1-a", "testrepo-db1-b"],
            [[1, 0], [1]],
        ),
        # Setting is renamed: it is tracked (and the last revision will set the name)
        (
            gen_repo(
                [Sql(Path("db1"), settings=[Sett(1, "b")])],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
            ),
            ["testrepo-db1-b"],
            [[1, 0]],
        ),
        # Settings entry skips a revision; it counts as a new one
        (
            gen_repo(
                [
                    Sql(Path("db1"), settings=[Sett(1, "a")]),
                    Sql(Path("db1"), settings=[Sett(2, "c")]),
                ],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
                [
                    Sql(Path("db1"), settings=[Sett(1, "a")]),
                    Sql(Path("db1"), settings=[Sett(2, "b")]),
                ],
            ),
            ["testrepo-db1-a", "testrepo-db1-b", "testrepo-db1-c"],
            [[2, 1, 0], [0], [2]],
        ),
        # Renaming an sqlite is allowed
        (
            gen_repo(
                [Sql(Path("db2"), settings=[Sett(1, "a")])],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
            ),
            ["testrepo-db2-a"],
            [[1, 0]],
        ),
        # Renaming an sqlite is allowed, but a settings id must remain constant
        (
            gen_repo(
                [Sql(Path("db2"), settings=[Sett(2, "b")])],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
            ),
            ["testrepo-db1-a", "testrepo-db2-b"],
            [[0], [1]],
        ),
        # Renaming an sqlite is allowed, and a setting can be added at the same time
        (
            gen_repo(
                [
                    Sql(Path("db2"), settings=[Sett(1, "a")]),
                    Sql(Path("db2"), settings=[Sett(2, "b")]),
                ],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
            ),
            ["testrepo-db2-a", "testrepo-db2-b"],
            [[1, 0], [1]],
        ),
        # Renaming an sqlite is allowed, another sqlite may be present
        (
            gen_repo(
                [
                    Sql(Path("db1"), settings=[Sett(1, "a")]),
                    Sql(Path("db3"), settings=[Sett(1, "a")]),
                ],
                [
                    Sql(Path("db1"), settings=[Sett(1, "a")]),
                    Sql(Path("db2"), settings=[Sett(1, "a")]),
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
