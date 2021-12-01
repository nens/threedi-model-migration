from .factories import FileFactory
from .factories import RasterFactory
from .factories import RepoRevisionFactory
from .factories import RepositoryFactory
from pathlib import Path
from threedi_model_migration.conversion import repository_to_schematisations
from threedi_model_migration.repository import RepoSettings as Sett
from threedi_model_migration.repository import RepoSqlite as Sql

import pytest


def gen_repo(*revision_sqlites, changes=None):
    if changes is None:
        # Every sqlite is present in its associated changeset
        changes = [
            [sqlite.sqlite_path for sqlite in sqlites] for sqlites in revision_sqlites
        ]
    n = len(revision_sqlites)
    revisions = [
        RepoRevisionFactory(
            revision_nr=n - i - 1,
            sqlites=sqlites,
            changes=[FileFactory(path=path) for path in _changes],
        )
        for i, (sqlites, _changes) in enumerate(zip(revision_sqlites, changes))
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
            ["testrepo - db1 - 1 a"],
            [[0]],
        ),
        # Two revisions with the same sqlite and settings
        (
            gen_repo(
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
            ),
            ["testrepo - db1 - 1 a"],
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
            ["testrepo - db1 - 1 a", "testrepo - db2 - 1 a"],
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
            ["testrepo - db1 - 1 a", "testrepo - db1 - 2 b"],
            [[0], [0]],
        ),
        # Two revisions with one sqlites with different settings ("settings renumbered")
        (
            gen_repo(
                [Sql(Path("db1"), settings=[Sett(2, "b")])],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
            ),
            ["testrepo - db1 - 1 a", "testrepo - db1 - 2 b"],
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
            ["testrepo - db1 - 1 a", "testrepo - db2 - 1 a"],
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
            ["testrepo - db1 - 1 a", "testrepo - db1 - 2 b"],
            [[1, 0], [1]],
        ),
        # Setting is renamed: it is tracked (and the last revision will set the name)
        (
            gen_repo(
                [Sql(Path("db1"), settings=[Sett(1, "b")])],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
            ),
            ["testrepo - db1 - 1 b"],
            [[1, 0]],
        ),
        # Settings entry skips a revision
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
            ["testrepo - db1 - 1 a", "testrepo - db1 - 2 c"],
            [[2, 1, 0], [2, 0]],
        ),
        # Renaming an sqlite is not allowed
        (
            gen_repo(
                [Sql(Path("db2"), settings=[Sett(1, "a")])],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
            ),
            ["testrepo - db2 - 1 a"],
            [[1, 0]],
        ),
        # Renaming an sqlite is allowed, but a settings id must remain constant
        (
            gen_repo(
                [Sql(Path("db2"), settings=[Sett(2, "b")])],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
            ),
            ["testrepo - db1 - 1 a", "testrepo - db2 - 2 b"],
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
            ["testrepo - db2 - 1 a", "testrepo - db2 - 2 b"],
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
            ["testrepo - db1 - 1 a", "testrepo - db3 - 1 a"],
            [[1, 0], [1, 0]],
        ),
        # File is missing in changet of 2nd revision
        (
            gen_repo(
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
                changes=[[], [Path("db1")]],
            ),
            ["testrepo - db1 - 1 a"],
            [[0]],
        ),
        # Sqlite is missing in changet of 2nd revision but a raster was changed
        (
            gen_repo(
                [
                    Sql(
                        Path("db1"),
                        settings=[
                            Sett(1, "a", rasters=[RasterFactory(path=Path("r.tiff"))])
                        ],
                    )
                ],
                [
                    Sql(
                        Path("db1"),
                        settings=[
                            Sett(1, "a", rasters=[RasterFactory(path=Path("r.tiff"))])
                        ],
                    )
                ],
                changes=[[Path("r.tiff")], [Path("db1"), Path("r.tiff")]],
            ),
            ["testrepo - db1 - 1 a"],
            [[1, 0]],
        ),
        # Sqlite is missing in changet of 2nd revision and no raster was changed
        (
            gen_repo(
                [
                    Sql(
                        Path("db1"),
                        settings=[
                            Sett(1, "a", rasters=[RasterFactory(path=Path("r.tiff"))])
                        ],
                    )
                ],
                [
                    Sql(
                        Path("db1"),
                        settings=[
                            Sett(1, "a", rasters=[RasterFactory(path=Path("r.tiff"))])
                        ],
                    )
                ],
                changes=[[], [Path("db1"), Path("r.tiff")]],
            ),
            ["testrepo - db1 - 1 a"],
            [[0]],
        ),
    ],
)
def test_repo_to_schema(repository, expected_names, expected_nrs):
    actual = repository_to_schematisations(repository)["schematisations"]

    # sort by schematisation name
    actual = sorted(actual, key=lambda x: x.name)
    assert [x.name for x in actual] == expected_names
    assert [[rev.revision_nr for rev in x.revisions] for x in actual] == expected_nrs
