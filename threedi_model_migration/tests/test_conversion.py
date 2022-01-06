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
            ["testrepo - db1_a (1)"],
            [[0]],
        ),
        # Two revisions with the same sqlite and settings
        (
            gen_repo(
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
            ),
            ["testrepo - db1_a (1)"],
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
            ["testrepo - db1_a (1)", "testrepo - db2_a (1)"],
            [[0], [0]],
        ),
        # One revision with one sqlite with two settings
        (
            gen_repo(
                [
                    Sql(Path("db1"), settings=[Sett(1, "a"), Sett(2, "b")]),
                ]
            ),
            ["testrepo - db1_a (1)", "testrepo - db1_b (2)"],
            [[0], [0]],
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
            ["testrepo - db1_a (1)", "testrepo - db2_a (1)"],
            [[1, 0], [1]],
        ),
        # Two revisions with the same sqlite and settings, one settings entry added
        (
            gen_repo(
                [
                    Sql(Path("db1"), settings=[Sett(1, "a"), Sett(2, "b")]),
                    Sql(Path("db1"), settings=[]),
                ],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
            ),
            ["testrepo - db1_a (1)", "testrepo - db1_b (2)"],
            [[1, 0], [1]],
        ),
        # Setting is renamed: it is tracked (and the last revision will set the name)
        (
            gen_repo(
                [Sql(Path("db1"), settings=[Sett(1, "b")])],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
            ),
            ["testrepo - db1_b (1)"],
            [[1, 0]],
        ),
        # Settings entry skips a revision
        (
            gen_repo(
                [
                    Sql(Path("db1"), settings=[Sett(1, "a"), Sett(2, "c")]),
                ],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
                [
                    Sql(Path("db1"), settings=[Sett(1, "a"), Sett(2, "b")]),
                ],
            ),
            ["testrepo - db1_a (1)", "testrepo - db1_c (2)"],
            [[2, 1, 0], [2, 0]],
        ),
        # Renaming an sqlite is not allowed
        (
            gen_repo(
                [Sql(Path("db2"), settings=[Sett(1, "a")])],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
            ),
            ["testrepo - db1_a (1)", "testrepo - db2_a (1)"],
            [[0], [1]],
        ),
        # File is missing in changet of 2nd revision
        (
            gen_repo(
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
                [Sql(Path("db1"), settings=[Sett(1, "a")])],
                changes=[[], [Path("db1")]],
            ),
            ["testrepo - db1_a (1)"],
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
            ["testrepo - db1_a (1)"],
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
            ["testrepo - db1_a (1)"],
            [[0]],
        ),
        # The case and interpunction in filenames is ignored
        (
            gen_repo(
                [Sql(Path("db1a"), settings=[Sett(1, "a")])],
                [Sql(Path("DB1;a"), settings=[Sett(1, "a")])],
            ),
            ["testrepo - db1a_a (1)"],
            [[1, 0]],
        ),
    ],
)
def test_repo_to_schema(repository, expected_names, expected_nrs):
    actual = repository_to_schematisations(repository)["schematisations"]

    # sort by schematisation name
    actual = sorted(actual, key=lambda x: x.name)
    assert [x.name for x in actual] == expected_names
    assert [[rev.revision_nr for rev in x.revisions] for x in actual] == expected_nrs
