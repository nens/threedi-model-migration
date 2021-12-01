from pathlib import Path
from threedi_model_migration.file import File
from threedi_model_migration.file import Raster
from threedi_model_migration.repository import RepoRevision
from threedi_model_migration.repository import Repository

import factory


class FileFactory(factory.Factory):
    md5 = factory.Faker("md5", raw_output=False)
    size = factory.Faker("random_int", min=1)

    class Meta:
        model = File


class RasterFactory(factory.Factory):
    md5 = factory.Faker("md5", raw_output=False)
    size = factory.Faker("random_int", min=1)

    class Meta:
        model = Raster


class RepoRevisionFactory(factory.Factory):
    revision_hash = factory.Faker("md5", raw_output=False)
    last_update = factory.Faker("past_datetime")
    commit_msg = factory.Faker("sentence")
    commit_user = factory.Faker("first_name")

    class Meta:
        model = RepoRevision


class RepositoryFactory(factory.Factory):
    base_path = Path("/tmp")
    slug = factory.Faker("slug")

    class Meta:
        model = Repository
