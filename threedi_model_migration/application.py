"""Main module."""
from .conversion import repository_to_schematisations
from .json_utils import custom_json_object_hook
from .json_utils import custom_json_serializer
from .repository import Repository

import dataclasses
import json


def download_inspect_plan(
    base_path, inspection_path, metadata, slug, remote, uuid, indent, last_update, cache
):
    repository = Repository(base_path, slug)
    _inspection_path = inspection_path / f"{slug}.json"

    # Download & Inspect if necessary
    if not cache or not _inspection_path.exists():
        # COPY FROM download
        if uuid:
            remote_name = str(metadata[repository.slug].repo_uuid)
        else:
            remote_name = repository.slug

        if remote.endswith("/"):
            remote = remote[:-1]

        repository.download(remote + "/" + remote_name)

        # COPY FROM inspect
        for revision, sqlite, settings in repository.inspect(last_update):
            record = {
                **dataclasses.asdict(revision),
                **dataclasses.asdict(sqlite),
                **dataclasses.asdict(settings),
            }
            record.pop("sqlites")
            record.pop("settings")

        inspection_path.mkdir(exist_ok=True)
        with _inspection_path.open("w") as f:
            json.dump(
                repository,
                f,
                indent=indent,
                default=custom_json_serializer,
            )
    else:
        with _inspection_path.open("r") as f:
            repository = json.load(f, object_hook=custom_json_object_hook)

    # COPY FROM plan
    result = repository_to_schematisations(repository, metadata)
    with (inspection_path / f"{repository.slug}.plan.json").open("w") as f:
        json.dump(
            result,
            f,
            indent=indent,
            default=custom_json_serializer,
        )
