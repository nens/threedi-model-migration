from datetime import datetime
from threedi_model_migration.metadata import load_metadata
from uuid import UUID

import pytz


def test_load_metadata(metadata_json_path):
    metadata = load_metadata(metadata_json_path)
    assert len(metadata) == 2

    bb = metadata["binnen-buitenpolder"]
    assert bb.name == "Binnen- & Buitenpolder"
    assert bb.slug == "binnen-buitenpolder"
    assert bb.repo_uuid == UUID("87512cd2-6518-484f-846d-7c6c043845b1")
    assert bb.created == datetime(
        2014, 4, 30, 15, 30, 13, 994000, tzinfo=pytz.timezone("Europe/Amsterdam")
    )
    assert bb.created_by == "piet"
    assert bb.meta == {"version": "3Di", "description": "foo"}

    d = metadata["dijkpolder"]
    assert d.meta == {"version": "3Di v2"}