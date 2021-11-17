from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict
from uuid import UUID

import json
import pytz


TIMEZONE = pytz.timezone("Europe/Amsterdam")

# bin/django-admin dumpdata --natural-foreign --indent=4 model_databank.ModelType model_databank.ModelReference > var/migration_dump.json


@dataclass
class SchemaMeta:
    name: str
    slug: str
    repo_uuid: UUID

    owner: str
    created: datetime
    created_by: str

    meta: Dict[str, str]

    @classmethod
    def from_dump(cls, record):
        """Parse a record like this:

        {
            "model": "model_databank.modelreference",
            "pk": 9,
            "fields": {
                "owner": [
                    "wouter.vanesse"
                ],
                "organisation": [
                    "2cdbf687dc6f48989ce3316c828a2121"
                ],
                "type": 1,
                "identifier": "Aalkeet Binnen- & Buitenpolder",
                "slug": "aalkeet-binnen-buitenpolder",
                "uuid": "8f9080ca-d06b-11e3-86f2-0050569e2003",
                "description": "Aalkeet Binnen- & Buitenpolder",
                "created": "2014-04-30T15:30:13.994",
                "last_repo_update": "2014-07-24T09:14:03",
                "is_deleted": false
            }
        }
        """
        assert record["model"] == "model_databank.modelreference"
        fields = record["fields"]

        meta = {"version": fields["type"]}
        if fields["description"]:
            meta["description"] = fields["description"]
        return cls(
            name=fields["identifier"],
            slug=fields["slug"],
            repo_uuid=UUID(fields["uuid"]),
            owner=fields["organisation"][0],
            created=datetime.fromisoformat(fields["created"]).replace(tzinfo=TIMEZONE),
            created_by=fields["owner"][0],
            meta=meta,
        )


def load_metadata(metadata_path: Path):
    with metadata_path.open("r") as f:
        data = json.load(f)

    type_lut = {}
    result = {}
    for record in data:
        if record["model"] == "model_databank.modeltype":
            type_lut[record["pk"]] = record["fields"]["name"]
            continue
        if record["model"] != "model_databank.modelreference":
            continue
        # patch type
        record["fields"]["type"] = type_lut[record["fields"]["type"]]
        # skip deleted ones
        if record["fields"]["is_deleted"]:
            continue
        metadata = SchemaMeta.from_dump(record)
        result[metadata.slug] = metadata

    return result
