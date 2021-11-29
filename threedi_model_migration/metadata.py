from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict
from uuid import UUID

import json
import pytz


TIMEZONE = pytz.timezone("Europe/Amsterdam")

# MODELDATABANK
# bin/django-admin dumpdata --natural-foreign --indent=4 lizard_auth_client.Organisation model_databank.ModelType model_databank.ModelReference > var/migration_dump.json
# INPY
# bin/django dumpdata --indent=4 lizard_auth_client.Organisation threedi_model.ThreediModelRepository threedi_model.ThreediRevisionModel threedi_model.ThreediSQLiteModel threedi_model.ThreediModel > inpy.json

# SYMLINK_REGEX = re.compile(r".*\s(\S+)\s->.*(\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b).*")


@dataclass
class SchemaMeta:
    name: str
    slug: str
    repo_uuid: UUID

    owner: str
    created: datetime
    created_by: str
    last_update: datetime

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

        # parse datetimes:
        created = datetime.fromisoformat(fields["created"])
        if fields["last_repo_update"] is not None:
            last_update = datetime.fromisoformat(fields["last_repo_update"])
        else:
            last_update = created
        return cls(
            name=fields["identifier"],
            slug=fields["slug"],
            repo_uuid=UUID(fields["uuid"]),
            owner=fields["organisation"][0],
            created=TIMEZONE.localize(created),
            last_update=TIMEZONE.localize(last_update),
            created_by=fields["owner"][0],
            meta=meta,
        )


def load_modeldatabank(metadata_path: Path) -> Dict[str, SchemaMeta]:
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


@dataclass
class InpyMeta:
    n_threedimodels: int = 0
    n_inp_success: int = 0


def load_inpy(inpy_path: Path) -> Dict[str, InpyMeta]:
    with inpy_path.open("r") as f:
        data = json.load(f)

    repo_lut = {}
    org_lut = {}
    result = defaultdict(InpyMeta)
    for record in data:
        if record["model"] == "threedi_model.threedimodelrepository":
            repo_lut[record["pk"]] = record["fields"]["slug"]
        elif record["model"] == "lizard_auth_client.organisation":
            org_lut[record["fields"]["unique_id"]] = record["fields"]["name"]
        elif record["model"] == "threedi_model.threedimodel":
            repo_slug = repo_lut[record["fields"]["threedi_model_repository"]]
            result[repo_slug].n_threedimodels += 1
            if record["fields"]["inp_success"]:
                result[repo_slug].n_inp_success += 1

    return result, org_lut
