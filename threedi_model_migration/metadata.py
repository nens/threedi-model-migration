from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict
from typing import Optional
from uuid import UUID

import json
import pytz
import re


TIMEZONE = pytz.timezone("Europe/Amsterdam")

# MODELDATABANK
# bin/django-admin dumpdata --natural-foreign --indent=4 lizard_auth_client.Organisation model_databank.ModelType model_databank.ModelReference > var/migration_dump.json
# INPY
# bin/django dumpdata --indent=4 lizard_auth_client.Organisation threedi_model.ThreediModelRepository threedi_model.ThreediRevisionModel threedi_model.ThreediSQLiteModel threedi_model.ThreediModel > inpy.json

# SYMLINKS
# ls -larth

SYMLINK_REGEX = re.compile(
    r".*\s(\S+)\s->.*(\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b).*"
)


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
    def from_dump(cls, record, v2_type_pk, owner_blacklist):
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
        assert v2_type_pk is not None

        fields = record["fields"]

        # skip deleted ones
        if fields["is_deleted"]:
            return
        # only process 3di-v2 model types
        if fields["type"] != v2_type_pk:
            return
        # skip blacklisted organisations
        if fields["organisation"][0] in owner_blacklist:
            return

        meta = {}
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


def load_modeldatabank(
    metadata_path: Path, owner_blacklist_path: Optional[Path] = None
) -> Dict[str, SchemaMeta]:
    """Load modeldatabank database dump

    Some filters are in place:

    - organisation blacklist
    - only do 3di-v2 modeltypes
    """
    with metadata_path.open("r") as f:
        data = json.load(f)
    if owner_blacklist_path:
        with owner_blacklist_path.open("r") as f:
            lines = [x.strip() for x in f.readlines()]
            owner_blacklist = set([x for x in lines if len(x) == 32])
    else:
        owner_blacklist = set()

    v2_type_pk = None
    result = {}
    for record in data:
        if record["model"] == "model_databank.modeltype":
            if record["fields"]["slug"] == "3di-v2":
                v2_type_pk = record["pk"]
            continue
        if record["model"] != "model_databank.modelreference":
            continue

        metadata = SchemaMeta.from_dump(record, v2_type_pk, owner_blacklist)
        if metadata is not None:
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


def load_symlinks(symlink_path: Path) -> Dict[UUID, str]:
    with symlink_path.open("r") as f:
        lines = f.readlines()

    matches = [SYMLINK_REGEX.findall(line) for line in lines]
    matches = [m[0] for m in matches if len(m) == 1]
    return {UUID(m[1]): m[0] for m in matches}
