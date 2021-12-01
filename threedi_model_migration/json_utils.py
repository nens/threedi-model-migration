from .file import File
from .file import Raster
from .metadata import SchemaMeta
from .repository import RepoRevision
from .repository import RepoSettings
from .repository import Repository
from .repository import RepoSqlite
from .schematisation import SchemaRevision
from .schematisation import Schematisation
from collections import OrderedDict

import dataclasses
import datetime
import pathlib
import typing
import uuid


def custom_json_serializer(o):
    """For JSON dumping. Serialize datetimes, paths, dataclasses."""
    if isinstance(o, datetime.datetime):
        return o.isoformat()
    elif isinstance(o, pathlib.Path):
        return str(o)
    elif isinstance(o, uuid.UUID):
        return str(o)
    elif dataclasses.is_dataclass(o):
        result = OrderedDict(type=o.__class__.__name__)
        for field in dataclasses.fields(o):
            result[field.name] = getattr(o, field.name)
        return result


DATACLASS_TYPE_LOOKUP = {
    cls.__name__: cls
    for cls in (
        Repository,
        RepoRevision,
        RepoSqlite,
        RepoSettings,
        Schematisation,
        SchemaRevision,
        SchemaMeta,
        File,
        Raster,
    )
}


def custom_json_object_hook(dct):
    """For JSON loading. If an object contains a 'type' element: reconstitute dataclass"""
    cls = DATACLASS_TYPE_LOOKUP.get(dct.get("type", None))
    if cls is None:
        return dct

    kwargs = {}
    for name, dtype in typing.get_type_hints(cls).items():
        if name not in dct:
            continue
        value = dct[name]
        if dtype is datetime.datetime:
            value = datetime.datetime.fromisoformat(value)
        elif dtype is pathlib.Path:
            value = pathlib.Path(value)
        elif dtype is uuid.UUID:
            value = uuid.UUID(value)
        kwargs[name] = value

    return cls(**kwargs)
