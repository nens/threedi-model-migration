from .file import File
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


def custom_json_serializer(o):
    """For JSON dumping. Serialize datetimes, paths, dataclasses."""
    if isinstance(o, datetime.datetime):
        return o.isoformat()
    elif isinstance(o, pathlib.Path):
        return str(o)
    elif dataclasses.is_dataclass(o):
        result = OrderedDict(type=o.__class__.__name__)
        for field in dataclasses.fields(o):
            value = getattr(o, field.name)
            if value is not None:
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
        File,
    )
}


def custom_json_object_hook(dct):
    """For JSON loading. If an object contains a 'type' element: reconstitute dataclass"""
    cls = DATACLASS_TYPE_LOOKUP.get(dct.get("type", None))
    if cls is None:
        return dct

    kwargs = {}
    for name, dtype in cls.__annotations__.items():
        if name not in dct:
            continue
        value = dct[name]
        if dtype is datetime.datetime:
            value = datetime.datetime.fromisoformat(value)
        elif dtype is pathlib.Path:
            value = pathlib.Path(value)
        kwargs[name] = value
    return cls(**kwargs)
