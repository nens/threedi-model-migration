from enum import Enum
from pathlib import Path
from typing import BinaryIO
from typing import Optional

import dataclasses
import hashlib
import logging


logger = logging.getLogger(__name__)

SQLITE_COMPRESSION_RATIO = 7


class RasterOptions(Enum):
    dem_file = "dem_file"
    equilibrium_infiltration_rate_file = "equilibrium_infiltration_rate_file"
    frict_coef_file = "frict_coef_file"
    initial_groundwater_level_file = "initial_groundwater_level_file"
    initial_waterlevel_file = "initial_waterlevel_file"
    groundwater_hydro_connectivity_file = "groundwater_hydro_connectivity_file"
    groundwater_impervious_layer_level_file = "groundwater_impervious_layer_level_file"
    infiltration_decay_period_file = "infiltration_decay_period_file"
    initial_infiltration_rate_file = "initial_infiltration_rate_file"
    leakage_file = "leakage_file"
    phreatic_storage_capacity_file = "phreatic_storage_capacity_file"
    hydraulic_conductivity_file = "hydraulic_conductivity_file"
    porosity_file = "porosity_file"
    infiltration_rate_file = "infiltration_rate_file"
    max_infiltration_capacity_file = "max_infiltration_capacity_file"
    interception_file = "interception_file"


def _iter_chunks(fileobj: BinaryIO, chunk_size: int = 16777216):
    """Yield chunks from a file stream"""
    assert chunk_size > 0
    while True:
        data = fileobj.read(chunk_size)
        if len(data) == 0:
            break
        yield data


def compute_md5(path: Path, chunk_size: int = 16777216):
    """Returns md5 and file size"""
    logger.debug(f"Computing hash of file {path}...")
    with path.open("rb") as fileobj:
        hasher = hashlib.md5()
        for chunk in _iter_chunks(fileobj, chunk_size=chunk_size):
            hasher.update(chunk)
        md5 = hasher.hexdigest()
        file_size = fileobj.tell()

    return md5, file_size


def compare_paths(actual: Path, user_supplied: Path):
    if actual.suffix == ".sqlite":
        return actual == user_supplied

    # convert to lowercase string
    actual = actual.as_posix().lower()
    user_supplied = user_supplied.as_posix().lower()

    # replace backslash with slash
    user_supplied.replace("\\", "/")
    # strip whitespace and quotes
    user_supplied.strip().strip("'\"")

    return actual == user_supplied


@dataclasses.dataclass
class File:
    path: Path
    size: Optional[int] = None  # in bytes
    md5: Optional[str] = None

    def compute_md5(self, base_path: Path):
        self.md5, self.size = compute_md5(base_path / self.path)
        self.size = int(self.size / SQLITE_COMPRESSION_RATIO)

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self.path)})"

    def __hash__(self):
        return int(self.md5, 16)


@dataclasses.dataclass
class Raster(File):
    raster_type: RasterOptions = None

    def compute_md5(self, base_path: Path):
        self.md5, self.size = compute_md5(base_path / self.path)

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self.path)})"

    def as_file(self) -> File:
        return File(path=self.path, size=self.size, md5=self.md5)
