"""Edited from: https://github.com/bboe/deterministic_zip/blob/main/deterministic_zip/__init__.py"""

from pathlib import Path
from typing import BinaryIO
from typing import List

import os
import stat
import zipfile


def _add_file(zip_file, path, zip_path=None):
    permission = 0o555 if os.access(path, os.X_OK) else 0o444
    zip_info = zipfile.ZipInfo.from_file(path, zip_path)
    zip_info.date_time = (1970, 1, 1, 0, 0, 0)
    zip_info.external_attr = (stat.S_IFREG | permission) << 16
    with open(path, "rb") as fp:
        zip_file.writestr(
            zip_info,
            fp.read(),
            compress_type=zipfile.ZIP_DEFLATED,
            compresslevel=9,
        )


def deterministic_zip(fp: BinaryIO, paths: List[Path]):
    with zipfile.ZipFile(fp, "w") as zip_file:
        for path in paths:
            if not path.is_file():
                raise ValueError(f"{path} is not a file")
            _add_file(zip_file, path, path.name)
