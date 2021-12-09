from datetime import datetime
from threedi_model_migration.zip_utils import deterministic_zip

import io
import os
import time


def test_determistic_zip(tmp_path):
    path = tmp_path / "file.txt"
    with path.open("w") as f:
        f.write("foo")

    s1 = io.BytesIO()
    deterministic_zip(s1, [path])

    # set different modification time
    dt = datetime(2010, 1, 1)
    os.utime(path, (time.mktime(dt.timetuple()), time.mktime(dt.timetuple())))

    # set file permissions
    os.chmod(path, 0o400)

    s2 = io.BytesIO()
    deterministic_zip(s2, [path])

    assert s1.getvalue() == s2.getvalue()
