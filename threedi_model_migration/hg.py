from datetime import datetime
from pathlib import Path
from urllib.parse import unquote_to_bytes
import shutil

import logging
import os
import subprocess


logger = logging.getLogger(__name__)


def decode(bytestring: bytes):
    # Decode using the file system encoding.
    return os.fsdecode(bytestring)


def unquote(s: str):
    # Unquote and decode using the file system encoding.
    return decode(unquote_to_bytes(s))


def get_output(command, cwd=".", fail_on_exit_code=True, log=True):
    """Run command and return output.

    ``command`` is just a string like "cat something".
    ``cwd`` is the directory where to execute the command.
    """
    process = subprocess.Popen(
        command,
        cwd=cwd,
        shell=True,
        universal_newlines=False,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    i, o, e = (process.stdin, process.stdout, process.stderr)
    i.close()
    output = decode(o.read())
    error_output = decode(e.read())
    o.close()
    e.close()
    exit_code = process.wait()
    if exit_code and fail_on_exit_code:
        raise RuntimeError("Running %s failed: %s" % (command, error_output))
    if log:
        for row in output.strip().split("\n"):
            logger.debug(row)
    return output


def clone(repo_path, remote):
    get_output(f"hg clone -v {remote} {repo_path.resolve()}")


def pull(repo_path, remote):
    get_output(f"hg pull -v {remote}", cwd=repo_path)


def identify_tip(remote):
    return get_output(f"hg id -r tip {remote} -T {{node}}")


def update(repo_path, revision_hash):
    try:
        get_output(f"hg update -v {revision_hash} -C -y", cwd=repo_path)
    except RuntimeError:
        # just try again.... Mercurial...
        get_output(f"hg update -v {revision_hash} -C -y", cwd=repo_path)


def pull_all_largefiles(repo_path, remote):
    get_output(f'hg lfpull --rev "all()" {remote}', cwd=repo_path)


def clear_largefiles_cache():
    cachedir = Path.home() / ".cache/largefiles"
    if not cachedir.exists():
        return
    for root, dirs, files in os.walk(cachedir.as_posix()):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))


def init(repo_path):
    get_output(f"hg init -v {repo_path.resolve()}")


def add(repo_path, filename):
    get_output(f"hg add -v {filename}", cwd=repo_path)


def commit(repo_path, filename, message):
    get_output(f'hg commit -v -m "{message}" {filename}', cwd=repo_path)


def convert_to_date(date: str) -> datetime:
    # Mercurial's isodate isn't really ISO8601 compliant.
    # The timezone misses a :
    fixed_date = date[:-2] + ":" + date[-2:]
    return datetime.fromisoformat(fixed_date)


LOG_TEMPLATE = "{rev},{node},{desc|urlescape},{user|urlescape},{date|isodate},{file_adds % '{file|urlescape}|'},{file_mods % '{file|urlescape}|'},{file_copies % '{file|urlescape}|'}\n"


def filter_files(paths):
    for path in paths:
        if path.startswith(".hglf/"):
            path = path[6:]
        _, ext = os.path.splitext(path)
        if ext.lower() in (".tif", ".tiff", ".geotif", ".geotiff", ".sqlite"):
            yield Path(path)


def parse_log_entry(row):
    rev, node, desc, user, date, file_adds, file_mods, file_copies = row.split(",")
    # unpack the files
    files = (
        [unquote(f) for f in file_adds.split("|")[:-1]]
        + [unquote(f) for f in file_mods.split("|")[:-1]]
        + [unquote(f) for f in file_copies.split("|")[:-1]]
    )
    return {
        "revision_nr": int(rev),
        "revision_hash": node,
        "commit_msg": unquote(desc),
        "commit_user": unquote(user),
        "last_update": convert_to_date(date),
        "changes": list(filter_files(files)),
    }


def log(repo_path):
    output = get_output(f'hg log -T "{LOG_TEMPLATE}"', cwd=repo_path, log=False)
    return [parse_log_entry(row) for row in output.strip().split("\n")]


def files(repo_path, revision_hash):
    """List all files in the repo"""
    output = get_output(
        f'hg files --rev {revision_hash} -X ".hgignore"', cwd=repo_path, log=False
    )
    return list(filter_files(output.strip().split("\n")))
