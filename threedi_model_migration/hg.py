from datetime import datetime
from urllib.parse import unquote

import logging
import subprocess


logger = logging.getLogger(__name__)


def get_output(command, cwd=".", fail_on_exit_code=True, log=True):
    """Run command and return output.

    ``command`` is just a string like "cat something".
    ``cwd`` is the directory where to execute the command.
    """
    process = subprocess.Popen(
        command,
        cwd=cwd,
        shell=True,
        universal_newlines=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    i, o, e = (process.stdin, process.stdout, process.stderr)
    i.close()
    output = o.read()
    error_output = e.read()
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


def update(repo_path, revision_hash):
    get_output(f"hg update -v {revision_hash}", cwd=repo_path)


def pull_all_largefiles(repo_path):
    get_output('hg lfpull --rev "all()"', cwd=repo_path)


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


LOG_TEMPLATE = "{rev},{node},{desc|urlescape},{user|urlescape},{date|isodate}\n"


def parse_log_entry(row):
    rev, node, desc, user, date = row.split(",")
    return {
        "revision_nr": int(rev),
        "revision_hash": node,
        "commit_msg": unquote(desc),
        "commit_user": unquote(user),
        "last_update": convert_to_date(date),
    }


def log(repo_path):
    output = get_output(f'hg log -T "{LOG_TEMPLATE}"', cwd=repo_path, log=False)
    return [parse_log_entry(row) for row in output.strip().split("\n")]
