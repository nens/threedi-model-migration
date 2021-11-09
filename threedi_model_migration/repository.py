from . import hg
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List
from typing import Optional

import logging


logger = logging.getLogger(__name__)

DEFAULT_REMOTE = "https://hg.lizard.net/"


@dataclass
class RepositoryRevision:
    revision_nr: int
    revision_hash: str
    last_update: datetime
    commit_msg: str
    commit_user: str

    def __repr__(self):
        first_line = self.commit_msg.split("\n")[0]
        return f"RepositoryRevision({self.revision_nr}: {first_line})"


class Repository:
    def __init__(self, base_path: Path, name: str, remote: Optional[str] = None):
        self.base_path = base_path
        self.name = name
        if remote is None:
            self.remote = DEFAULT_REMOTE + name
        else:
            self.remote = remote
        self._revisions: Optional[List[RepositoryRevision]] = None

    @property
    def path(self):
        return self.base_path / self.name

    def download(self):
        """Get the latest commits from the remote (calls hg clone / pull and lfpull)"""
        if self.remote is None:
            raise ValueError("Cannot download because remote is not set")
        if self.path.exists():
            logger.info(f"Pulling from {self.remote}...")
            hg.pull(self.path, self.remote)
            logger.info("Done.")
        else:
            logger.info(f"Cloning from {self.remote}...")
            hg.clone(self.path, self.remote)
            logger.info("Done.")
        logger.info("Pulling largefiles...")
        hg.pull_all_largefiles(self.path)
        logger.info("Done.")

    @property
    def revisions(self) -> List[RepositoryRevision]:
        """Return a list of revisions, ordered newest first (calls hg log)"""
        if self._revisions is None:
            self._revisions = [RepositoryRevision(**x) for x in hg.log(self.path)]
        return self._revisions

    def checkout(self, revision_hash: str):
        """Update the working directory to given revision hash (calls hg update)"""
        try:
            int(revision_hash)
        except ValueError:
            pass
        else:
            raise ValueError("Please supply a revision hash, not a number")
        hg.update(self.path, revision_hash)
        logger.info(f"Updated working directory to revision {revision_hash}.")
