=======================
threedi_model_migration
=======================

Tooling to migrate models from models.lizard.net to 3di.live

Features
--------

* Download a 3Di model repository (mercurial / hg / tortoise)
* List all revisions in a repository and export to JSON / CSV
* Checkout a specific revision

Usage
-----

First cd into the root that contains your local repositories (or a tmpdir)::

$ cd /my/path/to/models

Alternatively specify this dir using `--base_path`.

Show help::

$ threedi_model_migration --help

Clone / pull::

$ threedi_model_migration download v2_bergermeer

Inspect revisions and write to the inspection file in `/_inspection/{repo_name}.json`::

$ threedi_model_migration inspect v2_bergermeer --last_update 2019-01-01

Create a migration plan in `/_inspection/{repo_name}.plan.json`::

$ threedi_model_migration plan v2_bergermeer
v2_bergermeer-v2_bergermeer.sqlite-simple_infil_no_grndwtr: 95-107

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
