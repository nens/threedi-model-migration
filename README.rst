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

Help::

>>> threedi_model_migration --help

Clone / pull::

>>> threedi_model_migration -b ./var -n v2_bergermeer download

List revisions::

>>> threedi_model_migration -b ./var -n v2_bergermeer ls --format csv

Checkout a revision::

>>> threedi_model_migration -b ./var -n v2_bergermeer checkout 63a9a8cd

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
