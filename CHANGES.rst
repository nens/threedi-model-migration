=======
History
=======

0.1.1 (unreleased)
------------------

- Renamed repository.name to repository.slug.

- Added load_metdata to load a database dump from the modeldatabank.

- Shifted 'remote' from Repository init to download.

- Added -m parameter (metadata_path) to cli, and --uuid to the download command. This
  enables mapping repo slugs to uuids.

- Extract rasters referenced in sqlite and files changed in commit (changeset).

- Added 'delete' command.

- Add metadata to schematisation output.


0.1.0 (2021-11-11)
------------------

- Set-up repository.

- Created Repository and RepositoryRevisions models. Repositories can download,
  list, and checkout.

- Create Schematisation models and logic to convert a repository to schematisations.

- Created command line interface.

- List sqlites in a revision and list settings entries in sqlites.

- Allow renaming of sqlite (only 1 sqlite can be renamed at a time).
