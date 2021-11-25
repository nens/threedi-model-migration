=======
History
=======

0.2.2 (unreleased)
------------------

- Nothing changed yet.


0.2.1 (2021-11-25)
------------------

- Fixed issues in legacy repositories that used Latin-1 encoding for filenames.

- Always to a clean update (ignoring uncommitted changes).

- Retry updates one time.


0.2.0 (2021-11-24)
------------------

- Renamed repository.name to repository.slug.

- Added load_metdata to load a database dump from the modeldatabank.

- Shifted 'remote' from Repository init to download.

- Added -m parameter (metadata_path) to cli, and --uuid to the download command. This
  enables mapping repo slugs to uuids.

- Extract rasters referenced in sqlite and files changed in commit (changeset).

- Added 'delete' command.

- Add metadata to schematisation output.

- Added 'batch' command.

- Deal with nonexisting files.

- Clear largefiles cache ($HOME/.cache/largefiles) after each clone or pull.

- Integrate data from inpy.


0.1.0 (2021-11-11)
------------------

- Set-up repository.

- Created Repository and RepositoryRevisions models. Repositories can download,
  list, and checkout.

- Create Schematisation models and logic to convert a repository to schematisations.

- Created command line interface.

- List sqlites in a revision and list settings entries in sqlites.

- Allow renaming of sqlite (only 1 sqlite can be renamed at a time).
