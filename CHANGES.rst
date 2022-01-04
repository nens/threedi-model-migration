=======
History
=======

0.7.1 (2022-01-04)
------------------

- Fix underscores in project name.


0.7.0 (2022-01-04)
------------------

- Adapt schematisation name and slug to be more similar to the old convention and to
  fix slug uniqueness issues.

- Changed Schematisation.sqlite_name to Schematisation.sqlite_path.


0.6.1 (2021-12-20)
------------------

- Fix revision number setting.


0.6.0 (2021-12-16)
------------------

- Filter global settings before sending sqlites to the API.


0.5.0 (2021-12-15)
------------------

- Add user_mapping_path to map Mercurial to API users.

- Added an amqp consumer.

- Fixed bug in sqlite zipping.

- Added sentry support.

- Add logfile cli argument.

- Do not emit errors if a schematisation does not exist in the API.


0.4.0 (2021-12-08)
------------------

- Skip 3Di v1 repositories.

- Implement owner_blacklist_path.

- Removed off-by-one in revision_nr. Now the original mercurial numbers are taken and
  not the ones from models.lizard.net (which are + 1).

- Added schematisation name (incl. uniqueness check).

- Added "files_omitted" attribute to schematisation plans.

- Don't include SchemaRevisions that didn't have any change for the specfic
  schematisation.

- All schematisations with identical (repository, sqlite path, settings id) are
  grouped together now.

- Added 'ifnewer' to the download method: only download the repo if the remote has a
  newer revision present.

- Added 'incremental' to inspect: only inspect revisions that were not previously
  inspected.

- Also look for raster files relative to the repo root (not only relative to the sqlite)

- Don't trust the "last_update" field in modeldatabank; get it from mercurial instead.

- Don't try to merge schematisations that come from renamed sqlites. It leads to complex
  issues when matching schematisations to the schematisations in the API.

- Added 'push' functionality.


0.3.0 (2021-11-29)
------------------

- Added patch-uuid command to enrich inspection reports without any metadata, using the
  symlinks to map uuids to repository slugs.


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
