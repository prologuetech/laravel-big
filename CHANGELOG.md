# Changelog

All Notable changes will be documented in this file.

Updates should follow the [Keep a CHANGELOG](http://keepachangelog.com/) principles.

## [0.1.1] - 2017-9-15
### Added
- Added ```BIG_DEFAULT_DATASET``` env option.
- Added optional delay to ```Big::createFromModel()``` to allow BigQuery time to create.
- Added STRUCT support for JSON type fields.

### Changed
- We now use [BigQuery's insertId](https://cloud.google.com/bigquery/streaming-data-into-bigquery#dataconsistency) if we have an ID column.
- Insert now returns true on success.
- We now pass errors onward to implementation.

### Fixed
- Fixed TIMESTAMP type.
