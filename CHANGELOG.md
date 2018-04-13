# Changelog

All Notable changes will be documented in this file.

Updates should follow the [Keep a CHANGELOG](http://keepachangelog.com/) principles.

## [Unreleased]
### Added
- Added nullable/required field modes.

### Changed
- Fix company name typo.
- Fixed `vendor:publish` command.
- Google Cloud SDK 0.49 -> 0.60.

### Removed
- Removed old composer requirements.

## [0.1.2] - 2018-4-06
### Added
- Added double support for BQ types.
- Added get max ID/date helpers.
- Added STRUCT support for JSON type fields.
- Added Laravels auto discovery support.
- Added ability to specify a default dataset - config('prologue-big.big.default_dataset').

### Changed
- Google Cloud v0.32.1 -> v0.49.

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
