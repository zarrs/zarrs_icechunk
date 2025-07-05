# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Bump MSRV to 1.85 to match `icechunk` (v0.2.8)

## [0.2.1] - 2025-03-10

### Fixed
- Fix `AsyncIcechunkStore::list_dir()` with prefixes

## [0.2.0] - 2025-02-21

### Changed
- Bump `icechunk` to 0.2.0

## [0.1.1] - 2025-02-08

### Changed
- Bump `icechunk` to 0.1.2
  - This release had a breaking change

## [0.1.0] - 2025-02-01

### Changed
- **Breaking**: Bump `icechunk` to 0.1.0

## [0.1.0-alpha.3] - 2025-01-24

### Added
- `AsyncIcechunkStore` now supports `size[_{key,prefix}]`

### Changed
- **Breaking**: Bump `icechunk` to 0.1.0-alpha.13

## [0.1.0-alpha.2] - 2025-01-06

### Added
- Add `AsyncIcechunkStore::session()`

### Changed
- **Breaking**: Bump `icechunk` to 0.1.0-alpha.8
- **Breaking**: Bump MSRV to 1.81

### Removed
- **Breaking**: Remove `AsyncIcechunkStore::{icechunk_store,current_branch,snapshot_id,current_version,has_uncommitted_changes,reset,checkout,new_branch,commit,tag}()`
  - Instead access the underlying methods from the session with `AsyncIcechunkStore::session()`, e.g. `store.session().write().await.commit(...)`

## [0.1.0-alpha.1] - 2024-11-28

### Changed
- Bump `zarrs_storage` to 0.3.0 (`zarrs` 0.18)
- Bump `icechunk` to 0.1.0-alpha.5

## [0.1.0-alpha.0] - 2024-10-18

### Added
- Initial release

[unreleased]: https://github.com/LDeakin/zarrs_icechunk/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/LDeakin/zarrs_icechunk/releases/tag/v0.2.1
[0.2.0]: https://github.com/LDeakin/zarrs_icechunk/releases/tag/v0.2.0
[0.1.1]: https://github.com/LDeakin/zarrs_icechunk/releases/tag/v0.1.1
[0.1.0]: https://github.com/LDeakin/zarrs_icechunk/releases/tag/v0.1.0
[0.1.0-alpha.3]: https://github.com/LDeakin/zarrs_icechunk/releases/tag/v0.1.0-alpha.3
[0.1.0-alpha.2]: https://github.com/LDeakin/zarrs_icechunk/releases/tag/v0.1.0-alpha.2
[0.1.0-alpha.1]: https://github.com/LDeakin/zarrs_icechunk/releases/tag/v0.1.0-alpha.1
[0.1.0-alpha.0]: https://github.com/LDeakin/zarrs_icechunk/releases/tag/v0.1.0-alpha.0
