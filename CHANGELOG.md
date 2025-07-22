# Changelog

## 0.1.2 - TO BE RELEASED
Development notes

Breaking changes:
- **Removed legacy configuration format support**: Single-pipeline configuration format is no longer supported. All configurations must use the multi-pipeline format with a `pipelines` section.

New features
- **TUI Mode**: Added Text User Interface (TUI) for enhanced visual monitoring of ETL processes with real-time pipeline status, dataset progress, and interactive log viewing. Enable with `--tui` flag or `mode: tui` in logging configuration.
- Specify number of parallel workers via a `processing` section in the config file.
- Comprehensive Sphinx documentation setup with automatic API documentation generation.
- Enhanced error tracking and reporting system for better debugging.
- File connectors support for CSV and JSON data sources.
- Pipeline-aware state management for multi-pipeline processing.

Bug fixes:
- Issue Bug #4: Column names that are a database keyword cause an error.
- Issue fix: Postgresql connector did not check if table existed before truncating table.
- Improved logging level consistency across all handlers (console, TUI, and file).

Developer improvements:
- Added comprehensive type annotations and pytest markers for unit tests.
- Enhanced CONTRIBUTING.md with detailed contribution guidelines and testing instructions.
- Improved code quality with better error handling and validation.

## 0.1.1 - 2025-07-18
Changes in this version
 
- Fixed copyright and author information
- Published package to pypi

## 0.1.0 - 2025-07-17
_First Release._ 