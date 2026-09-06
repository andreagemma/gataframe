# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

- Added repository Copilot instructions in `.github/copilot-instructions.md` aligned with project governance for versioning, docs, changelog, manifest, and licensing updates.
- Added third-party licensing artifacts generated from dependencies declared in `pyproject.toml`:
  `THIRD_PARTY_NOTICES.md`, `licenses/third_party/summary.tsv`, and archived package license files under `licenses/third_party/packages/`.
- Updated `MANIFEST.in` to include third-party notices and license inventory artifacts in source distributions.

## 0.1.0 - 2026-09-02

- Prepared the package for the future `andreagemma/gataframe` GitHub repository.
- Moved the import package to `src/gataframe`.
- Added project metadata, CI/release workflows, documentation, tests, and MIT
  license information for GataFrame.
- Fixed valid `GataFrame` column operations that were rejected by overly broad
  assertion checks.
- Removed the serializer helper from the public package and documentation.

