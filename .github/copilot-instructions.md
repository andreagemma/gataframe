# Copilot Instructions for gataframe

## Project Description

gataframe is a typed Python package with DuckDB-backed dataframe helpers for tabular and geospatial workflows.

## Scope

These instructions apply to the entire repository.

## Project Context

- This is a Python package with source code under `src/gataframe`.
- Keep compatibility with the existing package structure and public APIs unless explicitly requested.

## Development Rules

- Prefer small, targeted changes.
- Keep code typed and add type annotations for public functions, classes, and variables.
- Preserve existing public API behavior unless explicitly requested otherwise.
- Add tests for behavior changes in `tests/`.
- Keep style consistent with the existing codebase.
- Prefer efficient implementations while keeping code readable.
- Use reusable helpers to avoid duplication in the package.

## Quality Checks

Before finalizing changes, run:

- `pytest -q`

If relevant to modified files, also run project quality scripts.

## Versioning, Release Alignment, and Docs

- For any behavior, interface, or dependency change, update project version information consistently.
- Keep `CHANGELOG.md` updated for all user-visible changes.
- Keep `README.md` and `docs/` content aligned with implemented behavior.
- Verify required dependencies are correctly declared in `pyproject.toml`.
- Keep `MANIFEST.in` aligned with packaged artifacts.

## Third-Party Licensing Workflow

Based on dependencies declared in `pyproject.toml`:

- Collect and archive third-party license files under `licenses/third_party/packages/<package>/`.
- Save at least one `LICENSE` file for each package.
- Save `COPYING` too when upstream provides it.
- Keep `licenses/third_party/summary.tsv` updated with columns:
  `package`, `version`, `license_file`, `source_url`.
- Keep `THIRD_PARTY_NOTICES.md` updated.
- Ensure `MANIFEST.in` includes these licensing artifacts.

## Documentation

- Update user-facing docs when behavior or interfaces change.
- Keep docstrings concise and useful (Sphinx-style where applicable): short summary, params, returns, and raised exceptions.

## Safety

- Do not run destructive git history operations.
- If requirements are ambiguous, ask for clarification before broad refactors.