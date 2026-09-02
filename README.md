# GataFrame

[![CI](https://github.com/andreagemma/gataframe/actions/workflows/ci.yml/badge.svg)](https://github.com/andreagemma/gataframe/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/ga-gataframe.svg)](https://pypi.org/project/ga-gataframe/)
[![Python](https://img.shields.io/pypi/pyversions/ga-gataframe.svg)](https://pypi.org/project/ga-gataframe/)

GataFrame is a lightweight Python library built around DuckDB relations. It
adds a dataframe-style wrapper plus file and database read-write helpers for
tabular and geospatial workflows.

The PyPI distribution is named `ga-gataframe`; the import package is named
`gataframe`.

## Installation

```bash
python -m pip install ga-gataframe
```

Development and test tools are available as extras:

```bash
python -m pip install -e ".[test]"
python -m pip install -e ".[dev]"
```

Optional parallel cleanup helpers are available with:

```bash
python -m pip install -e ".[parallel]"
```

## Quick Start

```python
import pandas as pd

from gataframe import connect

engine = connect(extensions=None)

gf = engine.read(pd.DataFrame({"id": [1, 2], "value": [10, 20]}))
result = gf.withColumn("double_value", "value * 2").filter("double_value > 20").toPandas()

assert result["double_value"].tolist() == [40]

engine.close()
```

## GataFrame

`GataFrame` wraps a `duckdb.DuckDBPyRelation` and keeps operations chainable.
Most methods return a new `GataFrame` unless `inplace=True` is passed.

```python
import pandas as pd

from gataframe import connect

engine = connect(extensions=None)
gf = engine.read(pd.DataFrame({"a": [1, 2], "b": [3, 4]}))

out = (
    gf.withColumn("total", "a + b")
    .renameColumn("total", "sum_ab")
    .replaceColumn("sum_ab", "sum_ab * 10")
    .select("a", "sum_ab")
)

assert out.columns == ["a", "sum_ab"]
```

Common methods include:

- `withColumn(name, expression)` and `replaceColumn(name, expression)`
- `renameColumn(old, new)` and `excludeColumn(*columns)`
- `select(*columns)`, `filter(expression)`, and `limit(n)`
- `createTable(...)`, `createView(...)`, `dropTable(...)`, and `dropView(...)`
- `toPandas()`, `toGeoPandas(...)`, and `toPandasOrGeoPandas(...)`

## Engine

`Engine` owns the DuckDB connection and handles input/output.

```python
from gataframe import connect

engine = connect(extensions=None, file_based=False)

gf = engine.read("data.csv", header=True)
engine.write(gf, "out.parquet", mode="overwrite")

engine.close()
```

Supported readers and writers are inferred from file extensions where possible:
CSV, JSON, Parquet, GeoParquet, GeoJSON, GeoPackage, Shapefile, SQLite, and
PostgreSQL connection URLs.

DuckDB extensions are loaded only when requested by the caller or needed by a
specific geospatial/database operation.

## API Summary

- `connect(logger=None, extensions=None, options=None, file_based=False, file=None)`
- `read(engine, source, format=None, pre_limit=None, limit=None, ...)`
- `write(engine, df, destination, mode="overwrite", partitionBy=None, ...)`
- `Engine.connect(...)`
- `Engine.read(source, format=None, ...)`
- `Engine.write(df, destination, mode="overwrite", ...)`
- `GataFrame.withColumn(...)`, `replaceColumn(...)`, `renameColumn(...)`
- `GataFrame.select(...)`, `filter(...)`, `limit(...)`, `union(...)`
- `GataFrame.toPandas()`, `toGeoPandas(...)`, and `toPandasOrGeoPandas(...)`

## Development

GataFrame supports Python 3.10 and newer.

```bash
python -m pip install -e ".[dev]"
python -m compileall -q src
python -m pytest --cov=gataframe --cov-report=term-missing
ruff format --check .
ruff check .
mypy
python -m pip check
python -m build
python -m twine check dist/*
```

## GitHub Repository Setup

This project is prepared for the future repository `andreagemma/gataframe`.

1. Create the empty repository on GitHub.
2. Initialize the local repository if needed and push the project to `main`.
3. Confirm the CI workflow passes on GitHub.
4. Configure the PyPI Trusted Publisher for project `ga-gataframe`, owner
   `andreagemma`, repository `gataframe`, workflow `release.yml`, and
   environment `pypi`.

## Releases

`src/gataframe/_version.py` is the only version source. To publish a release:

1. Update `__version__` in `_version.py` and commit the release changes.
2. Push `main` and wait for CI to pass.
3. Run the **Create release** GitHub Actions workflow. With no override it
   creates the `v<version>` tag, creates release notes, and dispatches the build
   and PyPI publication workflow.

PyPI versions are immutable. Increment `_version.py` before publishing different
content.

## License

GataFrame is distributed under the MIT License. See [LICENSE](LICENSE).
