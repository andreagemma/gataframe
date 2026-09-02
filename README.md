# GataFrame

[![CI](https://github.com/andreagemma/gataframe/actions/workflows/ci.yml/badge.svg)](https://github.com/andreagemma/gataframe/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/gataframe.svg)](https://pypi.org/project/gataframe/)
[![Python](https://img.shields.io/pypi/pyversions/gataframe.svg)](https://pypi.org/project/gataframe/)

GataFrame is a lightweight Python library built around DuckDB relations. It
adds a dataframe-style wrapper, schema-driven transformations, file/database
read-write helpers, and small utilities for serialization and cleanup.

The PyPI distribution is named `gataframe`; the import package is named
`gataframe`.

## Installation

```bash
python -m pip install gataframe
```

Development and test tools are available as extras:

```bash
python -m pip install -e ".[test]"
python -m pip install -e ".[dev]"
```

Optional helpers:

```bash
python -m pip install -e ".[compression]"
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

## Schemas

`DataSchema` describes field names, DuckDB-compatible types, filters, limits,
generated fields, and projections. It can be built directly from dictionaries or
JSON files with Pydantic validation.

```python
from gataframe import DataSchema

schema = DataSchema.model_validate(
    {
        "fields": [
            {"name": "id", "type": "int", "nullable": False, "default": 0},
            {"name": "name", "type": "string"},
        ],
        "project": ["id", "name"],
    }
)
```

Type aliases such as `int`, `string`, `float`, `array(int)`, and
`map(text, int)` are normalized to DuckDB-compatible type strings.

## Serialization

`Serializer` provides pickle-compatible object serialization with optional
compression. Standard-library compression methods such as `gzip`, `bz2`, `zip`,
and `lzma` work without extra packages. Blosc, Snappy, and Dill support are
available through the `compression` extra.

```python
from gataframe import Serializer

payload = Serializer.dumps({"answer": 42}, compression="gzip")
assert Serializer.loads(payload, compression="gzip") == {"answer": 42}
```

## API Summary

- `connect(logger=None, extensions=None, options=None, file_based=False, file=None)`
- `read(engine, source, schema=None, format=None, pre_limit=None, limit=None, ...)`
- `write(engine, df, destination, mode="overwrite", partitionBy=None, ...)`
- `Engine.connect(...)`
- `Engine.read(source, schema=None, format=None, ...)`
- `Engine.write(df, destination, mode="overwrite", ...)`
- `GataFrame.withColumn(...)`, `replaceColumn(...)`, `renameColumn(...)`
- `GataFrame.select(...)`, `filter(...)`, `limit(...)`, `union(...)`
- `GataFrame.apply_schema(schema)` and `GataFrame.get_schema(...)`
- `DataSchema`, `SchemaField`, `AdditionalSchemaField`, and reader configs
- `Serializer.dumps(...)`, `loads(...)`, `dump(...)`, and `load(...)`

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
4. Configure the PyPI Trusted Publisher for project `gataframe`, owner
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
