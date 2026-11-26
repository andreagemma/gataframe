# API Reference

The package exports its main helpers and types from `gataframe`:

```python
from gataframe import (
    Engine,
    GataFrame,
    connect,
    create_db_string_connection,
    read,
    write,
)
```

## Source and destination strings

`Engine.read(source, ...)` accepts in-memory objects as well as file paths and
database URLs. When `source` is a string or `Path`, use one of the forms below.

### File paths and directories

Pass a file path directly; the format is inferred from its extension unless
`format=` is provided:

```python
engine.read("data/roads.csv")
engine.read("data/network.parquet")
engine.read("data/boundaries.gpkg")
engine.read("data/network.shp")
```

Paths are matched case-insensitively. For a directory, specify `format=` so
the reader knows which files to find recursively. `format=` can specify a
canonical format or one of the aliases below.

| Canonical format | Accepted aliases |
| --- | --- |
| `csv` | `csv`, `txt`, `tsv` |
| `parquet` | `parquet`, `pq` |
| `geoparquet` | `geoparquet`, `gpq` |
| `geopackage` | `gpkg`, `geopackage` |
| `shp` | `shp`, `shapefile` |
| `json` | `json` |
| `geojson` | `geojson` |
| `sqlite` | `sqlite`, `sqlite3`, `db`, `db3` |

For example, `engine.read("data/roads.txt")` detects the CSV reader, and
`engine.read("data/roads.bin", format="csv")` reads a file with a non-standard
extension as CSV. Reader-specific options such as `header=True`, `delim=";"`,
or `union_by_name=True` are passed as keyword arguments.

For a GeoPackage layer, append `|layer=<layer-name>` to its path:

```python
engine.read("data/network.gpkg|layer=roads")
```

The part before `|` must be the path. The layer selector is passed to the
GeoPackage reader as its `layer` option.

### Database URLs

Database sources use URL syntax. A URL must include a scheme and must not be
the name of an existing local path.

PostgreSQL accepts `postgresql://` and `postgres://`. Select a table with
`table` and optionally `schema` (default: `public`):

```text
postgresql://<user>:<password>@<host>:<port>/<database>?schema=public&table=roads
```

Alternatively, supply a SQL statement in the `query` parameter instead of
`table`:

```text
postgresql://<user>:<password>@<host>:<port>/<database>?query=<URL-encoded-SQL>
```

SQLite database URLs use `sqlite:////` for an absolute file path. Select a
table or provide a SQL query:

```text
sqlite:////data/reference.sqlite?table=roads
sqlite:////data/reference.sqlite?query=<URL-encoded-SQL>
```

The SQLite `schema` parameter is ignored. URL-encode SQL query values and
reserved characters in credentials or parameter values. For example:

```python
from urllib.parse import urlencode

params = urlencode({"query": "SELECT * FROM roads WHERE class = 'primary'"})
source = f"sqlite:////data/reference.sqlite?{params}"
gf = engine.read(source)
```

For either database scheme, `table` or `query` is required. With PostgreSQL
URLs, `pre_filter` and `pre_limit` are pushed into the remote query. For other
sources they are applied before schema processing. The regular `filter` and
`limit` are applied after schema processing.

### In-memory sources

`Engine.read` also accepts `GataFrame`, Pandas or GeoPandas dataframes,
`dict` values, and lists of record dictionaries. Passing `None` returns `None`.

## Engine

`Engine` owns the DuckDB connection. Create one with the package helper or its
class method:

```python
engine = connect(extensions=None)
# equivalent:
engine = Engine.connect(extensions=None)
```

The class can also be constructed directly:

```python
engine = Engine(logger=None, extensions=None, options=None, file_based=True, file=None)
```

Direct construction defaults to a temporary file-backed database. In contrast,
`Engine.connect()` defaults to an in-memory database and loads spatial, H3, and
encodings extensions.

The package-level `connect` defaults to `extensions=None`. `Engine.connect`
also accepts `file_based=True` and `file=...` to use a specific DuckDB file.

### `Engine.read`

```python
engine.read(
    source,
    schema=None,
    format=None,
    pre_limit=None,
    limit=None,
    pre_filter=None,
    filter=None,
    **reader_options,
)
```

Reads an in-memory object, file, directory, or database URL and returns a
`GataFrame`. `schema` may be a `DataSchema` or dictionary. `pre_filter` and
`pre_limit` run before schema application; `filter` and `limit` run after it.
See [Source and destination strings](#source-and-destination-strings) for the
accepted path and URL syntax.

### `Engine.write`

```python
engine.write(
    df,
    destination,
    mode="overwrite",
    partitionBy=None,
    pk=None,
    index=None,
    n_partitions=None,
    geometry="geometry",
    crs_source=None,
    crs_target=None,
    hive_partitioning=True,
    format=None,
    **writer_options,
)
```

Writes a `GataFrame`, Pandas DataFrame, or GeoPandas GeoDataFrame to a file or
database. File destinations are paths; their format is inferred from the
extension unless `format=` is supplied. Supported file formats are CSV,
Parquet, GeoParquet, JSON, GeoJSON, Shapefile, GeoPackage, and SQLite. An
explicit `format` should use a writer format name: `csv`, `parquet`, `geoparquet`, `gpkg`,
`shp`, `json`, `geojson`, or `sqlite`. For GeoPackage output, pass
`format="gpkg"` explicitly; the reader's canonical format name is
`geopackage`. Path extensions can use the aliases in the reader table for
formats whose writer name matches the detected format.

GeoPackage destinations can select a layer with the same `|layer=<name>`
suffix used by the reader:

```python
engine.write(gf, "output/network.gpkg|layer=roads", format="gpkg")
```

Database destinations are PostgreSQL or SQLite URLs. Include `table` in the
query string; PostgreSQL also accepts `schema` (default: `public`). `pk` and
`index` can be passed as function arguments, or supplied as URL parameters:

```python
engine.write(
    gf,
    "postgresql://<user>:<password>@<host>:5432/<database>?schema=public&table=roads",
    pk="road_id",
)
engine.write(gf, "sqlite:////data/output.sqlite?table=roads", mode="append")
engine.write(gf, "output/network.gpkg|layer=roads", format="gpkg")
```

For file destinations, `mode` supports `overwrite`, `append`, `error`, and
`warning`. Database destinations additionally support `truncate`. `partitionBy`
selects partition columns; `hive_partitioning` controls Hive-style file
partitioning where supported, and `n_partitions` controls the requested number
of output partitions. `geometry`, `crs_source`, and `crs_target` configure
geometry serialization and optional CRS transformation. Extra keyword
arguments are forwarded to the selected writer.

### Other `Engine` methods

| Method | Description |
| --- | --- |
| `Engine.create_function(name, function, parameters, return_type, null_handling="special")` | Register a typed Python scalar function with DuckDB. |
| `Engine.create_db_string_connection(...)` | Build a PostgreSQL connection URL with optional schema and table parameters. |
| `Engine.get_schema_file(source)` | Return the conventional `<source-name>.schema.json` sidecar path. |
| `Engine.read_schema(schema_file)` | Read a `DataSchema` JSON file, or return `None` if it is absent. |
| `Engine.close()` | Close the connection and remove its temporary database file, if any. |
| `with Engine.connect(...) as engine:` | Use an engine as a context manager; it is closed on exit. |

`SourceFormatEnum.parse(value)` parses a canonical format name and returns the
corresponding enum member. Unknown values raise `ValueError`.

## Package-level functions

| Function | Description |
| --- | --- |
| `connect(logger=None, extensions=None, options=None, file_based=False, file=None)` | Create an `Engine`; extensions are not loaded by default. |
| `read(engine, source, schema=None, format=None, pre_limit=None, limit=None, pre_filter=None, filter=None, **kwargs)` | Read through an existing engine, or create a default engine when `engine` is `None`. |
| `write(engine, df, destination, mode="overwrite", **kwargs)` | Write through the supplied engine. Supports the same destination syntax as `Engine.write`; `chunk_size` is retained for compatibility. |
| `create_db_string_connection(...)` | Package-level wrapper for `Engine.create_db_string_connection`. |

## `GataFrame`

```python
GataFrame(relation, con=None, alias=None, description=None, version=0)
```

Wraps a `duckdb.DuckDBPyRelation`. Transformations return a new `GataFrame` by
default; methods that accept `inplace=True` instead update the current object.

### Properties

| Property | Description |
| --- | --- |
| `uuid` | Unique identifier for this wrapper. |
| `conection` | Backing DuckDB connection (legacy spelling). |
| `relation` | Underlying DuckDB relation. |
| `get_type` | Current relation kind: relation, table, view, or inconsistent. |
| `alias` | Table or view alias, if set. |
| `empty`, `isEmpty()` | Whether the relation has zero rows. |
| `shape` | `(row_count, column_count)`. |
| `count`, `n_rows` | Row count. |
| `n_cols` | Column count. |
| `columns` | Column names; assigning a list renames all columns. |
| `types`, `dtypes` | DuckDB types in positional or name-to-type form. |
| `pandasDType` | Best-effort mapping of column names to Pandas dtype labels. |

### Transformations and queries

| Method | Description |
| --- | --- |
| `withColumn(name, expr)` | Add a computed column; a mapping can add multiple columns. |
| `replaceColumn(name, expr)` | Replace values in existing columns with SQL expressions. |
| `renameColumn(old, new)` | Rename one column or a mapping of columns. |
| `excludeColumn(*cols)` | Remove the specified columns. |
| `project(*cols, include_others=False)` / `select(*cols)` | Select columns or expressions; `include_others=True` retains other columns and replaces matching ones. |
| `filter(filter_expr)` | Keep rows matching a DuckDB SQL predicate. |
| `limit(n)` | Limit the number of rows; `None` leaves the relation unbounded and emits a warning. |
| `distinct()` | Remove duplicate rows. |
| `orderBy(list_cols)` | Sort by expressions, `(expression, ascending)` pairs, or dictionaries with `col` and `asc` keys. |
| `sql(sql, alias=None)` | Run SQL on the backing connection and wrap its result. |
| `execute(sql)` | Execute a SQL statement for its side effects. |
| `union(right)` | Combine this relation with another GataFrame or DuckDB relation. |
| `min(col)`, `max(col)` | Return the minimum or maximum value of a column. |
| `distinctCount(col)` | Count distinct non-null values in a column. |

For the column transformation methods, `inplace=True` updates the current
object; otherwise a new wrapper is returned.

### Tables and views

| Method | Description |
| --- | --- |
| `createTable(table_name=None, replace=False, inplace=False, temporary=True)` | Materialize the relation as a temporary or persistent DuckDB table. |
| `createView(view_name=None, replace=False, inplace=False)` | Create a DuckDB view over the relation. |
| `dropTable(name=None)`, `dropView(name=None)` | Drop the named table or view; the current alias is used when omitted. |
| `dropColumn(*cols, inplace=False, table=None)` | Drop columns from the current relation or a named table. |
| `exists(name=None)` | Check whether a table or view exists. |
| `drop()` | Drop the aliased table or view represented by this object. |
| `insertInto(target_table, inplace=False)` | Append this relation's rows to an existing table. |
| `persist(temporary=False, force=False, inplace=False)` | Store this relation in a table named from its UUID. |
| `unpersist()` | Drop the table associated with this object's alias. |

### Conversion and schemas

| Method | Description |
| --- | --- |
| `as_type(columns, inplace=False)` | Cast selected columns using DuckDB types or per-column conversion options. |
| `toPandas()` | Convert the relation to a Pandas DataFrame; geometry columns become WKT text. |
| `toGeoPandas(geometry="geometry", crs="EPSG:4326")` | Convert a WKT geometry column to an active GeoPandas geometry column. |
| `toPandasOrGeoPandas(...)` | Return a DataFrame without geometry, or a GeoDataFrame when geometry columns are present. |
| `get_schema(fields_attributes=None, metadata=None, reader=None)` | Build a `DataSchema` from the relation and optional metadata. |
| `apply_schema(schema, inplace=False)` | Apply schema mappings, conversions, generated fields, filters, and projections. |
| `printSchema(logger=None)` | Display column names and DuckDB types. |
| `explain(analyze=False, logger=None)` | Display the DuckDB query plan, optionally with execution analysis. |
| `show(max_rows=10, logger=None)` | Display relation rows, optionally through a logger. |

`GataFrame.get_new_alias(prefix="t_")` creates a unique alias.
`GataFrame.qident(name, cols=None)` quotes an SQL identifier, and
`GataFrame.qname(*parts)` joins quoted identifier components.
`GFDataType.parse(value)` parses a supported GataFrame type name.

## Schema models

The package also exports Pydantic models for describing schemas and reader
options:

- `DataSchema` is the top-level schema configuration.
- `SchemaField`, `AdditionalSchemaField`, `GeneratorSpec`, `FieldMetadata`,
  `SchemaMetadata`, and `StructField` describe fields, generated values, and
  metadata.
- `ReaderConfig` and its `CsvReaderConfig`, `ParquetReaderConfig`,
  `GeoParquetReaderConfig`, `GpkgReaderConfig`, `JsonReaderConfig`, and
  `ShpReaderConfig` subclasses describe format-specific reader options.
- `GataSourceType` is the type alias for sources accepted by `Engine.read`.
