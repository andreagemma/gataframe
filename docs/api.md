# API Reference

## `connect`

```python
connect(logger=None, extensions=None, options=None, file_based=False, file=None)
```

Creates an `Engine`. The package-level helper defaults to `extensions=None` so
basic dataframe work does not try to install DuckDB extensions.

## `Engine`

```python
Engine.connect(...)
engine.read(source, schema=None, format=None, pre_limit=None, limit=None, ...)
engine.write(df, destination, mode="overwrite", ...)
```

`Engine` owns the DuckDB connection. It reads Pandas dataframes, `GataFrame`
instances, and supported file/database sources into DuckDB relations, then wraps
them as `GataFrame`.

Supported file formats include CSV, JSON, Parquet, GeoParquet, GeoJSON,
GeoPackage, Shapefile, SQLite, and PostgreSQL URLs.

## `GataFrame`

```python
GataFrame(relation, con=None, alias=None, description=None, version=0)
```

Wraps a `duckdb.DuckDBPyRelation` and returns chainable `GataFrame` instances.

Main methods:

- `withColumn(name, expression)` and `replaceColumn(name, expression)`
- `renameColumn(old, new)` and `excludeColumn(*columns)`
- `select(*columns)`, `filter(expression)`, and `limit(n)`
- `createTable(...)`, `createView(...)`, `dropTable(...)`, and `dropView(...)`
- `union(other)`, `sql(sql)`, and `execute(sql)`
- `as_type(columns)` and `apply_schema(schema)`
- `toPandas()`, `toGeoPandas(...)`, and `toPandasOrGeoPandas(...)`

## `DataSchema`

```python
DataSchema.model_validate({...})
DataSchema.from_json_file(path)
```

Defines schema fields, mapping rules, generated fields, filters, limits,
projections, metadata, and reader options. Types are normalized to
DuckDB-compatible strings.

Related classes:

- `SchemaField`
- `AdditionalSchemaField`
- `GeneratorSpec`
- `ReaderConfig`, `CsvReaderConfig`, `ParquetReaderConfig`,
  `GeoParquetReaderConfig`, `JsonReaderConfig`, `GpkgReaderConfig`,
  `ShpReaderConfig`

## `Serializer`

```python
Serializer.dumps(data, compression=None, clevel=5, to_string=False)
Serializer.loads(data, compression=None)
Serializer.dump(data, path, compression=None, clevel=5, to_string=False)
Serializer.load(path, compression=None)
```

Serializes Python objects with pickle-compatible behavior and optional
compression.
