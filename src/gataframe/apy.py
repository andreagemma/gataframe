from .engine import Engine
from .gata_frame import GataFrame
from .data_schema import DataSchema
import pandas as pd
import geopandas as gpd
from pathlib import Path
from typing import Any, Sequence
import logging


def read(
    engine: Engine | None,
    source: str | Path | pd.DataFrame | GataFrame | None,
    schema: DataSchema | None = None,
    format: str | None = None,
    pre_limit: int | None = None,
    limit: int | None = None,
    pre_filter: str | None = None,
    filter: str | None = None,
    **kwargs: dict[str, Any],  # reader kwargs: header, delim, names, dtype, hive_partitioning, union_by_name, etc.
) -> GataFrame | None:
    """Read a source through an :class:`Engine` and return a :class:`GataFrame`."""
    if engine is None:
        engine = Engine.connect(extensions=None)
    return engine.read(
        source=source,
        schema=schema,
        format=format,
        pre_limit=pre_limit,
        limit=limit,
        pre_filter=pre_filter,
        filter=filter,
        **kwargs,
    )


def write(
    engine: Engine,
    df: GataFrame | pd.DataFrame | gpd.GeoDataFrame,
    destination: str | Path,
    mode: str = "overwrite",  # overwrite, append, error, truncate (db), ignore, warning
    partitionBy: str | Sequence[str] | None = None,
    pk: str | Sequence[str] | None = None,
    index: str | Sequence[str] | Sequence[str | tuple[str, str]] | None = None,
    n_partitions: Sequence[int] | int | None = None,
    geometry: str | None = "geometry",
    crs_source: str | None = None,  # source CRS for transform pipeline (optional)
    crs_target: str | None = None,  # destination CRS (transform geometry if provided)
    chunk_size: int | None = None,
    hive_partitioning: bool = True,
    format: str | None = None,
    **kwargs: dict[str, Any],  # writer kwargs (parquet options, compression, etc.) / db options
) -> None:
    """Write a dataframe-like object through an :class:`Engine`."""
    return engine.write(
        df=df,
        destination=destination,
        mode=mode,
        partitionBy=partitionBy,
        pk=pk,
        index=index,
        n_partitions=n_partitions,
        geometry=geometry,
        crs_source=crs_source,
        crs_target=crs_target,
        chunk_size=chunk_size,
        hive_partitioning=hive_partitioning,
        format=format,
        **kwargs,
    )


def connect(
    logger: logging.Logger | None = None,
    extensions: tuple[str | tuple[str, str], ...] | None = None,  # ("spatial", ("h3", "community"), "encodings"),
    options: dict[str, str | int | float | bool] | None = None,
    file_based: bool = False,
    file: str | Path | None = None,
    **kwargs: dict[str, Any],
) -> Engine:
    """Create a configured :class:`Engine` instance."""
    return Engine.connect(
        logger=logger,
        extensions=extensions,
        options=options,
        file_based=file_based,
        file=file,
        **kwargs,
    )


def create_db_string_connection(
    db_type: str = "postgresql",
    user: str = "postgres",
    password: str = "",
    host: str = "localhost",
    port: int = 5432,
    dbname: str = "postgres",
    schema: str = "public",
    table: str = "",
) -> str:
    """Create a database URL with schema and table query parameters."""
    return Engine.create_db_string_connection(
        db_type=db_type,
        user=user,
        password=password,
        host=host,
        port=port,
        dbname=dbname,
        schema=schema,
        table=table,
    )
