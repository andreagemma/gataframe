"""Public API for the GataFrame package."""

from ._version import __version__
from .apy import connect, create_db_string_connection, read, write
from .data_schema import (
    AdditionalSchemaField,
    CsvReaderConfig,
    DataSchema,
    FieldMetadata,
    GeneratorSpec,
    GeoParquetReaderConfig,
    GpkgReaderConfig,
    JsonReaderConfig,
    ParquetReaderConfig,
    ReaderConfig,
    SchemaField,
    SchemaMetadata,
    ShpReaderConfig,
    StructField,
)
from .engine import Engine
from .gata_frame import GFDataType, GataFrame

__all__ = [
    "__version__",
    "AdditionalSchemaField",
    "CsvReaderConfig",
    "DataSchema",
    "GataFrame",
    "GFDataType",
    "Engine",
    "FieldMetadata",
    "GeneratorSpec",
    "GeoParquetReaderConfig",
    "GpkgReaderConfig",
    "JsonReaderConfig",
    "ParquetReaderConfig",
    "ReaderConfig",
    "SchemaField",
    "SchemaMetadata",
    "ShpReaderConfig",
    "StructField",
    "read",
    "write",
    "connect",
    "create_db_string_connection",
]
