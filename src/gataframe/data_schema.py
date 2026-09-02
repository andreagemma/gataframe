from __future__ import annotations

from typing import Literal, Any

import re

from pydantic import AliasChoices, BaseModel, Field, ConfigDict, field_validator, RootModel, model_validator
from .conversion_types import DUCKDB_TYPE_ALIASES

# Canonical DuckDB types and aliases.
# The "type" field of SchemaField accepts any string, but it is
# normalized to a canonical DuckDB-compatible representation using
# these aliases and simple parsing rules (including LIST/ARRAY, MAP, STRUCT).


def _normalize_duckdb_type(type_str: str) -> str:
    """Normalize a logical type/alias to a DuckDB-compatible type string.

    Supports:
    - simple aliases (string -> VARCHAR, int -> INTEGER, ...)
    - container types: LIST/ARRAY, MAP, STRUCT
      e.g. "list(int)", "array(varchar)", "map(text, int)",
           "struct(id int, name string)".
    The result is always returned in a canonical, mostly upper-case form.
    """

    s = type_str.strip()
    if not s:
        raise ValueError("type string cannot be empty")

    lower = s.lower()

    # STRUCT with field list: struct(col1 int, col2 string)
    if lower.startswith("struct"):
        start = s.find("(")
        end = s.rfind(")")
        if start == -1 or end == -1 or end <= start:
            # Bare STRUCT without fields
            return "STRUCT"
        inner = s[start + 1 : end].strip()
        # split on commas at top level (we assume no nested structs here)
        parts = [p.strip() for p in inner.split(",") if p.strip()]
        norm_parts: list[str] = []
        for part in parts:
            # expected form: "name type"
            if " " in part:
                name, type_part = part.split(None, 1)
                type_norm = _normalize_duckdb_type(type_part)
                norm_parts.append(f"{name} {type_norm}")
            else:
                # If format is unexpected, just upper-case it
                norm_parts.append(part.upper())
        return f"STRUCT({', '.join(norm_parts)})"

    # LIST / ARRAY with element type: list(int), array(varchar)
    m = re.match(r"(?i)^(list|array)\s*\((.+)\)$", s)
    if m:
        elem = m.group(2).strip()
        elem_norm = _normalize_duckdb_type(elem)
        # Canonical container name in DuckDB is LIST
        return f"LIST({elem_norm})"

    # MAP with key/value types: map(text, int)
    m = re.match(r"(?i)^map\s*\((.+),(.+)\)$", s)
    if m:
        key_raw = m.group(1).strip()
        val_raw = m.group(2).strip()
        key_norm = _normalize_duckdb_type(key_raw)
        val_norm = _normalize_duckdb_type(val_raw)
        return f"MAP({key_norm}, {val_norm})"

    # Bare container names without inner types
    if lower in ("list", "array"):
        return "LIST"
    if lower == "map":
        return "MAP"
    if lower == "struct":
        return "STRUCT"

    # Simple (non-container) type: use alias table, then upper-case fallback
    canon = DUCKDB_TYPE_ALIASES.get(lower)
    if canon is not None:
        return canon

    # Fallback: return upper-cased token, assuming it is already a valid type
    return s.upper()


# Public annotation for field.type: keep as str | None per gestire campi
# opzionali (es. "skip": true senza type), normalizzati dal validator.
FieldType = str | None

SchemaType = Literal["struct"]


class ReaderConfig(BaseModel):
    """Configurazione generica del reader.

    Rimane come fallback, mentre per i singoli formati si usano
    le sottoclassi CsvReaderConfig, ParquetReaderConfig, ecc.
    """

    # Formato sorgente (csv, parquet, json, shp, geojson, ...)
    format: str | None = None
    # Limite righe da leggere (passato come nrows/limit al reader)
    nrows: int | None = None
    # Separatore per CSV
    sep: str | None = None

    # Permettiamo chiavi extra per opzioni specifiche di DuckDB.read_* (dtype, names, ecc.)
    model_config = ConfigDict(extra="allow")


class CsvReaderConfig(ReaderConfig):
    """Reader per sorgenti CSV."""

    # format fissato a "csv" a livello di valore (tipo ereditato: str | None)
    format: str | None = "csv"
    header: bool | int | None = None
    nrows: int | None = None
    sep: str | None = None
    encoding: str | None = None


class ParquetReaderConfig(ReaderConfig):
    """Reader per file Parquet (incl. .parquet, .pq)."""

    format: str | None = "parquet"
    nrows: int | None = None


class GeoParquetReaderConfig(ReaderConfig):
    """Reader per file GeoParquet."""

    format: str | None = "geoparquet"
    nrows: int | None = None


class JsonReaderConfig(ReaderConfig):
    """Reader per file JSON."""

    format: str | None = "json"
    nrows: int | None = None
    encoding: str | None = None


class GpkgReaderConfig(ReaderConfig):
    """Reader per GeoPackage (GPKG/Geopackage)."""

    format: str | None = "gpkg"
    # Layer opzionale; se assente, l'engine usa il nome file
    layer: str | None = None


class ShpReaderConfig(ReaderConfig):
    """Reader per Shapefile (SHP)."""

    format: str | None = "shp"


class FieldMetadata(RootModel[dict[str, Any]]):
    """Metadati aggiuntivi per il singolo campo.

    Può contenere QUALSIASI struttura chiave/valore annidata, senza
    vincoli sul contenuto, purché sia un dict JSON-like.
    Esempi validi:

    - {"description": "...", "unit": "km/h"}
    - {"tz": "Europe/Rome", "format": "%Y-%m-%d"}
    - {"mapping": {"1": "car", "2": "truck"}}
    - {"extra": {"a": 1, "b": [1, 2, 3]}}
    """

    root: dict[str, Any] = Field(default_factory=dict)


class StructField(BaseModel):
    """Definizione di un singolo campo interno a un tipo STRUCT.

    Riusa la stessa logica di SchemaField ma in forma ridotta, per
    descrivere i sotto-campi di un STRUCT DuckDB.
    """

    name: str
    type: FieldType
    nullable: bool | None = True
    metadata: FieldMetadata = Field(default_factory=FieldMetadata)


class GeneratorSpec(BaseModel):
    """Definizione della generazione di un campo.

    Il campo `generator` nello schema può essere:
    - una stringa SQL/expression
    - un dizionario con almeno `expr` e opzionalmente `type`
    """

    expr: str
    type: FieldType = None

    model_config = ConfigDict(extra="allow")

    @field_validator("type")
    @classmethod
    def _normalize_type(cls, v: FieldType) -> FieldType:
        if v is None:
            return v
        return _normalize_duckdb_type(v)


class SchemaField(BaseModel):
    """Definizione di un campo di schema compatibile con example_schema.json.

    È pensata per riflettere sia i campi "fields" che "additional_fields".
    """

    metadata: FieldMetadata = Field(default_factory=FieldMetadata)
    name: str | None = None
    nullable: bool = True
    type: FieldType = None
    type_width: int | None = None
    type_precision: int | None = None
    # Campi opzionali usati nell'esempio
    skip: bool = False
    format: str | list[str] | None = None
    tz: str | None = None
    decimal_sep: str | None = None
    thousand_sep: str | None = None
    # Il JSON può usare sia "generator" sia "generator:" per retrocompatibilità.
    generator: str | GeneratorSpec | None = Field(
        default=None,
        validation_alias=AliasChoices("generator", "generator:"),
    )
    default: int | float | bool | str | None = None

    # Personalizzazioni per tipi complessi DuckDB
    # - Per LIST/ARRAY: tipo dell'elemento
    element_type: FieldType = None
    # - Per MAP: tipo chiave/valore
    key_type: FieldType = None
    value_type: FieldType = None
    # - Per STRUCT: elenco dei sotto-campi strutturati
    struct_fields: list[StructField] | None = None

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    @model_validator(mode="after")
    def _validate_type_specific_options(self) -> "SchemaField":
        """Applica vincoli tra tipo logico e opzioni disponibili.

        - tz/format: solo per TIMESTAMP / TIMESTAMPTZ
        - decimal_sep: solo per tipi floating (DOUBLE/REAL/DECIMAL/FLOAT)
        - element_type: solo per LIST/ARRAY
        - key_type/value_type: solo per MAP
        - struct_fields: solo per STRUCT
        - se nullable=False, default deve essere valorizzato
        """

        t = (self.type or "").upper()

        if self.nullable is False and self.default is None:
            raise ValueError("default non puo essere None quando nullable è False")

        # timestamp/timestamptz options
        if self.tz is not None or self.format is not None:
            if t not in ("TIMESTAMP", "TIMESTAMPTZ"):
                raise ValueError("tz/format sono ammessi solo per campi TIMESTAMP/TIMESTAMPTZ")

        # numeric decimal separator
        if self.decimal_sep is not None:
            if t not in ("DOUBLE", "REAL", "DECIMAL", "FLOAT"):
                raise ValueError("decimal_sep è ammesso solo per campi floating (DOUBLE/REAL/DECIMAL/FLOAT)")

        if self.thousand_sep is not None:
            if t not in ("DOUBLE", "REAL", "DECIMAL", "FLOAT"):
                raise ValueError("decimal_sep è ammesso solo per campi floating (DOUBLE/REAL/DECIMAL/FLOAT)")

        # list/array element type
        if self.element_type is not None:
            if not (t.startswith("LIST") or t.startswith("ARRAY")):
                raise ValueError("element_type è ammesso solo per campi LIST/ARRAY")

        # map key/value types
        if self.key_type is not None or self.value_type is not None:
            if not t.startswith("MAP"):
                raise ValueError("key_type/value_type sono ammessi solo per campi MAP")

        # struct nested fields
        if self.struct_fields is not None:
            if not t.startswith("STRUCT"):
                raise ValueError("struct_fields è ammesso solo per campi STRUCT")

        return self

    @field_validator("type")
    @classmethod
    def _normalize_type(cls, v: FieldType) -> FieldType:
        """Normalizza il tipo logico in un tipo compatibile con DuckDB.

        Esempi supportati:
        - "string", "text"           -> "VARCHAR"
        - "int", "integer", "long"  -> "INTEGER" / "BIGINT"
        - "float", "double"          -> "DOUBLE"
        - "list(int)" / "array(int)"  -> "LIST(INTEGER)"
        - "map(text, int)"             -> "MAP(VARCHAR, INTEGER)"
        - "struct(id int, name string)"-> "STRUCT(id INTEGER, name VARCHAR)"
        """

        if v is None:
            return v
        return _normalize_duckdb_type(v)


class AdditionalSchemaField(BaseModel):
    """Definizione di un campo generato in `additional_fields`.

    Per i campi aggiuntivi il metadato e opzionale, mentre la definizione del
    campo da creare deve sempre dichiarare almeno nome, nullabilita, tipo e
    generatore.
    """

    metadata: FieldMetadata = Field(default_factory=FieldMetadata)
    name: str
    type: FieldType
    expr: str = Field(validation_alias=AliasChoices("expr", "expr:"))
    element_type: FieldType = None
    key_type: FieldType = None
    value_type: FieldType = None
    struct_fields: list[StructField] | None = None

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def _validate_type_specific_options(self) -> "AdditionalSchemaField":
        t = (self.type or "").upper()

        if self.element_type is not None:
            if not (t.startswith("LIST") or t.startswith("ARRAY")):
                raise ValueError("element_type e ammesso solo per campi LIST/ARRAY")

        if self.key_type is not None or self.value_type is not None:
            if not t.startswith("MAP"):
                raise ValueError("key_type/value_type sono ammessi solo per campi MAP")

        if self.struct_fields is not None:
            if not t.startswith("STRUCT"):
                raise ValueError("struct_fields e ammesso solo per campi STRUCT")

        return self

    @field_validator("type")
    @classmethod
    def _normalize_type(cls, v: FieldType) -> FieldType:
        if v is None:
            return v
        return _normalize_duckdb_type(v)


class SchemaMetadata(BaseModel):
    provider: str | None = None
    description: str | None = None
    crs: str | dict[str, str] | None = None
    new_id: str | None = None

    model_config = ConfigDict(extra="allow")


class DataSchema(BaseModel):
    """Schema complessivo, compatibile con example_schema.json.

    Esempio di struttura supportata:

    {
      "fields": [...],
      "additional_fields": [...],
      "id": "id_fcd",
      "project": ["id_fcd", ...],
      "metadata": {...},
      "reader": {"header": false, "format": "csv"}
    }
    """

    fields: list[SchemaField] | dict[str, SchemaField] | None = None
    mapping: dict[str, str | GeneratorSpec | None] | None = None
    additional_fields: list[AdditionalSchemaField | list[AdditionalSchemaField]] = Field(default_factory=list)  # pyright: ignore[reportUnknownVariableType]
    type: SchemaType = "struct"

    # Elenco colonne da proiettare nell'ordine desiderato
    project: list[str] | None = None

    # Filtro SQL da applicare dopo la lettura, le conversioni e le trasformazioni, prima del project finale (Engine.read: filter)
    filter: str | None = None
    # Limite massimo di righe da restituire nella lettura dopo filter
    limit: int | None = None
    # Filtro SQL da applicare prima di eventuali trasformazioni prima dei generetar e additional fields (Engine.read: pre_filter)
    pre_filter: str | None = None
    # Limite massimo di righe da restituire nella lettura dopo il pre_filter
    pre_limit: int | None = None
    # Importa campi non definiti se fields
    import_others: bool = True
    # eventuale crs del dato di input
    crs: str | dict[str, str] | None = None

    metadata: SchemaMetadata = Field(default_factory=SchemaMetadata)
    # Configurazione del reader (specifica per formato o generica)
    reader: (
        CsvReaderConfig
        | ParquetReaderConfig
        | GeoParquetReaderConfig
        | JsonReaderConfig
        | GpkgReaderConfig
        | ShpReaderConfig
        | ReaderConfig
        | None
    ) = None

    model_config = ConfigDict(extra="forbid")

    @property
    def flat_additional_fields(self) -> list[AdditionalSchemaField]:
        """Restituisce `additional_fields` appiattito in una lista semplice."""

        flat: list[AdditionalSchemaField] = []
        for item in self.additional_fields:
            if isinstance(item, list):
                if not item:
                    raise ValueError("I gruppi in additional_fields non possono essere vuoti")
                flat.extend(item)
            else:
                flat.append(item)
        return flat

    @model_validator(mode="after")
    def _validate_unique_field_names(self) -> "DataSchema":
        """Verifica che non esistano nomi duplicati nei soli `fields`."""
        if self.fields is None:
            return self
        if isinstance(self.fields, list):
            seen: set[str] = set()
            duplicates: set[str] = set()
            for field in self.fields:
                if field.name is None:
                    raise ValueError("Tutti i campi in 'fields' devono avere un nome definito se fields è una lista")
                name = field.name.strip()
                if name in seen:
                    duplicates.add(name)
                else:
                    seen.add(name)

            if duplicates:
                dup_list = ", ".join(sorted(duplicates))
                raise ValueError(f"I nomi dei campi devono essere univoci. Duplicati: {dup_list}")

        return self

    @classmethod
    def from_json_file(cls, path: str) -> "DataSchema":
        """Carica uno schema da un file JSON compatibile.

        Usa il parser JSON standard; il file deve essere JSON valido.
        """

        from pathlib import Path

        text = Path(path).read_text(encoding="utf-8")
        return cls.model_validate_json(text)


if __name__ == "__main__":
    """Piccolo test manuale per la lettura di example_schema.json."""

    from pathlib import Path

    here = Path(__file__).resolve().parent
    schema_path = here / "example.schema.json"
    schema = DataSchema.from_json_file(str(schema_path))

    print(f"Schema caricato da: {schema_path}")
    if isinstance(schema.fields, list):
        print(f"Campi: {[f.name for f in schema.fields]}")
        print(f"Campi: {[f.skip for f in schema.fields]}")
        if schema.additional_fields:
            print(f"Campi aggiuntivi: {[f.name for f in schema.flat_additional_fields]}")
        if schema.reader is not None:
            print(f"Reader config: {schema.reader}")
