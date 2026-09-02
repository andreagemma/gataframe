import re


DUCKDB_TYPE_ALIASES: dict[str, str] = {
    # string-like
    "string": "VARCHAR",
    "str": "VARCHAR",
    "varchar": "VARCHAR",
    "char": "VARCHAR",
    "bpchar": "VARCHAR",
    "text": "VARCHAR",
    # boolean
    "bool": "BOOLEAN",
    "boolean": "BOOLEAN",
    # integers
    "tinyint": "TINYINT",
    "int1": "TINYINT",
    "smallint": "SMALLINT",
    "int2": "SMALLINT",
    "int16": "SMALLINT",
    "short": "SMALLINT",
    "int": "INTEGER",
    "integer": "INTEGER",
    "int4": "INTEGER",
    "int32": "INTEGER",
    "bigint": "BIGINT",
    "int8": "BIGINT",
    "int64": "BIGINT",
    "long": "BIGINT",
    "hugeint": "HUGEINT",
    "int128": "HUGEINT",
    "utinyint": "UTINYINT",
    "uint8": "UTINYINT",
    "usmallint": "USMALLINT",
    "uint16": "USMALLINT",
    "uinteger": "UINTEGER",
    "uint32": "UINTEGER",
    "ubigint": "UBIGINT",
    "uint64": "UBIGINT",
    "uhugeint": "UHUGEINT",
    "uint128": "UHUGEINT",
    # floating point / decimals
    "real": "FLOAT",
    "float4": "FLOAT",
    "float": "DOUBLE",
    "double": "DOUBLE",
    "float8": "DOUBLE",
    "decimal": "DECIMAL",
    "numeric": "DECIMAL",
    # temporal
    "date": "DATE",
    "time": "TIME",
    "timestamp": "TIMESTAMP",
    "timestamptz": "TIMESTAMPTZ",
    # other common types
    "uuid": "UUID",
    "blob": "BLOB",
    "binary": "BLOB",
    "geometry": "GEOMETRY",
    "json": "JSON",
}


def duckdb_type_to_postgres(dtype: str) -> str:
    t = dtype.strip().upper()

    if t == "GEOMETRY" or t.startswith("GEOMETRY("):
        return "GEOMETRY"

    # DECIMAL/NUMERIC con precisione e scala
    m = re.match(r"^(DECIMAL|NUMERIC)\((\d+)\s*,\s*(\d+)\)$", t)
    if m:
        return f"NUMERIC({m.group(2)},{m.group(3)})"

    # ARRAY esplicito DuckDB, es. INTEGER[3]
    m = re.match(r"^(.+)\[\d*\]$", t)
    if m and not t.startswith("DECIMAL"):
        base = duckdb_type_to_postgres(m.group(1))
        return f"{base}[]"

    # LIST DuckDB, es. INTEGER[] oppure LIST(INTEGER) in alcuni contesti
    m = re.match(r"^LIST\((.+)\)$", t)
    if m:
        base = duckdb_type_to_postgres(m.group(1))
        return f"{base}[]"

    # tipi complessi, meglio serializzarli in JSONB
    if t.startswith("STRUCT(") or t.startswith("MAP(") or t.startswith("UNION("):
        return "JSONB"

    mapping = {
        "BOOLEAN": "BOOLEAN",
        "BOOL": "BOOLEAN",
        "TINYINT": "SMALLINT",
        "SMALLINT": "SMALLINT",
        "INTEGER": "INTEGER",
        "INT": "INTEGER",
        "BIGINT": "BIGINT",
        "HUGEINT": "NUMERIC(38,0)",
        # PostgreSQL non ha unsigned nativi
        "UTINYINT": "SMALLINT",
        "USMALLINT": "INTEGER",
        "UINTEGER": "BIGINT",
        "UBIGINT": "NUMERIC(20,0)",
        "UHUGEINT": "NUMERIC(39,0)",
        "REAL": "REAL",
        "FLOAT": "REAL",
        "DOUBLE": "DOUBLE PRECISION",
        "DECIMAL": "NUMERIC",
        "NUMERIC": "NUMERIC",
        "VARCHAR": "TEXT",
        "CHAR": "TEXT",
        "BPCHAR": "TEXT",
        "STRING": "TEXT",
        "BLOB": "BYTEA",
        "BYTEA": "BYTEA",
        "BIT": "BIT",
        "DATE": "DATE",
        "TIME": "TIME",
        "TIME_TZ": "TIME WITH TIME ZONE",
        "TIMESTAMP": "TIMESTAMP",
        "TIMESTAMP_S": "TIMESTAMP",
        "TIMESTAMP_MS": "TIMESTAMP",
        "TIMESTAMP_NS": "TIMESTAMP",
        "TIMESTAMP_TZ": "TIMESTAMPTZ",
        "INTERVAL": "INTERVAL",
        "UUID": "UUID",
        "JSON": "JSONB",
        "SQLNULL": "TEXT",
        "NULL": "TEXT",
    }

    return mapping.get(t, "TEXT")


def duckdb_type_to_sqlite(dtype: str) -> str:
    """
    Mappa un tipo DuckDB a un tipo SQLite.
    Nota: SQLite usa type affinity, quindi questa è una mappatura pratica.
    """
    t = dtype.strip().upper()

    if t == "GEOMETRY" or t.startswith("GEOMETRY("):
        return "BLOB"

    # DECIMAL/NUMERIC(p,s)
    m = re.match(r"^(DECIMAL|NUMERIC)\((\d+)\s*,\s*(\d+)\)$", t)
    if m:
        return "NUMERIC"

    # ARRAY esplicito, es. INTEGER[3]
    m = re.match(r"^(.+)\[\d*\]$", t)
    if m and not t.startswith("DECIMAL"):
        return "TEXT"

    # LIST(...)
    m = re.match(r"^LIST\((.+)\)$", t)
    if m:
        return "TEXT"

    # tipi complessi, in SQLite conviene serializzarli
    if t.startswith("STRUCT(") or t.startswith("MAP(") or t.startswith("UNION("):
        return "TEXT"

    mapping = {
        # booleani
        "BOOLEAN": "INTEGER",
        "BOOL": "INTEGER",
        # interi
        "TINYINT": "INTEGER",
        "SMALLINT": "INTEGER",
        "INTEGER": "INTEGER",
        "INT": "INTEGER",
        "BIGINT": "INTEGER",
        # oltre 64 bit o unsigned grandi, meglio NUMERIC
        "HUGEINT": "NUMERIC",
        "UTINYINT": "INTEGER",
        "USMALLINT": "INTEGER",
        "UINTEGER": "INTEGER",
        "UBIGINT": "NUMERIC",
        "UHUGEINT": "NUMERIC",
        # floating
        "REAL": "REAL",
        "FLOAT": "REAL",
        "DOUBLE": "REAL",
        "DOUBLE PRECISION": "REAL",
        # decimali
        "DECIMAL": "NUMERIC",
        "NUMERIC": "NUMERIC",
        # stringhe
        "VARCHAR": "TEXT",
        "CHAR": "TEXT",
        "BPCHAR": "TEXT",
        "STRING": "TEXT",
        "TEXT": "TEXT",
        # binari
        "BLOB": "BLOB",
        "BYTEA": "BLOB",
        "BIT": "INTEGER",
        # date/ora: SQLite non ha tipi temporali forti, TEXT è la scelta più robusta
        "DATE": "TEXT",
        "TIME": "TEXT",
        "TIME_TZ": "TEXT",
        "TIMESTAMP": "TEXT",
        "TIMESTAMP_S": "TEXT",
        "TIMESTAMP_MS": "TEXT",
        "TIMESTAMP_NS": "TEXT",
        "TIMESTAMP_TZ": "TEXT",
        "INTERVAL": "TEXT",
        # altri
        "UUID": "TEXT",
        "JSON": "TEXT",
        "SQLNULL": "TEXT",
        "NULL": "TEXT",
    }

    return mapping.get(t, "TEXT")
