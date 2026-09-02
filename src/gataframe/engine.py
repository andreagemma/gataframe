from __future__ import annotations
from pathlib import Path
import os
import sqlite3
import pandas as pd
import geopandas as gpd
import logging
import glob
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode, ParseResult
from pathlib import Path
import duckdb
import tempfile
from uuid import uuid4
import threading as th
from typing import Sequence, Any, Optional, Generator

from .files import clean_folder, remove_path
from .data_schema import DataSchema
from .gata_frame import GataFrame
from .conversion_types import duckdb_type_to_postgres, duckdb_type_to_sqlite


def _is_jupyter() -> bool:
    try:
        from IPython import get_ipython  # pyright: ignore[reportPrivateImportUsage]

        shell = get_ipython()
        if shell is None:
            return False
        return shell.__class__.__name__ == "ZMQInteractiveShell"
    except Exception:
        return False


def _has_ipywidgets() -> bool:
    from importlib.util import find_spec

    return find_spec("ipywidgets") is not None


class Engine:
    parallelism: int = 10

    def __init__(
        self,
        logger: logging.Logger | None = None,
        extensions: tuple[str | tuple[str, str], ...] | None = None,  # ("spatial", ("h3", "community"), "encodings"),
        options: dict[str, str | int | float | bool] | None = None,
        file_based: bool = True,
        file: str | Path | None = None,
        **kwargs: dict[str, Any],
    ):

        self.file_based: bool = file_based
        self.file: str | Path | None = file

        temp_folder: Path = Path(tempfile.gettempdir()) / "io_engine_temp"
        temp_folder.mkdir(parents=True, exist_ok=True)

        self.logger: logging.Logger = logger or logging.getLogger(__name__)

        # Ensure the current source tree is available to Spark Python workers.
        # In debug/local runs it may work via CWD, but executors spawn separate
        # Python workers that need PYTHONPATH/pyFiles to import `fcd_spark`.
        if file_based:
            if file is None:
                self.file = temp_folder / f"fcd_{uuid4().hex}.duckdb"
                self.logger.debug(f"Creating temporary DuckDB file at {self.file}")
                self.file_temp = True
            else:
                self.file = Path(file)
                self.file_temp = False
        else:
            self.file = ":memory:"
            self.file_temp = None
        self.connection: duckdb.DuckDBPyConnection | None = duckdb.connect(database=self.file)  # pyright: ignore[reportUnknownMemberType]
        # abilita progress bar
        variables: list[str] = []
        self.connection.execute("SET SESSION preserve_insertion_order = false;")
        variables.append("preserve_insertion_order")
        show_progress = os.getenv("DUCKDB_PROGRESS_SHOW", "true").lower() in ("1", "true", "yes")
        # check if ipwidgets is available and we're in a Jupyter environment, otherwise disable progress bar to avoid issues with missing ipywidgets
        ipwidgets_available = _has_ipywidgets()
        if ipwidgets_available or not _is_jupyter():
            if show_progress:
                variables.append("enable_progress_bar")
                variables.append("progress_bar_time")
                self.connection.execute("SET SESSION enable_progress_bar = true;")
                time = os.getenv("DUCKDB_PROGRESS_BAR_TIME", "10000")
                self.connection.execute(f"SET SESSION progress_bar_time = {time};")  # 10000 ms
            else:
                self.connection.execute("SET SESSION enable_progress_bar = false;")
                variables.append("enable_progress_bar")

        self.connection.execute(f"SET temp_directory = '{temp_folder}';")
        variables.append("temp_directory")
        if os.environ.get("DUCKDB_CPU_LIMIT"):
            self.connection.execute(f"SET threads = '{os.environ.get('DUCKDB_CPU_LIMIT')}';")
            variables.append("threads")
        if os.environ.get("DUCKDB_MEM_LIMIT_GB"):
            self.connection.execute(f"SET memory_limit = '{os.environ.get('DUCKDB_MEM_LIMIT_GB')}GB';")
            variables.append("memory_limit")
        if tz := os.environ.get("DUCKDB_TIMEZONE", "Etc/UTC"):
            self.connection.execute(f"SET TimeZone = '{tz}';")
            variables.append("TimeZone")
        if options:
            for k, v in options.items():
                variables.append(k)
                if isinstance(v, str):
                    v = f"'{v}'"
                if isinstance(v, bool):
                    v = "true" if v else "false"
                self.connection.execute(f"SET {k} = {v};")
        # opzionale: mostra la progress bar solo se la query dura almeno N ms
        df_vars = self.connection.sql(
            "SELECT * FROM duckdb_settings() where name in ({}) order by name".format(
                ", ".join(f"'{v}'" for v in variables)
            )
        ).df()
        for _, row in df_vars.iterrows():
            self.logger.info(f"DuckDB setting: {row['name']} = {row['value']} ({row['scope']})")
        par = self.connection.sql("SELECT current_setting('threads')")
        if par:
            row = par.fetchone()
            Engine.parallelism = row[0] if row else 1

        def fn_clean() -> None:
            removed_files: int = clean_folder(
                temp_folder,
                age=1,
                unit="days",
                time_type="mtime",
                recursive=True,
                remove_empty_dirs=True,
                n_jobs=Engine.parallelism,
            )
            if removed_files:
                self.logger.info(f"Cleaned temp folder: Removed {removed_files} files from '{temp_folder}'")

        th.Thread(target=fn_clean, daemon=True).start()
        self.engine = self
        self.loaded_extensions: dict[str | tuple[str, str], bool] = {}
        self._ensure_extensions(extensions)
        self._db_attached: dict[tuple[str, str], str] = {}  # (alias, schema) -> file/url

    def _ensure_extensions(self, extensions: tuple[str | tuple[str, str], ...] | None):
        if extensions is None:
            return
        for extension in extensions:
            if extension in self.loaded_extensions:
                continue
            try:
                if isinstance(extension, (tuple, list)):
                    if len(extension) != 2:
                        raise ValueError(f"Extension tuple must be (name, repository), got: {extension}")
                    name: str = extension[0]
                    repository: str | None = extension[1]
                    if name and repository:
                        self.connection.install_extension(extension=name, repository=repository)  # pyright: ignore[reportOptionalMemberAccess]
                        self.connection.load_extension(extension=name)  # pyright: ignore[reportOptionalMemberAccess]
                    elif name:
                        self.connection.install_extension(extension=name)  # pyright: ignore[reportOptionalMemberAccess]
                        self.connection.load_extension(extension=name)  # pyright: ignore[reportOptionalMemberAccess]
                    else:
                        raise ValueError(f"Extension tuple must include at least the name, got: {extension}")
                else:
                    self.connection.install_extension(extension=extension)  # pyright: ignore[reportOptionalMemberAccess]
                    self.connection.load_extension(extension=extension)  # pyright: ignore[reportOptionalMemberAccess]
            except Exception as e:
                raise RuntimeError(f"Failed to load extension '{extension}': {str(e)}") from e
            else:
                self.loaded_extensions[extension] = True

    def _ensure_postgres(self):
        self._ensure_extensions(("postgres",))

    def _ensure_h3(self):
        self._ensure_extensions((("h3", "community"),))

    def _ensure_spatial(self):
        self._ensure_extensions(("spatial",))

    def _ensure_sqlite_scanner(self):
        self._ensure_extensions(("sqlite_scanner",))

    def _ensure_sqlite(self):
        self._ensure_extensions(("sqlite",))

    def _ensure_encodings(self):
        self._ensure_extensions(("encodings",))

    @staticmethod
    def connect(
        logger: logging.Logger | None = None,
        extensions: tuple[str | tuple[str, str], ...] | None = ("spatial", ("h3", "community"), "encodings"),
        options: dict[str, str | int | float | bool] | None = None,
        file_based: bool = False,
        file: str | Path | None = None,
        **kwargs: dict[str, Any],
    ) -> Engine:
        return Engine(logger=logger, extensions=extensions, options=options, file_based=file_based, file=file, **kwargs)

    def __enter__(self) -> Engine:
        if self.engine is None:
            self.engine = Engine.connect()
        return self.engine

    def close(self):
        try:
            if self.connection is not None:
                self.connection.close()
                self.connection = None
                self.engine = None
        except Exception as e:
            print_info = f"Error to close DuckDB database: {str(e)}"
            self.logger.warning(f"Error to close DuckDB database {print_info}")

        try:
            if self.file_temp and self.file is not None:
                remove_path(self.file)
        except Exception as e:
            print_info = f"Error to remove temp file {self.file}: {str(e)}"
            self.logger.warning(f"Error to remove temp file {print_info}")

    def __exit__(self, exc_type: type | None, exc_value: BaseException | None, exc_tb: Any | None):
        self.close()

    # ---------------------------
    # Internal helpers
    # ---------------------------

    def _is_db_url(self, s: str) -> bool:
        if "://" not in s:
            return False
        if "|" in s:  # gpkg special
            return False
        p = Path(s)
        return not p.exists()

    def _parse_gpkg_source(self, s: str) -> tuple[str, Optional[str]]:
        if "|" not in s:
            return s, None
        a, b = s.split("|", 1)
        a = a.strip()
        b = b.strip() if b.strip() else None
        return a, b

    def _guess_format(self, p: Path) -> str:
        ext = p.suffix.lower().lstrip(".").strip()
        if ext in ("parquet", "pq"):
            return "parquet"
        if ext in ("geoparquet", "gpq"):
            return "geoparquet"
        elif ext in ("csv", "txt"):
            return "csv"
        elif ext in ("json",):
            return "json"
        elif ext in ("geojson",):
            return "geojson"
        elif ext in ("gpkg", "geopackage"):
            return "gpkg"
        elif ext in ("shp", "shapefile"):
            return "shp"
        elif ext in ("sqlite", "sqlite3", "db", "db3"):
            return "sqlite"
        else:
            return ext

    def _to_insensitive_pattern_ext(self, ext: str) -> str:
        insensitive_ext = ""
        for c in ext.lower():
            insensitive_ext += f"[{c.lower()}{c.upper()}]"
        return insensitive_ext

    def _to_insensitive_pattern(self, pattern: str | Path) -> str:
        p = Path(pattern)  # validate it's a valid path pattern
        ext = p.suffix.lower().lstrip(".").strip()
        insensitive_ext = self._to_insensitive_pattern_ext(ext)
        return str(p.with_suffix("." + insensitive_ext))

    def _to_insensitive_patterns(self, patterns: list[str]) -> list[str]:
        return [self._to_insensitive_pattern(p) for p in patterns]

    def _list_supported_files_recursive(
        self, root: Path, patterns: list[str] = ["**/*.parquet", "**/*.pq", "**/*.csv", "**/*.gpkg", "**/*.shp"]
    ) -> list[str]:
        files: list[Path] = []
        patterns = self._to_insensitive_patterns(patterns)
        for pat in patterns:
            files.extend(Path(x) for x in glob.glob(str(root / pat), recursive=True))
        return sorted({p.resolve().as_posix() for p in files if p.is_file()})

    def _kwargs_to_string(self, sep_kwargs: str = " ", quote_char: Any = "'", **kwargs: dict[str, Any]) -> str:
        if kwargs:
            params = {}
            for k, v in kwargs.items():
                if isinstance(v, str):
                    v = f"{str(quote_char)}{v}{str(quote_char)}"
                else:
                    v = str(v)
                params[k] = v
            params = f", {', '.join(f'{k}{sep_kwargs} {v}' for k, v in params.items())}"  # pyright: ignore[reportUnknownVariableType]
        else:
            params = ""
        return params

    def _read_parquet(self, src_path: str | Path, **kwargs: dict[str, Any]) -> duckdb.DuckDBPyRelation:
        self._ensure_spatial()
        kwargs.pop("file_globs", None)
        kwargs.pop("hive_partitioning", None)
        if isinstance(src_path, str):
            src_path = Path(src_path)
        if src_path.exists():
            if src_path.is_dir():
                files: Sequence[str] = self._list_supported_files_recursive(
                    src_path, patterns=["**/*.parquet", "**/*.pq", "**/*.geoparquet"]
                )
                rel: duckdb.DuckDBPyRelation = self.connection.read_parquet(
                    file_globs=files,  # pyright: ignore[reportOptionalMemberAccess]
                    hive_partitioning=True,
                    **kwargs,
                )  # pyright: ignore[reportArgumentType]

                return rel
            else:
                rel: duckdb.DuckDBPyRelation = self.connection.read_parquet(
                    file_glob=src_path.as_posix(),  # pyright: ignore[reportOptionalMemberAccess]
                    hive_partitioning=True,
                    **kwargs,
                )  # pyright: ignore[reportArgumentType]
                return rel
        else:
            raise FileNotFoundError(f"Parquet source not found: {src_path}")

    def _read_csv(self, src_path: str | Path, **kwargs: dict[str, Any]) -> duckdb.DuckDBPyRelation:
        rel: duckdb.DuckDBPyRelation | None = None  # pyright: ignore[reportRedeclaration, reportAssignmentType]
        if isinstance(src_path, str):
            src_path = Path(src_path)
        kwargs.setdefault("hive_partitioning", True)  # pyright: ignore[reportArgumentType]
        kwargs.setdefault("union_by_name", True)  # pyright: ignore[reportArgumentType]
        if src_path.exists():
            if src_path.is_dir():
                kwargs.setdefault("encoding", "utf-8")  # pyright: ignore[reportArgumentType]
                files = self._list_supported_files_recursive(src_path, patterns=["**/*.csv", "**/*.txt"])
                if str(kwargs.get("encoding")).lower() != "utf-8":
                    self._ensure_encodings()
                    params = self._kwargs_to_string(sep_kwargs="=", **kwargs)

                    for file in files:
                        new_rel = self.connection.sql(f"SELECT * FROM read_csv('{file}' {params} )")  # pyright: ignore[reportOptionalMemberAccess]
                        rel = rel.union(new_rel) if rel else new_rel
                else:
                    rel = self.connection.read_csv(files, **kwargs)  # pyright: ignore[reportOptionalMemberAccess, reportArgumentType]

                return rel
            else:
                rel: duckdb.DuckDBPyRelation = self.connection.read_csv(
                    str(src_path),  # pyright: ignore[reportOptionalMemberAccess]
                    **kwargs,
                )  # pyright: ignore[reportArgumentType]
                return rel
        else:
            raise FileNotFoundError(f"Parquet source not found: {src_path}")

    def _read_json(self, src_path: Path, **kwargs: dict[str, Any | str]) -> duckdb.DuckDBPyRelation:
        kwargs.setdefault("union_by_name", True)  # pyright: ignore[reportArgumentType]
        if src_path.exists():
            if src_path.is_dir():
                files = self._list_supported_files_recursive(src_path, patterns=["**/*.json"])
            else:
                files = [src_path]
            if len(files) == 0:
                raise FileNotFoundError(f"No supported files found in directory: {src_path}")
            rel = self.connection.read_json(files, **kwargs)  # pyright: ignore[reportOptionalMemberAccess, reportArgumentType]
            if rel:
                return rel
            raise FileNotFoundError(f"JSON source not found: {src_path}")
        else:
            raise FileNotFoundError(f"JSON source not found: {src_path}")

    def _read_geojson(self, src_path: Path, **kwargs: dict[str, Any]) -> duckdb.DuckDBPyRelation:
        self._ensure_spatial()
        if src_path.exists():
            if src_path.is_dir():
                files = self._list_supported_files_recursive(src_path, patterns=["**/*.json", "**/*.geojson"])
            else:
                files = [src_path]
            if len(files) == 0:
                raise FileNotFoundError(f"No supported files found in directory: {src_path}")
            params = self._kwargs_to_string(sep_kwargs=" ", **kwargs)

            rel: duckdb.DuckDBPyRelation | None = None  # pyright: ignore[reportRedeclaration, reportAssignmentType]
            for file in files:
                new_rel = self.connection.sql(f"SELECT * FROM ST_Read('{file}' {params} )")  # pyright: ignore[reportOptionalMemberAccess]
                rel = rel.union(new_rel) if rel else new_rel
            if rel is not None:
                return rel
            raise FileNotFoundError(f"GeoJSON source not found: {src_path}")
        else:
            raise FileNotFoundError(f"GeoJSON source not found: {src_path}")

    def _read_sqlite(self, src_path: Path, **kwargs: dict[str, Any]) -> duckdb.DuckDBPyRelation:
        self._ensure_spatial()
        if src_path.exists():
            if src_path.is_dir():
                files = self._list_supported_files_recursive(
                    src_path, patterns=["**/*.db", "**/*.sqlite", "**/*.sqlite3", "**/*.db3"]
                )
            else:
                files = [src_path]
            if len(files) == 0:
                raise FileNotFoundError(f"No supported files found in directory: {src_path}")
            params = self._kwargs_to_string(sep_kwargs=" ", **kwargs)

            rel: duckdb.DuckDBPyRelation | None = None  # pyright: ignore[reportRedeclaration, reportAssignmentType]
            for file in files:
                new_rel = self.connection.sql(f"SELECT * FROM ST_Read('{file}' {params} )")  # pyright: ignore[reportOptionalMemberAccess]
                rel = rel.union(new_rel) if rel else new_rel
            if rel is not None:
                return rel
            raise FileNotFoundError(f"SQLite source not found: {src_path}")
        else:
            raise FileNotFoundError(f"SQLite source not found: {src_path}")

    def _read_shp(self, src_path: Path, **kwargs: dict[str, Any]) -> duckdb.DuckDBPyRelation:
        self._ensure_spatial()
        if src_path.exists():
            if src_path.is_dir():
                files = self._list_supported_files_recursive(src_path, patterns=["**/*.shp"])
            else:
                files = [src_path]
            if len(files) == 0:
                raise FileNotFoundError(f"No supported files found in directory: {src_path}")
            params = self._kwargs_to_string(sep_kwargs=" ", **kwargs)

            rel: duckdb.DuckDBPyRelation | None = None  # pyright: ignore[reportRedeclaration, reportAssignmentType]
            for file in files:
                new_rel = self.connection.sql(f"SELECT * FROM ST_ReadSHP('{file}' {params} )")  # pyright: ignore[reportOptionalMemberAccess]
                rel = rel.union(new_rel) if rel else new_rel
            if rel is not None:
                return rel
            raise FileNotFoundError(f"SHP source not found: {src_path}")
        else:
            raise FileNotFoundError(f"SHP source not found: {src_path}")

    def _read_gpkg(self, src_path: str | Path, **kwargs: dict[str, Any | str]) -> duckdb.DuckDBPyRelation:
        self._ensure_spatial()
        if isinstance(src_path, str):
            src_path = Path(src_path)
        src_path, layername = self._parse_gpkg_source(src_path.as_posix())  # pyright: ignore[reportAssignmentType]
        src_path = Path(src_path)
        if layername and str(layername).strip().lower().startswith("layer"):
            if "=" in layername:
                kwargs.setdefault("layer", layername.split("=")[1].strip())  # pyright: ignore[reportArgumentType]
        if src_path.exists():
            if src_path.is_dir():
                files = self._list_supported_files_recursive(src_path, patterns=["**/*.gpkg", "**/*.geopackage"])
            else:
                files = [src_path]
            if len(files) == 0:
                raise FileNotFoundError(f"No supported files found in directory: {src_path}")
            params = self._kwargs_to_string(sep_kwargs=" ", **kwargs)
            rel: duckdb.DuckDBPyRelation | None = None  # pyright: ignore[reportRedeclaration, reportAssignmentType]
            for file in files:
                new_rel = self.connection.sql(f"SELECT * FROM ST_Read('{file}' {params} )")  # pyright: ignore[reportOptionalMemberAccess]
                rel = rel.union(new_rel) if rel else new_rel
            if rel is not None:
                return rel
            raise FileNotFoundError(f"GeoPackage source not found: {src_path}")
        else:
            raise FileNotFoundError(f"GeoPackage source not found: {src_path}")

    def _read_postgres(
        self, src: str, pre_filter: str | None = None, pre_limit: int | None = None
    ) -> duckdb.DuckDBPyRelation:
        self._ensure_postgres()
        self._ensure_spatial()
        # DuckDB expects parameters as a comma-separated string, while parse_qs returns lists
        db_scheme, db_params, db_parsed = self._db_params(src)

        table = db_params.get("table", [None])[0]
        schema = db_params.get("schema", ["public"])[0]
        sql_query = db_params.get("query", [None])[0]

        if db_scheme in ("postgres", "postgresql"):
            self._ensure_postgres()
            # Strip helper params from URL query for ATTACH
            for k in ("table", "schema", "query", "pk"):
                db_params.pop(k, None)
            clean_url = urlunparse(db_parsed._replace(query=urlencode(db_params, doseq=True)))
            attach_alias = self._db_attached.get((clean_url, ""), None)
            if attach_alias is None:
                attach_alias = f"pg_{len(self._db_attached) + 1}"
                self.connection.execute(f"ATTACH '{clean_url}' AS {attach_alias} (TYPE postgres);")  # pyright: ignore[reportOptionalMemberAccess]
                self._db_attached[(clean_url, "")] = attach_alias
            if pre_filter is not None:
                if sql_query:
                    tmp_view = GataFrame.get_new_alias()
                    sql_query = f"with {tmp_view} as ({sql_query}) select * from {tmp_view} WHERE {pre_filter} "
                elif not table:
                    raise ValueError("DB url must include ?table=... or ?query=...")
                else:
                    sql_query = f"select * from {GataFrame.qname(schema, table)} WHERE {pre_filter}"
                if pre_limit is not None:
                    sql_query += f" LIMIT {pre_limit}"
            elif pre_limit is not None:
                if sql_query:
                    tmp_view = GataFrame.get_new_alias()
                    sql_query = f"with {tmp_view} as ({sql_query}) select * from {tmp_view} "
                elif not table:
                    raise ValueError("DB url must include ?table=... or ?query=...")
                else:
                    sql_query = f"select * from {GataFrame.qname(schema, table)}"
                sql_query += f" LIMIT {pre_limit}"

            if sql_query:
                escaped_query = sql_query.replace("'", "''")
                return self.connection.sql(f"SELECT * FROM postgres_query('{attach_alias}', '{escaped_query}');")  # pyright: ignore[reportOptionalMemberAccess]
            if not table:
                raise ValueError("DB url must include ?table=... or ?query=...")
            if schema:
                return self.connection.sql(f'SELECT * FROM {attach_alias}."{schema}"."{table}"')  # pyright: ignore[reportOptionalMemberAccess]
            else:
                return self.connection.sql(f'SELECT * FROM {attach_alias}."{table}"')  # pyright: ignore[reportOptionalMemberAccess]
        else:
            raise ValueError(f"Unsupported database scheme: {db_scheme}")

    def _read_sqlite_db(self, src: str) -> duckdb.DuckDBPyRelation:
        self._ensure_sqlite()
        self._ensure_spatial()
        # DuckDB expects parameters as a comma-separated string, while parse_qs returns lists
        db_scheme, db_params, db_parsed = self._db_params(src)

        table = db_params.get("table", [None])[0]
        # per SQLite ignoriamo un eventuale parametro "schema" e usiamo sempre lo schema interno 'main'
        db_params.pop("schema", None)
        sql_query = db_params.get("query", [None])[0]

        if db_scheme == "sqlite":
            self._ensure_sqlite()

            # sqlite:////path/to.db
            sqlite_path = db_parsed.path
            if os.name == "nt" and sqlite_path.startswith("/"):
                sqlite_path = sqlite_path.lstrip("/")

            # Strip helper params from URL query for ATTACH
            for k in ("table", "schema", "query", "pk"):
                db_params.pop(k, None)

            # crea fisicamente il file SQLite se non esiste
            db_file_key = sqlite_path
            if sqlite_path:
                sqlite_file = Path(sqlite_path)
                sqlite_file.parent.mkdir(parents=True, exist_ok=True)
                if not sqlite_file.exists():
                    sqlite_file.touch()

            attach_alias = self._db_attached.get((db_file_key, ""), None)
            if attach_alias is None:
                attach_alias = f"sq_{len(self._db_attached) + 1}"
                # ATTACH usa il path del file, non l'URL sqlite://
                self.connection.execute(f"ATTACH '{sqlite_path}' AS {attach_alias} (TYPE sqlite);")  # pyright: ignore[reportOptionalMemberAccess]
                self._db_attached[(db_file_key, "")] = attach_alias

            if sql_query:
                settings = self.connection.sql(
                    "SELECT value FROM duckdb_settings() WHERE name = 'search_path';"
                ).fetchone()  # pyright: ignore[reportOptionalSubscript, reportOptionalMemberAccess]
                if settings and settings[0]:
                    old_search_path: str | None = settings[0]
                else:
                    old_search_path: str | None = None
                try:
                    self.connection.execute(f"SET search_path = '{attach_alias}';")  # pyright: ignore[reportOptionalMemberAccess]
                    rel = self.connection.sql(sql_query)  # pyright: ignore[reportOptionalMemberAccess]
                except Exception as e:
                    raise e
                finally:
                    if old_search_path is not None:
                        self.connection.execute(f"SET search_path = '{old_search_path}';")  # pyright: ignore[reportOptionalMemberAccess]
                return rel
            elif not table:
                raise ValueError("DB url must include ?table=... or ?query=...")
            else:
                # per coerenza con la scrittura, le tabelle SQLite vengono viste come
                # "attach_alias"."main"."table"; DuckDB consente di omettere 'main'
                return self.connection.sql(f'SELECT * FROM "{attach_alias}"."main"."{table}"')  # pyright: ignore[reportOptionalMemberAccess]
        else:
            raise ValueError(f"Unsupported database scheme: {db_scheme}")

    def _db_params(
        self, url: str
    ) -> tuple[
        str,
        dict[
            str,
            list[str],
        ],
        ParseResult,
    ]:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        scheme = (parsed.scheme or "").lower()
        return scheme, params, parsed

    @staticmethod
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
        if db_type in {"postgresql", "postgres"}:
            conn_str = f"{db_type}://{user}:{password}@{host}:{port}/{dbname}"
            options = []
            if schema:
                options.append(f"schema={schema}")
            if table:
                options.append(f"table={table}")
            if options:
                conn_str += "?" + "&".join(options)
            return conn_str
        raise ValueError(f"Unsupported database scheme: {db_type}")

    def read(
        self,
        source: str | Path | pd.DataFrame | GataFrame | None,
        schema: DataSchema | None = None,
        format: str | None = None,
        pre_limit: int | None = None,
        limit: int | None = None,
        pre_filter: str | None = None,
        filter: str | None = None,
        **kwargs: dict[str, Any],  # reader kwargs: header, delim, names, dtype, hive_partitioning, union_by_name, etc.
    ) -> GataFrame | None:

        self.logger.debug(f"Read {source}")
        rel: GataFrame | None = None
        if source is None:
            return None
        assert self.connection is not None, "Database connection is not initialized"
        assert self.engine is not None, "Engine is not initialized"
        assert isinstance(source, (str, Path, pd.DataFrame, GataFrame, gpd.GeoDataFrame)), (
            f"Unsupported source type: {type(source)}"
        )
        # Already a relation
        if isinstance(source, GataFrame):
            rel = source
        # DataFrames
        elif isinstance(source, pd.DataFrame):
            self.logger.debug("Converting Pandas DataFrame to DuckDB relation")
            rel = GataFrame(self.connection.from_df(source), self.connection)  # pyright: ignore[reportOptionalMemberAccess]
        elif gpd is not None and isinstance(source, gpd.GeoDataFrame):
            self.logger.debug("Converting GeoDataFrame to DuckDB relation")
            rel = GataFrame(self.connection.from_df(source), self.connection)  # pyright: ignore[reportOptionalMemberAccess]
        else:
            src_str = str(source)
            src_path = Path(src_str.split("|", 1)[0])  # for gpkg "path|layer"

            # DB URL
            if self._is_db_url(src_str):
                db_scheme, _, _ = self._db_params(src_str)
                if db_scheme in ("postgres", "postgresql"):
                    rel = GataFrame(
                        self._read_postgres(src_str), self.connection, pre_filter=pre_filter, pre_limit=pre_limit
                    )
                    pre_filter, pre_limit = None, None
                elif db_scheme == "sqlite":
                    rel = GataFrame(self._read_sqlite_db(src_str), self.connection)
                else:
                    raise ValueError(f"Unsupported database scheme: {db_scheme}")
            elif not src_path.exists():
                raise FileNotFoundError(f"Source not found: {src_path}")
            # Directory: efficient multi-file reads where possible
            elif src_path.exists():
                format = self._guess_format(src_path) if format is None else format.lower().strip()
                if format in ("parquet",):
                    rel = GataFrame(self._read_parquet(src_path, **kwargs), self.connection)
                elif format in ("geoparquet",):
                    rel = GataFrame(self._read_parquet(src_path, **kwargs), self.connection)
                elif format in ("csv",):
                    rel = GataFrame(self._read_csv(src_path, **kwargs), self.connection)
                elif format in ("shp",):
                    rel = GataFrame(self._read_shp(src_path, **kwargs), self.connection)
                elif format in ("gpkg"):
                    rel = GataFrame(self._read_gpkg(src_str, **kwargs), self.connection)
                elif format in ("json",):
                    rel = GataFrame(self._read_json(src_path, **kwargs), self.connection)
                elif format in ("geojson",):
                    rel = GataFrame(self._read_geojson(src_path, **kwargs), self.connection)
                elif format in ("sqlite",):
                    rel = GataFrame(self._read_sqlite(src_path, **kwargs), self.connection)
                else:
                    raise ValueError(f"Unsupported format: {format}")

        assert rel is not None, "Failed to read source into a relation"

        if pre_filter:
            rel.filter(pre_filter, inplace=True)
        if pre_limit is not None:
            rel.limit(int(pre_limit), inplace=True)
        # explicit schema
        if schema is not None:
            rel = rel.apply_schema(schema)

        # filter/limit after schema application, to ensure they work on the final column names/types
        if filter:
            rel.filter(filter, inplace=True)
        if limit is not None:
            rel.limit(int(limit), inplace=True)

        return rel

    @staticmethod
    def get_schema_file(source: str | Path) -> Optional[Path]:
        source = Path(source)
        return source.parent / (source.name + ".schema.json")

    @staticmethod
    def read_schema(schema_file: str | Path) -> Optional[DataSchema]:
        schema_file = Path(schema_file)
        if not schema_file.exists():
            return None
        return DataSchema.from_json_file(schema_file.as_posix())

    def _strip_helper_params(self, url: str) -> str:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        for k in ("table", "schema", "query", "user", "password", "pk"):
            qs.pop(k, None)
        return urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))

    def _rel_with_geometry_and_crs(
        self,
        rel: duckdb.DuckDBPyRelation,
        geometry: str | None,
        crs_target: str | None,
        crs_source: str | None = None,
    ) -> duckdb.DuckDBPyRelation:
        if geometry is None:
            return rel
        if crs_source is None:
            return rel
        if crs_target is None:
            return rel

        self._ensure_spatial()

        crs_source = crs_source.strip().upper() or "OGC:CRS84"
        crs_target = crs_target.strip().upper() or "OGC:CRS84"
        if crs_source == "EPSG:4326":
            crs_source = "OGC:CRS84"
        if crs_target == "EPSG:4326":
            crs_target = "OGC:CRS84"
        # if geometry already has SRID, transform using (geom, 'src','dst') pipeline as you used
        if crs_source != crs_target:
            return rel.project(f"""* REPLACE (
                    ST_Transform({geometry}, '{crs_source}', '{crs_target}') AS {geometry}
                )""")
        else:
            return rel

    def _manage_mode_file(self, mode: str, dest_path: Path):
        if dest_path.exists():
            if mode == "error":
                raise FileExistsError(f"Destination exists: {dest_path}")
            if mode == "ignore":
                return
            if mode == "warning":
                self.logger.warning(f"Destination exists, proceeding (mode=warning): {dest_path}")
            if mode.startswith("overwrite"):
                if dest_path.is_dir():
                    remove_path(dest_path)
                else:
                    if dest_path.suffix.lower() == ".shp":
                        # Shapefile is a set of files with same stem and different extensions; delete them all
                        files = dest_path.parent.glob(dest_path.stem + ".*")
                        files = [
                            f
                            for f in files
                            if f.is_file()
                            and f.suffix.lower()
                            in (
                                ".shp",
                                ".shx",
                                ".dbf",
                                ".prj",
                                ".cpg",
                                ".sbn",
                                ".sbx",
                                ".fbn",
                                ".fbx",
                                ".ain",
                                ".aih",
                                ".atx",
                            )
                            or str(f).endswith(".shp.xml")
                        ]
                        remove_path(files)
                    else:
                        dest_path.unlink()
            if mode == "append":
                # ok, but only meaningful for directory outputs
                pass

    def _table_exists(
        self,
        database: str,
        table: str,
        schema: str = "public",
        sa_connection: Any | None = None,
    ) -> bool:
        """Check table existence.

        If ``sa_connection`` is provided, use SQLAlchemy inspection; otherwise fall
        back to DuckDB's ``duckdb_tables()`` metadata (current behaviour).
        """
        # SQLAlchemy branch (used for external DBs like SQLite/SpatiaLite)
        if sa_connection is not None:
            try:
                from sqlalchemy import inspect  # type: ignore[import]
            except Exception as e:  # pragma: no cover - optional dependency
                raise RuntimeError("SQLAlchemy is required when passing sa_connection to _table_exists") from e

            inspector = inspect(sa_connection)
            # For SQLite we typically ignore database/schema and rely only on table
            return bool(inspector.has_table(table_name=table))

        # Default DuckDB-based implementation
        if self.connection is None:
            raise Exception("Connection required")
        ret = self.connection.execute(
            """
            select exists (
                select 1
                from duckdb_tables()
                where database_name = ?
                and schema_name = ?
                and table_name = ?
                and table_name not in ('SpatialIndex', 'ElementaryGeometries')
            )
        """,
            [database, schema, table],
        ).fetchone()
        if ret:
            return ret[0]
        else:
            raise Exception("Error checking table existence")

    def _manage_mode_db(
        self, mode: str, database: str, table: str, schema: str = "public", sa_connection: Any | None = None
    ):
        """Handle destination table according to ``mode``.

        When ``sa_connection`` is provided, operations are executed via SQLAlchemy
        (used for SQLite/SpatiaLite). Otherwise, the previous DuckDB-based
        behaviour is preserved.
        """
        # SQLAlchemy branch
        if sa_connection is not None:
            try:
                from sqlalchemy import text  # type: ignore[import]
            except Exception as e:  # pragma: no cover - optional dependency
                raise RuntimeError("SQLAlchemy is required when passing sa_connection to _manage_mode_db") from e

            dest_path = f'"{table}"' if not schema else f'"{schema}"."{table}"'
            exists = self._table_exists(database=database, schema=schema, table=table, sa_connection=sa_connection)
            if exists:
                if mode == "error":
                    raise FileExistsError(f"Destination exists: {dest_path}")
                if mode == "ignore":
                    return False
                if mode == "warning":
                    self.logger.warning(f"Destination exists, proceeding (mode=warning): {dest_path}")
                    return False
                if mode == "truncate":
                    sa_connection.execute(text(f"DELETE FROM {dest_path};"))
                    return False
                if mode.startswith("overwrite"):
                    sa_connection.execute(text(f"DROP TABLE IF EXISTS {dest_path};"))
                    return True
                if mode == "append":
                    return False
            else:
                return True

        # Default DuckDB-based behaviour
        if self.connection is None:
            raise Exception("Connection is required")
        dest_path = f'"{database}"."{schema}"."{table}"'
        exists = self._table_exists(database=database, schema=schema, table=table)
        if exists:
            if mode == "error":
                raise FileExistsError(f"Destination exists: {dest_path}")
            if mode == "ignore":
                return False
            if mode == "warning":
                self.logger.warning(f"Destination exists, proceeding (mode=warning): {dest_path}")
                return False
            if mode == "truncate":
                self.connection.execute(f"TRUNCATE {dest_path};")
                return False
            if mode.startswith("overwrite"):
                self.connection.execute(f"DROP TABLE IF EXISTS {dest_path};")
                return True
            if mode == "append":
                pass
        else:
            return True

    def _get_sequence(self, seq: str | Sequence[str] | None) -> Sequence[str]:
        if isinstance(seq, str):
            return seq.split(",")
        if seq is None:
            return []
        return seq

    def _get_partition_clause(self, partitionBy: str | Sequence[str] | None) -> str:
        part_cols = self._get_sequence(partitionBy)
        part_clause: str = ""
        if part_cols:
            cols_sql = ", ".join(f'"{c.strip(chr(34))}"' for c in part_cols)
            part_clause = f", PARTITION_BY ({cols_sql})"
        return part_clause

    def _get_partition_clause_with_star(self, partitionBy: str | Sequence[str] | None, **kwargs: dict[str, Any]) -> str:
        partitionBy = self._get_sequence(partitionBy)
        tmp: list[Any] = [v for k, v in kwargs.items() if k.upper() == "PER_THREAD_OUTPUT"]
        if tmp:
            per_thread_output = str(tmp[0])
        else:
            per_thread_output = "TRUE"
        tmp: list[Any] = [v for k, v in kwargs.items() if k.upper() == "FILE_SIZE_BYTES"]
        if tmp:
            file_size_bytes = str(tmp[0])
        else:
            file_size_bytes = "128M"
        if partitionBy and (len(partitionBy) == 1 and partitionBy[0] == "*"):
            part_clause = f", PER_THREAD_OUTPUT {per_thread_output}, FILE_SIZE_BYTES '{file_size_bytes}'"
        else:
            part_clause = self._get_partition_clause(partitionBy)
        return part_clause

    def _get_partition_clause_with_star_gdal(
        self, partitionBy: str | Sequence[str] | None, **kwargs: dict[str, Any]
    ) -> str:
        partitionBy = self._get_sequence(partitionBy)
        tmp: list[Any] = [v for k, v in kwargs.items() if k.upper() == "PER_THREAD_OUTPUT"]
        if tmp:
            per_thread_output = str(tmp[0])
        else:
            per_thread_output = "TRUE"
        if partitionBy and (len(partitionBy) == 1 and partitionBy[0] == "*"):
            part_clause = f", PER_THREAD_OUTPUT {per_thread_output}"
        else:
            part_clause = self._get_partition_clause(partitionBy)
        return part_clause

    def _iter_partitioned_relations(
        self,
        rel: duckdb.DuckDBPyRelation,
        con: duckdb.DuckDBPyConnection,
        partitionBy: Sequence[str] | str | None,
        base_path: str | Path = "",
        remove_partition_cols: bool = True,
    ) -> Generator[tuple[Path, duckdb.DuckDBPyRelation], None, None]:
        """
        Genera coppie (folder, filtered_rel) per tutte le combinazioni delle colonne
        di partizione indicate in partitionBy.

        Parameters
        ----------
        rel : duckdb.DuckDBPyRelation
            Relazione di input.
        con : duckdb.DuckDBPyConnection
            Connessione DuckDB da usare per interrogare i valori distinti.
        partitionBy : Sequence[str]
            Colonne di partizione, in ordine gerarchico.
        base_path : str | Path, default ""
            Cartella base da cui partire.

        Yields
        ------
        tuple[Path, duckdb.DuckDBPyRelation]
            Cartella corrispondente alla combinazione e relazione filtrata.
        """
        root = Path(base_path)

        partitionBy_parsed: Sequence[str] | None = self._get_sequence(partitionBy)
        if not bool(partitionBy_parsed):
            yield root, rel
            return
        part_cols = [c.strip() for c in partitionBy_parsed if c and c.strip()]

        def _sql_literal(value: Any) -> str:
            if value is None:
                return "NULL"
            if isinstance(value, bool):
                return "TRUE" if value else "FALSE"
            if isinstance(value, (int, float)):
                return str(value)
            return "'" + str(value).replace("'", "''") + "'"

        def _folder_name(col: str, value: Any) -> str:
            value_str = "__HIVE_DEFAULT_PARTITION__" if value is None else str(value)
            return f"{col}={value_str}"

        def _recurse(
            current_rel: duckdb.DuckDBPyRelation,
            remaining_cols: Sequence[str],
            current_path: Path,
            remove_partition_cols: bool = True,
        ) -> Generator[tuple[Path, duckdb.DuckDBPyRelation], None, None]:
            if not remaining_cols:
                yield current_path, current_rel
                return

            part_col = remaining_cols[0]

            tmp_view = GataFrame.get_new_alias("rel_")
            con.register(tmp_view, current_rel)
            try:
                values = con.execute(f'''
                    SELECT DISTINCT "{part_col}"
                    FROM {tmp_view}
                    ORDER BY "{part_col}"
                ''').fetchall()
            finally:
                con.unregister(tmp_view)

            for row in values:
                part_value = row[0]

                if part_value is None:
                    filtered_rel = current_rel.filter(f'"{part_col}" IS NULL')
                else:
                    filtered_rel = current_rel.filter(f'"{part_col}" = {_sql_literal(part_value)}')
                if remove_partition_cols:
                    filtered_rel = filtered_rel.project(f'* EXCLUDE("{part_col}")')

                next_path = current_path / _folder_name(part_col, part_value)

                yield from _recurse(
                    current_rel=filtered_rel,
                    remaining_cols=remaining_cols[1:],
                    current_path=next_path,
                    remove_partition_cols=remove_partition_cols,
                )

        yield from _recurse(rel, part_cols, root, remove_partition_cols=remove_partition_cols)

    def _write_parquet(
        self,
        rel: duckdb.DuckDBPyRelation,
        con: duckdb.DuckDBPyConnection,
        destination: str | Path,
        mode: str,
        partitionBy: str | Sequence[str] | None = None,
        geometry: str | None = None,
        crs_source: str | None = None,  # source CRS for transform pipeline (optional)
        crs_target: str | None = None,  # destination CRS (transform geometry if provided)
        hive_partitioning: bool = True,
        **kwargs: dict[str, Any],
    ):
        fmt = "PARQUET"
        kwargs.setdefault("compression", "ZSTD")  # pyright: ignore[reportArgumentType]
        tmp_view = GataFrame.get_new_alias("rel_")
        dest_path = Path(destination)
        self._manage_mode_file(mode, dest_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        params = self._kwargs_to_string(sep_kwargs=" ", **kwargs)
        part_clause = self._get_partition_clause_with_star(partitionBy if hive_partitioning else None)
        copy_mode = ", APPEND" if mode == "append" else ", OVERWRITE_OR_IGNORE"
        con.register(tmp_view, rel)
        con.execute(f"""
            COPY (SELECT * FROM {tmp_view})
            TO '{dest_path.as_posix()}'
            (FORMAT {fmt.upper()} {copy_mode} {params} {part_clause})
        """)
        con.unregister(tmp_view)

    def _write_csv(
        self,
        rel: duckdb.DuckDBPyRelation,
        con: duckdb.DuckDBPyConnection,
        destination: str | Path,
        mode: str,
        partitionBy: str | Sequence[str] | None = None,
        geometry: str | None = None,
        crs_source: str | None = None,  # source CRS for transform pipeline (optional)
        crs_target: str | None = None,  # destination CRS (transform geometry if provided)
        hive_partitioning: bool = True,
        **kwargs: dict[str, Any],
    ):
        fmt = "CSV"
        tmp_view = GataFrame.get_new_alias("rel_")
        dest_path = Path(destination)
        self._manage_mode_file(mode, dest_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        params = self._kwargs_to_string(sep_kwargs=" ", **kwargs)
        part_clause = self._get_partition_clause_with_star(partitionBy if hive_partitioning else None)
        copy_mode = ", APPEND" if mode == "append" else ", OVERWRITE_OR_IGNORE"
        if geometry and geometry in rel.columns:
            rel = rel.project(f'* REPLACE(ST_AsText("{geometry}") as "{geometry}")')
        con.register(tmp_view, rel)
        con.execute(f"""
            COPY (SELECT * FROM {tmp_view})
            TO '{dest_path.as_posix()}'
            (FORMAT {fmt.upper()} {copy_mode} {params} {part_clause})
        """)
        con.unregister(tmp_view)

    def _write_json(
        self,
        rel: duckdb.DuckDBPyRelation,
        con: duckdb.DuckDBPyConnection,
        destination: str | Path,
        mode: str,
        partitionBy: str | Sequence[str] | None = None,
        geometry: str | None = None,
        crs_source: str | None = None,  # source CRS for transform pipeline (optional)
        crs_target: str | None = None,  # destination CRS (transform geometry if provided)
        hive_partitioning: bool = True,
        **kwargs: dict[str, Any],
    ):
        fmt = "JSON"
        tmp_view = GataFrame.get_new_alias("rel_")
        dest_path = Path(destination)
        self._manage_mode_file(mode, dest_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        params = self._kwargs_to_string(sep_kwargs=" ", **kwargs)
        partitionBy = self._get_sequence(partitionBy)
        part_clause = self._get_partition_clause_with_star(partitionBy if hive_partitioning else None)
        copy_mode = ", APPEND" if mode == "append" else ", OVERWRITE_OR_IGNORE"
        if geometry and geometry in rel.columns:
            rel = rel.project(f'* REPLACE(ST_AsText("{geometry}") as "{geometry}")')
        if len(partitionBy) == 0 or (len(partitionBy) == 1 and partitionBy[0] == "*"):
            con.register(tmp_view, rel)
            con.execute(f"""
                COPY (SELECT * FROM {tmp_view})
                TO '{dest_path.as_posix()}'
                (FORMAT {fmt.upper()} {copy_mode} {params} {part_clause})
            """)
            con.unregister(tmp_view)
        else:
            copy_mode = ", APPEND"
            for folder, rel_part in self._iter_partitioned_relations(
                rel=rel, con=con, partitionBy=partitionBy, base_path=dest_path, remove_partition_cols=True
            ):
                folder.mkdir(parents=True, exist_ok=True)
                name = GataFrame.get_new_alias("")
                con.register(tmp_view, rel_part)
                con.execute(f"""
                    COPY (SELECT * FROM {tmp_view})
                    TO '{(folder / name).with_suffix(".json").as_posix()}'
                    (FORMAT {fmt.upper()} {copy_mode} {params})
                """)
                con.unregister(tmp_view)

    def _write_geojson(
        self,
        rel: duckdb.DuckDBPyRelation,
        con: duckdb.DuckDBPyConnection,
        destination: str | Path,
        mode: str,
        partitionBy: str | Sequence[str] | None = None,
        geometry: str | None = None,
        crs_source: str | None = None,  # source CRS for transform pipeline (optional)
        crs_target: str | None = None,  # destination CRS (transform geometry if provided)
        hive_partitioning: bool = True,
        **kwargs: dict[str, Any],
    ):
        fmt = "GeoJSON"
        tmp_view = GataFrame.get_new_alias("rel_")
        dest_path = Path(destination)
        self._manage_mode_file(mode, dest_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        params = self._kwargs_to_string(sep_kwargs="=", quote_char="", **kwargs)
        if params:
            params = f", LAYER_CREATION_OPTIONS '{params}'"
        partitionBy = self._get_sequence(partitionBy)
        # part_clause = self._get_partition_clause_with_star_gdal(partitionBy if hive_partitioning else None)
        copy_mode = ", APPEND" if mode == "append" else ", OVERWRITE_OR_IGNORE"
        if len(partitionBy) == 0 or (len(partitionBy) == 1 and partitionBy[0] == "*"):
            con.register(tmp_view, rel)
            con.execute(f"""
                COPY (SELECT * FROM {tmp_view})
                TO '{dest_path.as_posix()}'
                (FORMAT GDAL, Driver '{fmt}', SRS '{crs_target or "OGC:CRS84"}' {copy_mode} {params}, FILE_EXTENSION 'geojson')
            """)
            con.unregister(tmp_view)
        else:
            copy_mode = ""
            for folder, rel_part in self._iter_partitioned_relations(
                rel=rel,
                con=con,
                partitionBy=partitionBy,
                base_path=dest_path,
                remove_partition_cols=False,
            ):
                folder.mkdir(parents=True, exist_ok=True)
                name = GataFrame.get_new_alias("")
                con.register(tmp_view, rel_part)
                con.execute(f"""
                    COPY (SELECT * FROM {tmp_view})
                    TO '{(folder / name).with_suffix(".geojson").as_posix()}'
                    (FORMAT GDAL, Driver '{fmt}', SRS '{crs_target or "OGC:CRS84"}' {copy_mode} {params})
                """)
                con.unregister(tmp_view)

    def _write_shp(
        self,
        rel: duckdb.DuckDBPyRelation,
        con: duckdb.DuckDBPyConnection,
        destination: str | Path,
        mode: str,
        partitionBy: str | Sequence[str] | None = None,
        geometry: str | None = None,
        crs_source: str | None = None,  # source CRS for transform pipeline (optional)
        crs_target: str | None = None,  # destination CRS (transform geometry if provided)
        hive_partitioning: bool = True,
        **kwargs: dict[str, Any],
    ):
        fmt = "Esri Shapefile"
        tmp_view = GataFrame.get_new_alias("rel_")
        dest_path = Path(destination)
        self._manage_mode_file(mode, dest_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        kwargs.setdefault("SPATIAL_INDEX", "YES")  # pyright: ignore[reportArgumentType]
        kwargs.setdefault("RESIZE", "YES")  # pyright: ignore[reportArgumentType]
        params = self._kwargs_to_string(sep_kwargs="=", quote_char="", **kwargs)
        if params:
            params = f", LAYER_CREATION_OPTIONS '{params}'"
        partitionBy = self._get_sequence(partitionBy)
        # part_clause = self._get_partition_clause_with_star_gdal(partitionBy if hive_partitioning else None)
        copy_mode = ", APPEND" if mode == "append" else ", OVERWRITE_OR_IGNORE"
        if len(partitionBy) == 0 or (len(partitionBy) == 1 and partitionBy[0] == "*"):
            con.register(tmp_view, rel)
            con.execute(f"""
                COPY (SELECT * FROM {tmp_view})
                TO '{dest_path.as_posix()}'
                (FORMAT GDAL, Driver '{fmt}', SRS '{crs_target or "OGC:CRS84"}' {copy_mode} {params}, FILE_EXTENSION '.shp')
            """)
            con.unregister(tmp_view)
        else:
            copy_mode = ""
            for folder, rel_part in self._iter_partitioned_relations(
                rel=rel,
                con=con,
                partitionBy=partitionBy,
                base_path=dest_path,
                remove_partition_cols=False,
            ):
                folder.mkdir(parents=True, exist_ok=True)
                name = GataFrame.get_new_alias("")
                con.register(tmp_view, rel_part)
                con.execute(f"""
                    COPY (SELECT * FROM {tmp_view})
                    TO '{(folder / name).with_suffix(".shp").as_posix()}'
                    (FORMAT GDAL, Driver '{fmt}', SRS '{crs_target or "OGC:CRS84"}' {copy_mode} {params})
                """)
                con.unregister(tmp_view)

    def _write_gpkg(
        self,
        rel: duckdb.DuckDBPyRelation,
        con: duckdb.DuckDBPyConnection,
        destination: str | Path,
        mode: str,
        partitionBy: str | Sequence[str] | None = None,
        geometry: str | None = None,
        crs_source: str | None = None,  # source CRS for transform pipeline (optional)
        crs_target: str | None = None,  # destination CRS (transform geometry if provided)
        hive_partitioning: bool = True,
        **kwargs: dict[str, Any],
    ):
        fmt = "GPKG"
        tmp_view = GataFrame.get_new_alias("rel_")
        dest_path = Path(destination)
        dest_path, layername = self._parse_gpkg_source(dest_path.as_posix())  # pyright: ignore[reportAssignmentType]
        dest_path = Path(dest_path)
        self._manage_mode_file(mode, dest_path)
        if layername and str(layername).strip().lower().startswith("layer"):
            if "=" in layername:
                kwargs.setdefault("layer", layername.split("=")[1].strip())  # pyright: ignore[reportArgumentType]
        kwargs.setdefault("SPATIAL_INDEX", "YES")  # pyright: ignore[reportArgumentType]
        kwargs.setdefault("GEOMETRY_NAME", geometry)  # pyright: ignore[reportArgumentType]
        params = self._kwargs_to_string(sep_kwargs="=", quote_char="", **kwargs)
        if params:
            params = f", LAYER_CREATION_OPTIONS '{params}'"
        partitionBy = self._get_sequence(partitionBy)
        # part_clause = self._get_partition_clause_with_star_gdal(partitionBy if hive_partitioning else None)
        copy_mode = ", APPEND" if mode == "append" else ", OVERWRITE_OR_IGNORE"
        if len(partitionBy) == 0 or (len(partitionBy) == 1 and partitionBy[0] == "*"):
            con.register(tmp_view, rel)
            con.execute(f"""
                COPY (SELECT * FROM {tmp_view})
                TO '{dest_path.as_posix()}'
                (FORMAT GDAL, Driver '{fmt}', SRS '{crs_target or "OGC:CRS84"}' {copy_mode} {params}, FILE_EXTENSION 'gpkg')
            """)
            con.unregister(tmp_view)
        else:
            copy_mode = ""
            for folder, rel_part in self._iter_partitioned_relations(
                rel=rel,
                con=con,
                partitionBy=partitionBy,
                base_path=dest_path,
                remove_partition_cols=False,
            ):
                folder.mkdir(parents=True, exist_ok=True)
                name = GataFrame.get_new_alias("")
                con.register(tmp_view, rel_part)
                con.execute(f"""
                    COPY (SELECT * FROM {tmp_view})
                    TO '{(folder / name).with_suffix(".gpkg").as_posix()}'
                    (FORMAT GDAL, Driver '{fmt}', SRS '{crs_target or "OGC:CRS84"}' {copy_mode} {params})
                """)
                con.unregister(tmp_view)

    def _write_sqlite(
        self,
        rel: duckdb.DuckDBPyRelation,
        con: duckdb.DuckDBPyConnection,
        destination: str,
        mode: str,
        partitionBy: str | Sequence[str] | None = None,
        geometry: str | None = None,
        crs_source: str | None = None,  # source CRS for transform pipeline (optional)
        crs_target: str | None = None,  # destination CRS (transform geometry if provided)
        hive_partitioning: bool = True,
        **kwargs: dict[str, Any],
    ):
        fmt = "SQLite"
        tmp_view = GataFrame.get_new_alias("rel_")
        dest_path = Path(destination)
        dest_path, layername = self._parse_gpkg_source(dest_path.as_posix())  # pyright: ignore[reportAssignmentType]
        dest_path = Path(dest_path)
        self._manage_mode_file(mode, dest_path)
        if layername and str(layername).strip().lower().startswith("layer"):
            if "=" in layername:
                kwargs.setdefault("layer", layername.split("=")[1].strip())  # pyright: ignore[reportArgumentType]
        kwargs.setdefault("SPATIAL_INDEX", "YES")  # pyright: ignore[reportArgumentType]
        kwargs.setdefault("GEOMETRY_NAME", geometry)  # pyright: ignore[reportArgumentType]
        params = self._kwargs_to_string(sep_kwargs="=", quote_char="", **kwargs)
        if params:
            params = f", LAYER_CREATION_OPTIONS '{params}'"
        partitionBy = self._get_sequence(partitionBy)
        # part_clause = self._get_partition_clause_with_star_gdal(partitionBy if hive_partitioning else None)
        copy_mode = ", APPEND" if mode == "append" else ", OVERWRITE_OR_IGNORE"
        if len(partitionBy) == 0 or (len(partitionBy) == 1 and partitionBy[0] == "*"):
            con.register(tmp_view, rel)
            con.execute(f"""
                COPY (SELECT * FROM {tmp_view})
                TO '{dest_path.as_posix()}'
                (FORMAT GDAL, Driver '{fmt}', SRS '{crs_target or "OGC:CRS84"}' {copy_mode} {params}, FILE_EXTENSION 'sqlite')
            """)
            con.unregister(tmp_view)
        else:
            copy_mode = ""
            for folder, rel_part in self._iter_partitioned_relations(
                rel=rel,
                con=con,
                partitionBy=partitionBy,
                base_path=dest_path,
                remove_partition_cols=False,
            ):
                folder.mkdir(parents=True, exist_ok=True)
                name = GataFrame.get_new_alias("")
                con.register(tmp_view, rel_part)
                con.execute(f"""
                    COPY (SELECT * FROM {tmp_view})
                    TO '{(folder / name).with_suffix(".sqlite").as_posix()}'
                    (FORMAT GDAL, Driver '{fmt}', SRS '{crs_target or "OGC:CRS84"}' {copy_mode} {params})
                """)
                con.unregister(tmp_view)

    def _write_db_sqlite_old(
        self,
        rel: duckdb.DuckDBPyRelation,
        con: duckdb.DuckDBPyConnection,
        destination: str,
        mode: str,
        partitionBy: str | Sequence[str] | None = None,
        pk: Sequence[str] | str | None = None,
        index: str | Sequence[str] | Sequence[str | tuple[str, str]] | None = None,
        n_partitions: Sequence[int] | int | None = None,
        geometry: str | None = None,
        crs_source: str | None = None,  # source CRS for transform pipeline (optional)
        crs_target: str | None = None,  # destination CRS (transform geometry if provided)
        **kwargs: dict[str, Any],
    ):
        self._ensure_sqlite()
        # se c'è una colonna geometrica vogliamo che l'estensione spatial sia caricata
        if geometry is not None and geometry in list(rel.columns):
            self._ensure_spatial()

        # parametri della connessione
        db_scheme, db_params, db_parsed = self._db_params(destination)

        table = db_params.pop("table", [None])[0]
        # per SQLite non esistono schemi logici: ignoriamo l'eventuale parametro "schema"
        db_params.pop("schema", None)

        pk = pk or db_params.pop("pk", [None])[0]
        index = index or db_params.pop("index", None)
        if index is not None:
            # normalizza come in _write_db_postgres
            if isinstance(index, str):
                index = [index]
            if isinstance(index, tuple):
                index = list(index)
            for i, idx in enumerate(index):
                if isinstance(idx, list):
                    if len(idx) != 2:
                        raise ValueError("index has been str, list[str] or list[str|tuple[str,str]]")
                    index[i] = tuple(idx)  # pyright: ignore

        # non supportiamo query di origine per la scrittura
        db_params.pop("query", None)

        if db_scheme != "sqlite":
            raise ValueError(f"Unsupported database scheme for SQLite writer: {db_scheme}")

        def create_spatialite_db(db_path: str):
            try:
                # Remove existing DB if needed (for demo purposes)
                if os.path.exists(db_path):
                    os.remove(db_path)

                # Connect to SQLite database
                conn = sqlite3.connect(db_path)

                # Load SpatiaLite extension
                try:
                    conn.enable_load_extension(True)
                    # The extension name may vary depending on OS and installation
                    conn.load_extension("mod_spatialite")
                except sqlite3.OperationalError as e:
                    conn.close()
                    raise Exception("Error loading SpatiaLite extension:", e)

                cur = conn.cursor()

                # Initialize spatial metadata
                cur.execute("SELECT InitSpatialMetadata(1);")

                conn.commit()
                conn.close()

            except Exception as e:
                raise Exception("Unexpected error:", e)

        # assicuriamoci che il file sqlite esista; in caso contrario creiamolo
        sqlite_path = db_parsed.path
        if os.name == "nt" and sqlite_path.startswith("/"):
            sqlite_path = sqlite_path.lstrip("/")
        if sqlite_path:
            sqlite_file = Path(sqlite_path)
            sqlite_file.parent.mkdir(parents=True, exist_ok=True)
            if not sqlite_file.exists():
                create_spatialite_db(sqlite_file.as_posix())

        # ATTACH per SQLite deve usare il path del file, non l'URL sqlite://
        db_file_key = sqlite_path
        attach_alias = self._db_attached.get((db_file_key, ""), None)
        if attach_alias is None:
            attach_alias = f"sq_{len(self._db_attached) + 1}"
            con.execute(f"ATTACH '{sqlite_path}' AS {attach_alias} (TYPE sqlite);")  # pyright: ignore[reportOptionalMemberAccess]
            self._db_attached[(db_file_key, "")] = attach_alias

        if not table:
            raise ValueError("DB url must include ?table=... or ?query=...")

        # SQLite usa di fatto uno schema 'main' interno; per la gestione esistenza
        # ci appoggiamo a duckdb_tables() come per Postgres
        schema_name = "main"

        # gestisce modalità overwrite/append/error/warning/truncate
        create_table = self._manage_mode_db(mode=mode, database=attach_alias, schema=schema_name, table=table)

        # helper per quoting degli identificatori
        rel_cols = list(rel.columns)
        rel_types = list(rel.types)
        rel_colset = set(rel_cols)

        def qident(name: str, cols: set[str] | list[str] | None = None) -> str:
            if cols is None:
                return '"' + str(name).replace('"', '""') + '"'
            if name.strip() in cols:
                return '"' + str(name).replace('"', '""') + '"'
            return name

        def safe_table_suffix(value: Any) -> str:
            s = str(value)
            return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in s)

        # nome fully-qualified della tabella remota
        dest = f'"{attach_alias}"."{schema_name}"."{table}"'

        # definizione tabella (senza partizioni, solo PK e tipi SQLite)
        if create_table:
            pk_cols = list(self._get_sequence(pk)) if pk is not None else []

            ddl_cols_parts: list[str] = []
            for col, dtype in zip(rel_cols, rel_types):
                col_type = duckdb_type_to_sqlite(str(dtype))
                ddl_cols_parts.append(f"{qident(col)} {col_type}")

            # chiave primaria
            if pk_cols:
                pk_list = ", ".join(qident(c, rel_cols) for c in pk_cols)
                ddl_cols_parts.append(f"PRIMARY KEY ({pk_list})")

            ddl_cols = ",\n".join(ddl_cols_parts)
            con.execute(f"CREATE TABLE {dest} (\n{ddl_cols}\n);")

        # scrittura dei dati
        temp_view: str = GataFrame.get_new_alias("rel_")
        con.register(temp_view, rel)
        con.execute(f"INSERT INTO {dest} SELECT * FROM {temp_view}")
        con.unregister(temp_view)

        # costruzione indici: da partitionBy (usato come suggerimento) e da 'index'
        ddl_indexes: list[str] = []

        # 1) indice sulle colonne di partizione (se richieste)
        part_cols = [c for c in self._get_sequence(partitionBy) if c in rel_colset]
        if create_table and part_cols:
            index = list(index) if index is not None else []
            index.append(",".join(part_cols))  # pyright: ignore[reportOptionalMemberAccess]
            # idx_name = f"{table}_" + "_".join(safe_table_suffix(c) for c in part_cols) + "_idx"
            # cols_sql = ", ".join(qident(c, rel_cols) for c in part_cols)
            # ddl_indexes.append(f"CREATE INDEX {qident(idx_name)} ON {dest} ({cols_sql});")

        # 2) indici aggiuntivi espliciti
        def normalize_index_seq(
            idx: Sequence[str | tuple[str, str]] | str | tuple[str, str] | None,
        ) -> list[str | tuple[str, str]]:
            if idx is None:
                return []
            if isinstance(idx, str):
                return [idx]
            if isinstance(idx, tuple):
                return [idx]  # pyright: ignore[reportReturnType]
            return list(idx)

        def build_additional_indexes(
            idx_specs: list[str | tuple[str, str]],
            rel_colset: set[str],
            dest: str,
            table: str,
        ) -> list[str]:
            rel_colset_lower = {c.lower() for c in rel_colset}
            ddls: list[str] = []
            used_names: set[str] = set()

            for spec in idx_specs:
                if isinstance(spec, str):
                    col = spec
                else:
                    if len(spec) != 2:
                        raise ValueError(f"Specifica indice non valida: {spec}. Atteso ('colonna', 'tipo_indice').")
                    col, _ = spec

                col_str = str(col)
                is_col = col_str.strip().lower() in rel_colset_lower

                # nome indice safe
                safe_col = col_str.replace(",", "_").replace(" ", "")
                idx_name_base = f"{table}_{safe_table_suffix(safe_col)}_idx"
                idx_name = idx_name_base
                i = 1
                while idx_name in used_names:
                    idx_name = f"{idx_name_base}_{i}"
                    i += 1
                used_names.add(idx_name)

                # se è una colonna semplice, la quotiamo, altrimenti lasciamo l'espressione così com'è
                if is_col:
                    col_sql = qident(col_str, rel_cols)
                else:
                    col_sql = col_str

                ddls.append(f"CREATE INDEX {qident(idx_name)} ON {dest} ({col_sql});")

            return ddls

        idx_specs = normalize_index_seq(index)
        if create_table and idx_specs:
            ddl_indexes.extend(
                build_additional_indexes(
                    idx_specs=idx_specs,
                    rel_colset=rel_colset,
                    dest=dest,
                    table=table,
                )
            )

        for sql in ddl_indexes:
            con.execute(sql)

    def _write_db_sqlite(
        self,
        rel: duckdb.DuckDBPyRelation,
        con: duckdb.DuckDBPyConnection,
        destination: str,
        mode: str,
        partitionBy: str | Sequence[str] | None = None,
        pk: Sequence[str] | str | None = None,
        index: str | Sequence[str] | Sequence[str | tuple[str, str]] | None = None,
        n_partitions: Sequence[int] | int | None = None,
        geometry: str | None = None,
        crs_source: str | None = None,  # source CRS for transform pipeline (optional)
        crs_target: str | None = None,  # destination CRS (transform geometry if provided)
        **kwargs: dict[str, Any],
    ):
        """Scrittura verso SQLite/SpatiaLite usando SQLAlchemy / (Geo)Pandas.

        Rispetto alla versione precedente:
        - non utilizza più ATTACH di DuckDB;
        - gestisce ``mode`` tramite _manage_mode_db con connessione SQLAlchemy;
        - crea la tabella (se necessario) via SQLAlchemy;
        - scrive i dati con pandas / geopandas;
        - crea eventuali indici tramite SQLAlchemy.
        """

        self._ensure_sqlite()
        # se c'è una colonna geometrica vogliamo che l'estensione spatial sia caricata
        has_geometry: bool = geometry is not None and geometry in list(rel.columns)
        if has_geometry:
            self._ensure_spatial()

        # parametri della connessione
        db_scheme, db_params, db_parsed = self._db_params(destination)

        table = db_params.pop("table", [None])[0]
        # per SQLite non esistono schemi logici: ignoriamo l'eventuale parametro "schema"
        db_params.pop("schema", None)

        pk = pk or db_params.pop("pk", [None])[0]
        index = index or db_params.pop("index", None)
        if index is not None:
            # normalizza come in _write_db_postgres
            if isinstance(index, str):
                index = [index]
            if isinstance(index, tuple):
                index = list(index)
            for i, idx in enumerate(index):
                if isinstance(idx, list):
                    if len(idx) != 2:
                        raise ValueError("index has been str, list[str] or list[str|tuple[str,str]]")
                    index[i] = tuple(idx)  # pyright: ignore

        # non supportiamo query di origine per la scrittura
        db_params.pop("query", None)

        if db_scheme != "sqlite":
            raise ValueError(f"Unsupported database scheme for SQLite writer: {db_scheme}")

        # creazione iniziale del file SpatiaLite (se non esiste)
        def create_spatialite_db(db_path: str):
            try:
                # Connect to SQLite database
                conn = sqlite3.connect(db_path)

                # Load SpatiaLite extension
                try:
                    conn.enable_load_extension(True)
                    # The extension name may vary depending on OS and installation
                    conn.load_extension("mod_spatialite")
                except sqlite3.OperationalError as e:
                    conn.close()
                    raise Exception("Error loading SpatiaLite extension:", e)

                cur = conn.cursor()

                # Initialize spatial metadata
                cur.execute("SELECT InitSpatialMetadata(1);")

                conn.commit()
                conn.close()

            except Exception as e:
                raise Exception("Unexpected error:", e)

        # assicuriamoci che il file sqlite esista; in caso contrario creiamolo
        sqlite_path = db_parsed.path
        if os.name == "nt" and sqlite_path.startswith("/"):
            sqlite_path = sqlite_path.lstrip("/")
        if not sqlite_path:
            raise ValueError("Invalid SQLite path in destination URL")

        sqlite_file = Path(sqlite_path)
        sqlite_file.parent.mkdir(parents=True, exist_ok=True)
        if not sqlite_file.exists():
            create_spatialite_db(sqlite_file.as_posix())

        if not table:
            raise ValueError("DB url must include ?table=... or ?query=...")

        # Import SQLAlchemy solo qui per non renderlo hard‑dependency alla import del modulo
        try:
            from sqlalchemy import (
                create_engine,
                MetaData,
                Table,
                Column,
                text,
                Integer,
                Float,
                Numeric,
                Text,
                LargeBinary,
                Engine,
            )
        except Exception as e:  # pragma: no cover - opzionale
            raise RuntimeError("SQLAlchemy is required for _write_db_sqlite") from e

        # helper per quoting degli identificatori e per gli indici
        rel_cols = list(rel.columns)
        rel_types = list(rel.types)
        rel_colset = set(rel_cols)

        def qident(name: str, cols: set[str] | list[str] | None = None) -> str:
            if cols is None:
                return '"' + str(name).replace('"', '""') + '"'
            if name.strip() in cols:
                return '"' + str(name).replace('"', '""') + '"'
            return name

        def safe_table_suffix(value: Any) -> str:
            s = str(value)
            return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in s)

        def normalize_index_seq(
            idx: Sequence[str | tuple[str, str]] | str | tuple[str, str] | None,
        ) -> list[str | tuple[str, str]]:
            if idx is None:
                return []
            if isinstance(idx, str):
                return [idx]
            if isinstance(idx, tuple):
                return [idx]  # pyright: ignore[reportReturnType]
            return list(idx)

        def build_additional_indexes(
            idx_specs: list[str | tuple[str, str]],
            rel_colset: set[str],
            dest: str,
            table: str,
        ) -> list[str]:
            rel_colset_lower = {c.lower() for c in rel_colset}
            ddls: list[str] = []
            used_names: set[str] = set()

            for spec in idx_specs:
                if isinstance(spec, str):
                    col = spec
                else:
                    if len(spec) != 2:
                        raise ValueError(f"Specifica indice non valida: {spec}. Atteso ('colonna', 'tipo_indice').")
                    col, _ = spec

                col_str = str(col)
                is_col = col_str.strip().lower() in rel_colset_lower

                # nome indice safe
                safe_col = col_str.replace(",", "_").replace(" ", "")
                idx_name_base = f"{table}_{safe_table_suffix(safe_col)}_idx"
                idx_name = idx_name_base
                i = 1
                while idx_name in used_names:
                    idx_name = f"{idx_name_base}_{i}"
                    i += 1
                used_names.add(idx_name)

                # se è una colonna semplice, la quotiamo, altrimenti lasciamo l'espressione così com'è
                if is_col:
                    col_sql = qident(col_str, rel_cols)
                else:
                    col_sql = col_str

                ddls.append(f"CREATE INDEX {qident(idx_name)} ON {dest} ({col_sql});")

            return ddls

        # connessione SQLAlchemy verso il file SQLite/SpatiaLite
        engine: Engine = create_engine(f"sqlite:///{sqlite_file.as_posix()}")  # pyright: ignore[reportUnknownVariableType]

        # Scrittura all'interno di una transazione
        with engine.begin() as sa_con:  # pyright: ignore[reportUnknownMemberType]
            # gestisce modalità overwrite/append/error/warning/truncate via SQLAlchemy
            create_table = self._manage_mode_db(
                mode=mode,
                database="",
                schema="",
                table=table,
                sa_connection=sa_con,
            )

            # definizione tabella (senza partizioni, solo PK e tipi SQLite) via SQLAlchemy
            if create_table:
                metadata = MetaData()
                pk_cols = list(self._get_sequence(pk)) if pk is not None else []

                sa_cols: list[Column] = []  # pyright: ignore[reportUnknownVariableType, reportMissingTypeArgument]
                for col, dtype in zip(rel_cols, rel_types):
                    sqlite_type = duckdb_type_to_sqlite(str(dtype)).upper()
                    if sqlite_type == "INTEGER":
                        col_type = Integer()
                    elif sqlite_type in ("REAL", "FLOAT", "DOUBLE"):
                        col_type = Float()
                    elif sqlite_type == "NUMERIC":
                        col_type = Numeric()
                    elif sqlite_type == "BLOB":
                        col_type = LargeBinary()
                    elif sqlite_type == "TEXT":
                        col_type = Text()
                    else:
                        # fallback ragionevole
                        col_type = Text()

                    is_pk = col in pk_cols
                    sa_cols.append(Column(col, col_type, primary_key=is_pk))  # pyright: ignore[reportUnknownMemberType, reportArgumentType]

                table_obj = Table(table, metadata, *sa_cols)  # pyright: ignore[reportUnknownArgumentType]
                table_obj.create(bind=sa_con)

            # conversione del rel DuckDB in DataFrame / GeoDataFrame

            if has_geometry and geometry is not None and geometry in rel.columns:
                gdf = GataFrame(rel, con=con).toGeoPandas(
                    geometry=geometry, crs=crs_source or crs_target or "EPSG:4326"
                )
                if crs_source is not None:
                    gdf.set_crs(crs_source, allow_override=True, inplace=True)
                if crs_target is not None and crs_target != crs_source:
                    gdf = gdf.to_crs(crs_target)

                gdf.to_file(sqlite_file.as_posix(), layer=table, driver="SQLite")  # pyright: ignore[reportUnknownMemberType]
            else:
                df: pd.DataFrame = rel.df()
                # scrittura dati usando pandas -> SQLAlchemy (sempre append, la logica di mode è sopra)
                df.to_sql(
                    name=table,
                    con=sa_con,
                    if_exists="append",
                    index=False,
                )

            # costruzione indici: da partitionBy (usato come suggerimento) e da 'index'
            ddl_indexes: list[str] = []

            # 1) indice sulle colonne di partizione (se richieste)
            part_cols = [c for c in self._get_sequence(partitionBy) if c in rel_colset]
            if create_table and part_cols:
                index = list(index) if index is not None else []
                index.append(",".join(part_cols))  # pyright: ignore[reportOptionalMemberAccess]

            # 2) indici aggiuntivi espliciti
            idx_specs = normalize_index_seq(index)
            dest = f'"{table}"'
            if create_table and idx_specs:
                ddl_indexes.extend(
                    build_additional_indexes(
                        idx_specs=idx_specs,
                        rel_colset=rel_colset,
                        dest=dest,
                        table=table,
                    )
                )

            for sql in ddl_indexes:
                sa_con.execute(text(sql))

    def _create_table_postgresql(
        self,
        rel: duckdb.DuckDBPyRelation,
        schema: str,
        table: str,
        pk: Sequence[str] | str | None = None,
        partitionBy: Sequence[str] | str | None = None,
        n_partitions: Sequence[int] | int | None = None,
        index: Sequence[str | tuple[str, str]] | str | tuple[str, str] | None = None,
    ) -> tuple[list[str], str | None, list[str]]:
        """
        Crea i DDL PostgreSQL per una tabella eventualmente partizionata HASH in modo annidato.

        Regole:
        - partitionBy None o vuoto -> nessun partizionamento
        - n_partitions None -> nessun partizionamento
        - se non c'è partizionamento e partitionBy contiene colonne esistenti -> crea un indice btree su quelle colonne
        - se c'è partizionamento:
            * partizionamento HASH annidato per ogni colonna di partitionBy
            * numero partizioni del livello i:
                - n_partitions se int
                - n_partitions[i] se sequence
            * PK = pk + eventuali colonne di partitionBy mancanti
        - index:
            * str -> indice BTREE sulla colonna
            * tuple[str, str] -> indice del tipo specificato sulla colonna
        - restituisce:
            1) ddl_create: lista ddl CREATE TABLE ...
            2) ddl_pk: ddl ALTER TABLE ... ADD PRIMARY KEY ...
            3) ddl_indexes: lista ddl CREATE INDEX ...
        """

        # solo colonne esistenti nella relation
        rel_cols = list(rel.columns)
        rel_types = list(rel.types)
        rel_colset = set(rel_cols)

        def qident(name: str, cols: set[str] | list[str] | None = None) -> str:
            if cols is None:
                return '"' + str(name).replace('"', '""') + '"'
            if name.strip() in cols:
                return '"' + str(name).replace('"', '""') + '"'
            return name

        def qname(*parts: str) -> str:
            return ".".join(qident(p, None) for p in parts if p)

        def normalize_index_seq(
            idx: Sequence[str | tuple[str, str]] | str | tuple[str, str] | None,
        ) -> list[str | tuple[str, str]]:
            if idx is None:
                return []
            if isinstance(idx, str):
                return [idx]
            if isinstance(idx, tuple):
                return [idx]  # pyright: ignore[reportReturnType]
            return list(idx)

        def unique_preserve_order(items: Sequence[str]) -> list[str]:
            seen: set[str] = set()
            out: list[str] = []
            for x in items:
                if x not in seen:
                    seen.add(x)
                    out.append(x)
            return out

        def get_nparts_for_level(level: int) -> int | None:
            if n_partitions is None:
                return None
            if isinstance(n_partitions, int):
                return n_partitions
            if level >= len(n_partitions):
                raise ValueError(
                    f"n_partitions non ha abbastanza elementi per il livello {level}. "
                    f"partitionBy={partitions}, n_partitions={n_partitions}"
                )
            return int(n_partitions[level])

        def safe_table_suffix(value: Any) -> str:
            s = str(value)
            return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in s)

        def normalize_index_type(index_type: str | None) -> str:
            if not index_type:
                return "BTREE"
            return index_type.strip().upper()

        def build_additional_indexes(
            idx_specs: list[str | tuple[str, str]],
            rel_colset: set[str],
            dest: str,
            table: str,
        ) -> list[str]:
            rel_colset = {c.lower() for c in rel_colset}
            ddls: list[str] = []
            used_names: set[str] = set()

            for _, spec in enumerate(idx_specs):
                if isinstance(spec, str):
                    col = spec
                    idx_type = "BTREE"
                    is_col = col.strip() in rel_colset
                    if is_col:
                        col_idx: int = rel_cols.index(col)
                        if 0 < col_idx < len(rel_cols):
                            col_type = rel_types[col_idx]
                            if str(col_type).upper().startswith("GEOMETRY"):
                                idx_type = "GIST"

                else:
                    if len(spec) != 2:
                        raise ValueError(f"Specifica indice non valida: {spec}. Atteso ('colonna', 'tipo_indice').")
                    col, idx_type = spec
                    idx_type = normalize_index_type(idx_type)
                safe_col: str = col.replace(",", "_").replace(" ", "")

                idx_name_base = f"{table}_{safe_table_suffix(safe_col)}_{safe_table_suffix(idx_type.lower())}_idx"
                idx_name = idx_name_base
                i = 1
                while idx_name in used_names:
                    idx_name = f"{idx_name_base}_{i}"
                    i += 1
                used_names.add(idx_name)

                ddls.append(f"CREATE INDEX {qident(idx_name)} ON {dest} USING {idx_type} ({qident(col, rel_colset)});")

            return ddls

        def build_partition_tree(
            parent_fqname: str,
            parent_name_prefix: str,
            level: int,
            ddl_acc: list[str],
        ) -> None:
            """
            Crea ricorsivamente le partizioni HASH annidate.
            parent_fqname: nome fully-qualified del parent
            parent_name_prefix: prefisso nome locale per le partizioni
            level: indice del livello corrente su partitions
            """
            nparts = get_nparts_for_level(level)
            if nparts is None or nparts <= 0:
                raise ValueError(f"Numero di partizioni non valido per il livello {level}: {nparts}")

            has_child_level = level < len(partitions) - 1

            for remainder in range(nparts):
                child_name = f"{parent_name_prefix}_p{level}_{remainder}"
                child_fqname = qname(schema, child_name)

                if has_child_level:
                    next_part_col = partitions[level + 1]
                    ddl_acc.append(
                        f"CREATE TABLE {child_fqname} PARTITION OF {parent_fqname}\n"
                        f"FOR VALUES WITH (MODULUS {nparts}, REMAINDER {remainder})\n"
                        f"PARTITION BY HASH ({qident(next_part_col)});"
                    )
                    build_partition_tree(
                        parent_fqname=child_fqname,
                        parent_name_prefix=child_name,
                        level=level + 1,
                        ddl_acc=ddl_acc,
                    )
                else:
                    ddl_acc.append(
                        f"CREATE TABLE {child_fqname} PARTITION OF {parent_fqname}\n"
                        f"FOR VALUES WITH (MODULUS {nparts}, REMAINDER {remainder});"
                    )

        # normalizzazione input
        partitions = self._get_sequence(partitionBy)
        pk_cols = self._get_sequence(pk)
        index_specs = normalize_index_seq(index)

        # partitions = [c for c in partitions if c in rel_colset]
        # pk_cols = [c for c in pk_cols if c in rel_colset]

        # destinazione
        dest = qname(schema, table)

        # definizione colonne
        ddl_cols = ",\n".join(
            f"{qident(col)} {duckdb_type_to_postgres(str(dtype))}" for col, dtype in zip(rel.columns, rel.types)
        )

        # decide se partizionare
        do_partition = bool(partitions) and (n_partitions is not None)

        ddl_create: list[str] = []
        ddl_indexes: list[str] = []
        ddl_pk: str | None = None

        if schema:
            ddl_create.append(f"CREATE SCHEMA IF NOT EXISTS {qname(schema)}")
        if do_partition:
            first_partition_col = partitions[0]
            ddl_create.append(
                f"CREATE TABLE {dest} (\n{ddl_cols}\n)\nPARTITION BY HASH ({qident(first_partition_col, rel_cols)});"
            )

            build_partition_tree(
                parent_fqname=dest,
                parent_name_prefix=table,
                level=0,
                ddl_acc=ddl_create,
            )

            # su tabella partizionata la PK deve includere le colonne di partizione
            if pk_cols:
                final_pk_cols = unique_preserve_order([*pk_cols, *partitions])
            else:
                final_pk_cols = []

            if final_pk_cols:
                pk_name = f"{table}_pk"
                ddl_pk = (
                    f"ALTER TABLE {dest} "
                    f"ADD CONSTRAINT {qident(pk_name)} "
                    f"PRIMARY KEY ({', '.join(qident(c, rel_cols) for c in final_pk_cols)});"
                )

            # indici aggiuntivi
            ddl_indexes.extend(
                build_additional_indexes(
                    idx_specs=index_specs,
                    rel_colset=rel_colset,
                    dest=dest,
                    table=table,
                )
            )

        else:
            ddl_create.append(f"CREATE TABLE {dest} (\n{ddl_cols}\n);")

            if pk_cols:
                pk_name = f"{table}_pk"
                ddl_pk = (
                    f"ALTER TABLE {dest} "
                    f"ADD CONSTRAINT {qident(pk_name)} "
                    f"PRIMARY KEY ({', '.join(qident(c, rel_cols) for c in pk_cols)});"
                )

            # se non partiziona e partitionBy ha colonne valide -> crea indice btree
            if partitions:
                idx_name = f"{table}_" + "_".join(safe_table_suffix(c) for c in partitions) + "_idx"
                ddl_indexes.append(
                    f"CREATE INDEX {qident(idx_name)} "
                    f"ON {dest} USING BTREE ({', '.join(qident(c, rel_cols) for c in partitions)});"
                )

            # indici aggiuntivi
            ddl_indexes.extend(
                build_additional_indexes(
                    idx_specs=index_specs,
                    rel_colset=rel_colset,
                    dest=dest,
                    table=table,
                )
            )

        return ddl_create, ddl_pk, ddl_indexes

    def _write_db_postgres(
        self,
        rel: duckdb.DuckDBPyRelation,
        con: duckdb.DuckDBPyConnection,
        destination: str,
        mode: str,
        partitionBy: str | Sequence[str] | None = None,
        pk: Sequence[str] | str | None = None,
        index: str | Sequence[str] | Sequence[str | tuple[str, str]] | None = None,
        n_partitions: Sequence[int] | int | None = None,
        geometry: str | None = None,
        crs_source: str | None = None,  # source CRS for transform pipeline (optional)
        crs_target: str | None = None,  # destination CRS (transform geometry if provided)
        **kwargs: dict[str, Any],
    ):
        self._ensure_postgres()
        self._ensure_spatial()
        # DuckDB expects parameters as a comma-separated string, while parse_qs returns lists
        db_scheme, db_params, db_parsed = self._db_params(destination)

        table = db_params.pop("table", [None])[0]
        schema = db_params.pop("schema", ["public"])[0]

        pk = pk or db_params.pop("pk", [None])[0]
        index = index or db_params.pop("index", None)
        if index is not None:
            if isinstance(index, str):
                index = [index]
            if isinstance(index, tuple):
                index = list(index)
            for i, idx in enumerate(index):
                if isinstance(idx, list):
                    if len(idx) != 2:
                        raise ValueError("index has been str, list[str] or list[str|tuple[str,str]]")
                    index[i] = tuple(idx)  # pyright: ignore

        db_params.pop("query", None)
        if db_scheme in ("postgres", "postgresql"):
            clean_url = urlunparse(db_parsed)
            clean_url = f"dbname={db_parsed.path.lstrip('/')} port={db_parsed.port} host={db_parsed.hostname} user={db_parsed.username} password={db_parsed.password}"
            attach_alias = self._db_attached.get((clean_url, ""), None)
            if attach_alias is None:
                attach_alias = f"pg_{len(self._db_attached) + 1}"
                con.execute(f"ATTACH '{clean_url}' AS {attach_alias} (TYPE postgres);")  # pyright: ignore[reportOptionalMemberAccess]
                self._db_attached[(clean_url, "")] = attach_alias

            if not table:
                raise ValueError("DB url must include ?table=...")

            create_table = self._manage_mode_db(mode=mode, database=attach_alias, schema=schema, table=table)
            temp_view: str = GataFrame.get_new_alias("rel_")
            con.register(temp_view, rel)
            ddl_indexes: list[str] = []
            if create_table:
                ddl_create, ddl_pk, ddl_indexes = self._create_table_postgresql(
                    rel,
                    schema=schema,
                    table=table,
                    pk=pk,
                    partitionBy=partitionBy,
                    n_partitions=n_partitions,
                    index=index,
                )

                if ddl_create:
                    for sql in ddl_create:
                        con.execute(f"CALL postgres_execute('{attach_alias}','{sql}')")

                if ddl_pk:
                    con.execute(f"CALL postgres_execute('{attach_alias}','{ddl_pk}')")

            con.execute(f'insert into "{attach_alias}"."{schema}"."{table}" SELECT * FROM {temp_view}')  # pyright: ignore[reportOptionalMemberAccess]

            if create_table:
                if ddl_indexes:
                    for sql in ddl_indexes:
                        con.execute(f"CALL postgres_execute('{attach_alias}','{sql}')")

            con.unregister(temp_view)
        else:
            raise ValueError(f"Unsupported database scheme: {db_scheme}")

    def write(
        self,
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
        """
        destination:
          - file/dir path OR
          - sqlalchemy-like url: scheme://... ?table=...&schema=...&pk=... (pk optional)
        For DB:
          - postgres/sqlite "supported" detected by scheme; uses SQLAlchemy for robust DDL/insert.
          - if scheme not supported, uses SQLAlchemy.
        For file:
          - uses DuckDB COPY TO ... (FORMAT PARQUET/CSV), PARTITION_BY for hive layout if simulate_hive True.
          - if simulate_hive=False: writes a single file without partitioning.
          - if n_partitions is set and hive partitioning is off, writes N part files.
        """
        print(f"Write {destination}")
        mode = (mode or "overwrite").lower().strip()
        dest_str: str = str(destination)
        dest_path: Path = Path(dest_str.split("|", 1)[0])

        assert isinstance(df, (pd.DataFrame, gpd.GeoDataFrame, GataFrame)), f"Unsupported source type: {type(df)}"

        # ensure relation
        if isinstance(df, gpd.GeoDataFrame):
            if self.connection is None:
                raise RuntimeError("Database connection is not initialized")
            connection: duckdb.DuckDBPyConnection = self.connection
            geom_col = df.active_geometry_name
            df = pd.DataFrame(df.copy())
            df[geom_col] = gpd.GeoSeries.to_wkb(df[geom_col])  # pyright: ignore[reportCallIssue, reportArgumentType, reportUnknownMemberType]
            rel: duckdb.DuckDBPyRelation = connection.from_df(df)
            rel = rel.project(f'* REPLACE(st_GeomFromWKB("{geom_col}") as "{geom_col}")')
            df = GataFrame(rel, connection)
            geometry = geom_col

        elif isinstance(df, pd.DataFrame):
            if self.connection is None:
                raise RuntimeError("Database connection is not initialized")
            connection: duckdb.DuckDBPyConnection = self.connection
            rel: duckdb.DuckDBPyRelation = connection.from_df(df)
            df = GataFrame(rel, connection)
        else:
            rel: duckdb.DuckDBPyRelation = df.relation
            if self.connection is None:
                connection = df.conection
            else:
                connection: duckdb.DuckDBPyConnection = self.connection
            df = GataFrame(rel, connection)

        # transform geometry if requested
        df_cols = [c for c in rel.columns]
        if geometry is not None and geometry in df_cols:
            rel = self._rel_with_geometry_and_crs(rel, geometry=geometry, crs_target=crs_target, crs_source=crs_source)

        # Detect DB vs file

        is_db: bool = self._is_db_url(dest_str)
        if is_db:
            assert mode in ("overwrite", "append", "error", "warning", "truncate"), (
                'mode parameter has been in  ("overwrite","append","error","warning","truncate")'
            )
            db_scheme, _, _ = self._db_params(dest_str)
            if db_scheme in ("postgres", "postgresql"):
                self._write_db_postgres(
                    rel=rel,
                    con=connection,
                    destination=dest_str,
                    mode=mode,
                    partitionBy=partitionBy,
                    pk=pk,
                    index=index,
                    n_partitions=n_partitions,
                    geometry=geometry,
                    crs_source=crs_source,
                    crs_target=crs_target,
                    **kwargs,
                )
            elif db_scheme in ("sqlite"):
                self._write_db_sqlite(
                    rel=rel,
                    con=connection,
                    destination=dest_str,
                    mode=mode,
                    partitionBy=partitionBy,
                    pk=pk,
                    index=index,
                    n_partitions=n_partitions,
                    geometry=geometry,
                    crs_source=crs_source,
                    crs_target=crs_target,
                    **kwargs,
                )
            else:
                raise ValueError(f"Unsupported database scheme: {db_scheme}")
        else:
            # file
            assert mode in ("overwrite", "append", "error", "warning"), (
                'mode parameter has been in  ("overwrite","append","error","warning")'
            )
            format = self._guess_format(dest_path) if format is None else format.lower().strip()
            if format in ("parquet",):
                self._write_parquet(
                    rel=rel,
                    con=connection,
                    destination=dest_path,
                    mode=mode,
                    partitionBy=partitionBy,
                    geometry=geometry,
                    crs_source=crs_source,
                    crs_target=crs_target,
                    hive_partitioning=hive_partitioning,
                    **kwargs,
                )
            elif format in ("geoparquet",):
                self._write_parquet(
                    rel=rel,
                    con=connection,
                    destination=dest_path,
                    mode=mode,
                    partitionBy=partitionBy,
                    geometry=geometry,
                    crs_source=crs_source,
                    crs_target=crs_target,
                    hive_partitioning=hive_partitioning,
                    **kwargs,
                )
            elif format in ("csv",):
                self._write_csv(
                    rel=rel,
                    con=connection,
                    destination=dest_path,
                    mode=mode,
                    partitionBy=partitionBy,
                    geometry=geometry,
                    crs_source=crs_source,
                    crs_target=crs_target,
                    hive_partitioning=hive_partitioning,
                    **kwargs,
                )
            elif format in ("shp",):
                self._write_shp(
                    rel=rel,
                    con=connection,
                    destination=dest_path,
                    mode=mode,
                    partitionBy=partitionBy,
                    geometry=geometry,
                    crs_source=crs_source,
                    crs_target=crs_target,
                    hive_partitioning=hive_partitioning,
                    **kwargs,
                )
            elif format in ("gpkg"):
                self._write_gpkg(
                    rel=rel,
                    con=connection,
                    destination=dest_str,
                    mode=mode,
                    partitionBy=partitionBy,
                    geometry=geometry,
                    crs_source=crs_source,
                    crs_target=crs_target,
                    hive_partitioning=hive_partitioning,
                    **kwargs,
                )
            elif format in ("json",):
                self._write_json(
                    rel=rel,
                    con=connection,
                    destination=dest_str,
                    mode=mode,
                    partitionBy=partitionBy,
                    geometry=geometry,
                    crs_source=crs_source,
                    crs_target=crs_target,
                    hive_partitioning=hive_partitioning,
                    **kwargs,
                )
            elif format in ("geojson",):
                self._write_geojson(
                    rel=rel,
                    con=connection,
                    destination=dest_str,
                    mode=mode,
                    partitionBy=partitionBy,
                    geometry=geometry,
                    crs_source=crs_source,
                    crs_target=crs_target,
                    hive_partitioning=hive_partitioning,
                    **kwargs,
                )
            elif format in ("sqlite",):
                self._write_sqlite(
                    rel=rel,
                    con=connection,
                    destination=dest_str,
                    mode=mode,
                    partitionBy=partitionBy,
                    geometry=geometry,
                    crs_source=crs_source,
                    crs_target=crs_target,
                    hive_partitioning=hive_partitioning,
                    **kwargs,
                )
            else:
                raise ValueError(f"Unsupported format: {format}")
