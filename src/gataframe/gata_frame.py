from __future__ import annotations
from enum import Enum
import logging
from duckdb import DuckDBPyConnection, DuckDBPyRelation, ExplainType
from duckdb.sqltypes import DuckDBPyType
from uuid import uuid4
from typing import Any, Iterable, Literal
import warnings
import geopandas as gpd
import pandas as pd
from .data_schema import DataSchema, SchemaField, GeneratorSpec, AdditionalSchemaField
from .conversion_types import DUCKDB_TYPE_ALIASES


def _normalize_col_name(col: str) -> str:
    return col.strip().strip('"').strip("'")


_n = _normalize_col_name


class GFDataType(Enum):
    VARCHAR = "VARCHAR"
    BOOLEAN = "BOOLEAN"
    TINYINT = "TINYINT"
    SMALLINT = "SMALLINT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    HUGEINT = "HUGEINT"
    UTINYINT = "UTINYINT"
    USMALLINT = "USMALLINT"
    UINTEGER = "UINTEGER"
    UBIGINT = "UBIGINT"
    UHUGEINT = "UHUGEINT"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMPTZ"
    TIME = "TIME"
    UUID = "UUID"
    BLOB = "BLOB"
    GEOMETRY = "GEOMETRY"
    JSON = "JSON"

    @classmethod
    def parse(cls, value: str) -> GFDataType:
        value = value.upper()
        for dtype in cls:
            if dtype.value == value:
                return dtype
        raise ValueError(f"Unknown GFDataType: {value}")


class GataFrame:
    TypeRelation = "relation"
    TypeView = "view"
    TypeTable = "table"
    TypeInconsistent = "inconsistent"

    GataFramesStatus: dict[str, dict[str, Any]] = {}

    def __new__(
        cls, df: DuckDBPyRelation | GataFrame, con: DuckDBPyConnection | Any | None = None, *args: Any, **kwargs: Any
    ) -> GataFrame:
        if isinstance(df, cls):
            df._fresh = False
            return df  # restituisce esattamente lo stesso oggetto
        obj = super().__new__(cls)
        obj._fresh = True
        return obj

    def __init__(
        self,
        df: DuckDBPyRelation | GataFrame,
        con: DuckDBPyConnection | Any | None = None,
        alias: str | None = None,
        description: str | None = None,
        version: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if not getattr(self, "_fresh", False):
            return
        self._fresh: bool = False
        if isinstance(df, DuckDBPyRelation):
            self._rel: DuckDBPyRelation = df
        else:
            self._rel: DuckDBPyRelation = df._rel

        tmp_con: DuckDBPyConnection | None = None
        if isinstance(con, DuckDBPyConnection):
            tmp_con = con
        elif con is not None:
            for _, val in con.__dict__.items():
                if isinstance(val, DuckDBPyConnection):
                    tmp_con = val
        if tmp_con is None:
            from .engine import Engine

            con = Engine.connect(logger=None, extensions=None, options=None, file_based=False)
            if con is not None:
                final_conn: DuckDBPyConnection = con
            else:
                raise ValueError("GataFrame requires a duckdb connection or object with .connection attribute.")
        else:
            final_conn: DuckDBPyConnection = tmp_con
        self._con: DuckDBPyConnection = final_conn
        self._uuid: str = uuid4().hex
        self._alias: str | None = alias
        self._type = self.TypeRelation
        self._description = description
        self._version = version
        self._set_status(self._alias, self._type)
        self._ops: list[str] = ["init"]

    def _append_op(self, op: str | Iterable[str] | None):
        if op is None:
            return
        if isinstance(op, str):
            self._ops.append(op)
        else:
            self._ops.extend(op)

    def _create_from_relation(
        self,
        rel: DuckDBPyRelation,
        op: str | Iterable[str] | None = None,
        alias: str | None = None,
        type_: str = TypeRelation,
    ) -> GataFrame:
        new_df = GataFrame(rel, self._con, alias=alias, description=self._description, version=self._version + 1)
        new_df._set_status(alias, type_)
        new_df._ops = self._ops.copy()
        new_df._append_op(op)
        return new_df

    @staticmethod
    def get_new_alias(prefix: str = "t_") -> str:
        alias: str = uuid4().hex
        if prefix:
            alias = prefix + alias
        return alias

    @staticmethod
    def _get_status_by_name(name: str, type_: str | None = None) -> dict[str, Any] | None:
        for _, v in GataFrame.GataFramesStatus.items():
            if v.get("alias", None) == name and (type_ is None or v.get("type", None) == type_):
                return v
        return None

    def __str__(self) -> str:
        attrs: list[str] = []
        if self._description:
            attrs.append(f"description={self._description}")
        attrs.append(f"version={self._version}")
        if self._alias:
            attrs.append(f"alias={self._alias}")
        if self._type:
            attrs.append(f"type={self._type}")
        ret = f"GataFrame({', '.join(attrs)}, shape={self.shape})"
        if len(self._ops) > 0:
            ret += f"\n  Operations: {' -> '.join(self._ops)}"
        return ret

    def _set_status(self, alias: str | None, type_: str):
        GataFrame.GataFramesStatus[self._uuid] = {"alias": alias, "type": type_, "df": self}
        self._alias = alias
        self._type = type_

    def _reset(self):
        self._set_status(None, self.TypeRelation)

    def _set_inconsistent(self):
        self._set_status(None, self.TypeInconsistent)

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def conection(self) -> DuckDBPyConnection:
        return self._con

    @property
    def relation(self) -> DuckDBPyRelation:
        return self._rel

    @property
    def get_type(self) -> str:
        return self._type

    @property
    def alias(self) -> str | None:
        return self._alias

    def show(self, max_rows: int | None = 10, logger: logging.Logger | None = None) -> None:
        if logger:
            logger.info("\n" + str(self._rel.limit(max_rows) if max_rows is not None else self._rel))
        else:
            self._rel.show(max_rows=max_rows)

    def printSchema(self, logger: logging.Logger | None = None):
        if logger:
            _p = logger.info
        else:
            _p = print
        _p(f"Schema of relation '{self}':")
        for name, dtype in zip(self._rel.columns, self._rel.types):
            _p(f"|-{name}: {dtype}")

    def explain(self, analyze: bool = False, logger: logging.Logger | None = None):
        if logger:
            _p = logger.info
        else:
            _p = print
        _p(f"EXPLAIN{' ANALYZE' if analyze else ''} of relation '{self}':")
        if analyze:
            _p(self._rel.explain(ExplainType.ANALYZE))
        else:
            _p(self._rel.explain(ExplainType.STANDARD))

    @property
    def empty(self) -> bool:
        return self._rel.shape[0] == 0

    def isEmpty(self) -> bool:
        return self._rel.shape[0] == 0

    @property
    def shape(self) -> tuple[int, int]:
        return self._rel.shape

    @property
    def count(self) -> int:
        return self._rel.shape[0]

    @property
    def n_cols(self) -> int:
        return self._rel.shape[1]

    @property
    def n_rows(self) -> int:
        return self._rel.shape[0]

    @property
    def columns(self) -> list[str]:
        return self._rel.columns

    @property
    def types(self) -> list[DuckDBPyType]:
        return self._rel.types

    @property
    def dtypes(self) -> dict[str, DuckDBPyType]:
        return dict(zip(self._rel.columns, self._rel.types))  # pyright: ignore[reportReturnType]

    def withColumn(self, cols: str | dict[str, str], expr: Any | None = None, inplace: bool = False) -> GataFrame:
        assert isinstance(cols, (str, dict)), (
            "col must be a string or a dictionary mapping column names with expressions."
        )
        if isinstance(cols, str):
            assert expr is not None, "expr must be provided as string when col is a string."
        else:
            assert expr is None, "expr must not be provided when col is a dictionary."

        if isinstance(cols, dict):
            for k, v in cols.items():
                assert isinstance(k, str) and isinstance(v, str), (
                    "All keys and values in the col dictionary must be strings."
                )
            expr_str: str = ", ".join([f'{v} as "{_n(k)}"' for k, v in cols.items()])
            set_of_cols_str = ", ".join([f"'{_n(k)}'" for k in cols.keys()])
            op = f"withColumn({expr_str})"
            new_df = self._rel.project(f"COLUMNS(lambda x: x not in [{set_of_cols_str}]), {expr_str}")
            if inplace:
                self._rel = new_df
                self._reset()
                self._append_op(op)
                return self
            else:
                return self._create_from_relation(new_df, op=op)
        elif isinstance(cols, str):
            op = f"withColumn({_n(expr)} as {_n(cols)})"
            new_df = self._rel.project(f"COLUMNS(lambda x: x<>'{_n(cols)}'), {expr} as \"{_n(cols)}\"")
            if inplace:
                self._rel = new_df
                self._reset()
                self._append_op(op)
                return self
            else:
                return self._create_from_relation(new_df, op=op)
        else:
            raise ValueError("Parameters are not valid.")

    def renameColumn(self, cols: str | dict[str, str], rename: str | None = None, inplace: bool = False) -> GataFrame:
        assert isinstance(cols, (str, dict)), (
            "cols must be a string or a dictionary mapping column names with expressions."
        )
        if isinstance(cols, str):
            assert rename is not None, "rename must be provided as string when cols is a string."
        else:
            assert rename is None, "rename must not be provided when cols is a dictionary."
        or_cols = self.columns.copy()
        list_select: list[str] = []
        if isinstance(cols, str):
            if rename is None:
                raise ValueError("Rename name must be provided when col is a string.")
            cols = {_n(cols): _n(rename)}
        else:
            cols = {_n(k): _n(v) for k, v in cols.items()}

        if len(cols) == 0:
            raise ValueError("No columns available to rename.")
        elif len(cols) == 1:
            op = f"renameColumn({list(cols.keys())[0]} -> {list(cols.values())[0]})"
        for old_name in or_cols:
            op = f"renameColumn({cols} -> {rename})"
            new_name = cols.get(old_name, old_name)
            old_name = old_name
            if new_name == old_name:
                list_select.append(f'"{old_name}"')
            else:
                list_select.append(f'"{old_name}" as "{new_name}"')
        new_df = self._rel.project(", ".join(list_select))
        if inplace:
            self._rel = new_df
            self._reset()
            self._append_op(op)
            return self
        else:
            return self._create_from_relation(new_df, op=op)

    def excludeColumn(self, *cols: Iterable[str | Iterable[str]], inplace: bool = True) -> GataFrame:
        list_of_cols: list[str] = []
        for col in cols:
            if isinstance(col, str):
                list_of_cols.append(col)
            else:
                list_of_cols.extend(col)  # pyright: ignore[reportArgumentType]
        list_of_cols = [_n(c) for c in set(list_of_cols)]
        op = f"excludeColumn({', '.join(list_of_cols)})"
        if bool(list_of_cols):
            list_str: str = ", ".join(f'"{c}"' for c in list_of_cols)
            sql: str = f"* EXCLUDE({list_str})"
        else:
            sql: str = "*"
        new_df = self._rel.project(sql)
        if inplace:
            self._rel = new_df
            self._reset()
            self._append_op(op)
            return self
        else:
            return self._create_from_relation(new_df, op=op)

    def replaceColumn(self, cols: str | dict[str, str], expr: Any | None = None, inplace: bool = False) -> GataFrame:
        assert isinstance(cols, (str, dict)), (
            "col must be a string or a dictionary mapping column names with expressions."
        )
        if isinstance(cols, str):
            assert expr is not None, "expr must be provided as string when col is a string."
        else:
            assert expr is None, "expr must not be provided when col is a dictionary."

        if isinstance(cols, dict):
            for k, v in cols.items():
                assert isinstance(k, str) and isinstance(v, str), (
                    "All keys and values in the col dictionary must be strings."
                )
            expr_str: str = ", ".join([f'{v} as "{_n(k)}"' for k, v in cols.items()])
            op = f"replaceColumn({expr_str})"
            new_df = self._rel.project(f"* REPLACE({expr_str})")
            if inplace:
                self._rel = new_df
                self._reset()
                self._append_op(op)
                return self
            else:
                return self._create_from_relation(new_df, op=op)
        elif isinstance(cols, str):
            op = f"replaceColumn({_n(expr)} as {_n(cols)})"
            new_df = self._rel.project(f'* REPLACE({expr} as "{_n(cols)}")')
            if inplace:
                self._rel = new_df
                self._reset()
                self._append_op(op)
                return self
            else:
                return self._create_from_relation(new_df, op=op)
        else:
            raise ValueError("Parameters are not valid.")

    def project(
        self, *cols: Iterable[str | Iterable[str]], inplace: bool = False, include_others: bool = False
    ) -> GataFrame:
        list_of_cols: list[str] = []
        for col in cols:
            if isinstance(col, str):
                list_of_cols.append(col)
            else:
                list_of_cols.extend(col)  # pyright: ignore[reportArgumentType]
        list_of_cols = list(_n(c) for c in list_of_cols)
        new_df: DuckDBPyRelation | None = None
        if not bool(list_of_cols):
            if include_others == True:
                new_df = self._rel.project("*")
                op = f"project(*)"
            else:
                new_df = self._rel.select()
                op = f"select()"
        else:
            if include_others == False:
                list_cols = ", ".join(list_of_cols)
                new_df = self._rel.select(list_cols)
                op = f"select({list_cols})"
            else:
                list_cols = ", ".join(list_of_cols)
                tmp = self._rel.project(f"{list_cols}").limit(0)
                new_cols: list[str] = list(_n(c) for c in tmp.columns)
                to_replace: list[str] = []
                to_add: list[str] = []
                df_cols: list[str] = self._rel.columns
                for c, expr in zip(new_cols, list_of_cols):
                    if c not in df_cols:
                        to_add.append(expr)
                    else:
                        to_replace.append(expr)
                to_replace_str = "REPLACE (" + ", ".join(f'"{c}"' for c in to_replace) + ")"
                to_add_str = ", ".join(f'"{c}"' for c in to_add)
                sql = " * "
                if to_replace:
                    sql += to_replace_str
                if to_add:
                    sql += f", {to_add_str}"
                new_df = self._rel.project(sql)
                op = f"project({', '.join(list_of_cols)})"
        if inplace:
            self._rel = new_df
            self._reset()
            self._append_op(op)
            return self
        else:
            return self._create_from_relation(new_df, op=op)

    select = project

    def filter(self, filter_expr: str, inplace: bool = False) -> GataFrame:
        new_df = self._rel.filter(filter_expr)
        op = f"filter({filter_expr})"
        if inplace:
            self._rel = new_df
            self._reset()
            self._append_op(op)
            return self
        else:
            return self._create_from_relation(new_df, op=op)

    def limit(self, n: int | None, inplace: bool = False) -> GataFrame:
        if n is None:
            warnings.warn("Limit None is treated as no limit (full dataset).", UserWarning)
            new_df = self._rel
        else:
            new_df = self._rel.limit(n)
        op = f"limit({n})"
        if inplace:
            self._rel = new_df
            self._reset()
            self._append_op(op)
            return self
        else:
            return self._create_from_relation(new_df, op=op)

    def createTable(
        self, table_name: str | None = None, replace: bool = False, inplace: bool = False, temporary: bool = True
    ) -> GataFrame:
        if not (table_name):
            table_name = GataFrame.get_new_alias(prefix="t_")
        table_name = _n(table_name)
        if replace:
            self._con.execute(f'DROP TABLE IF EXISTS "{table_name}"')  # pyright: ignore[reportOptionalMemberAccess]
        internal_rel_4623562568234681414: DuckDBPyRelation = self._rel  # pyright: ignore[reportUnusedVariable]
        if temporary:
            op = f"createTempTable({table_name})"
            self._con.execute(
                f'CREATE TEMPORARY TABLE "{table_name}" AS SELECT * FROM internal_rel_4623562568234681414'
            )  # pyright: ignore[reportOptionalMemberAccess]
        else:
            op = f"createTable({table_name})"
            self._rel.create(f'"{table_name}"')
        new_rel = self._con.table(table_name)  # pyright: ignore[reportOptionalMemberAccess]
        if inplace:
            self._rel = new_rel
            self._set_status(table_name, self.TypeTable)
            self._ops.clear()
            self._append_op(op)
            return self
        else:
            new_df = self._create_from_relation(new_rel, alias=table_name, type_=self.TypeTable)
            new_df._ops.clear()
            new_df._append_op(op)
            return new_df

    def createView(self, view_name: str | None = None, replace: bool = False, inplace: bool = False) -> GataFrame:
        if not (view_name):
            view_name = GataFrame.get_new_alias(prefix="v_")
        view_name = _n(view_name)
        self._alias = view_name
        self._rel.create_view(view_name, replace=replace)
        op = f"createView({view_name})"
        new_rel = self._con.table(view_name)  # pyright: ignore[reportOptionalMemberAccess]
        if inplace:
            self._rel = new_rel
            self._set_status(view_name, self.TypeView)
            self._append_op(op)
            return self
        else:
            new_df = self._create_from_relation(new_rel, op=op, alias=view_name, type_=self.TypeView)
            return new_df

    def dropTable(self, name: str | None = None):
        if name is None:
            if self._alias is None:
                raise ValueError("No table name specified and current relation has no alias.")
            name = self._alias
        self._con.execute(f'DROP TABLE IF EXISTS "{name}"')  # pyright: ignore[reportOptionalMemberAccess]
        status: dict[str, Any] | None = GataFrame._get_status_by_name(name, type_=self.TypeTable)
        df: GataFrame | None = status.get("df", None) if status is not None else None
        if df is not None:
            df._set_inconsistent()
            df._append_op([f"dropTable({name})", f"-> {df._type}"])
        else:
            warnings.warn(
                f"Table '{name}' dropped, but no corresponding GataFrame found in status registry to mark as inconsistent.",
                UserWarning,
            )

    def dropView(self, name: str | None = None):
        if name is None:
            if self._alias is None:
                raise ValueError("No view name specified and current relation has no alias.")
            name = self._alias
        name = _n(name)
        self._con.execute(f'DROP VIEW IF EXISTS "{name}"')  # pyright: ignore[reportOptionalMemberAccess]
        status: dict[str, Any] | None = GataFrame._get_status_by_name(name, type_=self.TypeView)
        df: GataFrame | None = status.get("df", None) if status is not None else None
        if df is not None:
            df._set_inconsistent()
            df._append_op([f"dropView({name})", f"-> {df._type}"])
        else:
            warnings.warn(
                f"View '{name}' dropped, but no corresponding GataFrame found in status registry to mark as inconsistent.",
                UserWarning,
            )

    def dropColumn(
        self, *cols: Iterable[str | Iterable[str]], inplace: bool = False, table: str | None = None
    ) -> GataFrame | None:
        list_of_cols: list[str] = []
        for col in cols:
            if isinstance(col, str):
                list_of_cols.append(col)
            else:
                list_of_cols.extend(col)  # pyright: ignore[reportArgumentType]
        list_of_cols = list(_n(c) for c in set(list_of_cols))
        if table is not None:
            table = _n(table)
            for col in list_of_cols:
                self._con.execute(f'ALTER TABLE "{table}" DROP COLUMN "{col}"')  # pyright: ignore[reportOptionalMemberAccess]
            status: dict[str, Any] | None = GataFrame._get_status_by_name(table, type_=self.TypeTable)
            df: GataFrame | None = status.get("df", None) if status is not None else None
            if df is not None:
                df._append_op([f"dropColumn({', '.join(list_of_cols)})"])
                if len(df.columns) == 0:
                    df._set_inconsistent()
                    df._append_op(f"-> {df._type} (no columns left)")
                    warnings.warn(f"After dropping columns, no columns left in relation \n {df}", UserWarning)
            return df
        else:
            if self._type != self.TypeTable:
                df = self.excludeColumn(*list_of_cols, inplace=inplace)
                if len(df.columns) == 0:
                    df._set_inconsistent()
                    df._append_op(f"-> {df._type} (no columns left)")
                    warnings.warn(f"After dropping columns, no columns left in relation \n {df}", UserWarning)
                return df
            else:
                if self._alias is None:
                    raise ValueError(
                        "Current relation has no alias. Specify table name or create a temporary table/view with an alias before dropping columns."
                    )
                for col in list_of_cols:
                    self._con.execute(f"ALTER TABLE {self._alias} DROP COLUMN {col}")  # pyright: ignore[reportOptionalMemberAccess]
                op = f"dropColumn({', '.join(list_of_cols)})"
                if inplace:
                    self._rel = self._con.table(self._alias)  # pyright: ignore[reportOptionalMemberAccess]
                    self._append_op(op)
                    if len(self.columns) == 0:
                        self._set_inconsistent()
                        self._append_op(f"-> {self._type} (no columns left)")
                        warnings.warn(f"After dropping columns, no columns left in relation \n {self}", UserWarning)
                    return self
                else:
                    warnings.warn(
                        "After dropping columns, the relation is refreshed from the database. inplace=False option will not have any effect.",
                        UserWarning,
                    )
                    df = self._create_from_relation(
                        self._con.table(self._alias), alias=self._alias, type_=self._type, op=op
                    )  # pyright: ignore[reportOptionalMemberAccess]
                    if len(df.columns) == 0:
                        df._set_inconsistent()
                        df._append_op(f"-> {df._type} (no columns left)")
                        warnings.warn(f"After dropping columns, no columns left in relation \n {df}", UserWarning)
                    return df

    def exists(self, name: str | None = None) -> bool:
        if name is None:
            if self._alias is None:
                warnings.warn(
                    "No name specified and current relation has no alias. False will be returned.", UserWarning
                )
                return False
            name = self._alias
        try:
            self._con.table(name)  # pyright: ignore[reportOptionalMemberAccess]
            return True
        except Exception:
            return False

    def drop(self):
        if self._alias is None:
            warnings.warn("Current relation has no alias. Drop operation will not be performed.", UserWarning)
            return self
        if self._type == self.TypeTable:
            self.dropTable(self._alias)
        elif self._type == self.TypeView:
            self.dropView(self._alias)
        else:
            warnings.warn(f"Unsupported deleting relation type: {self._type}")
        return self

    def insertInto(self, target_table: str | GataFrame, inplace: bool = False) -> GataFrame:
        assert isinstance(target_table, (str, GataFrame)), (
            f"target_table must be a string or GataFrame. Got {type(target_table)}."
        )
        if isinstance(target_table, str):
            target_table_name = _n(target_table)
            self._rel.insert_into(target_table_name)
        else:
            df = target_table
            if df._type != self.TypeTable:
                raise ValueError(
                    f"Target relation for insertInto must be a table. Got type '{df._type}' with alias '{df._alias}'."
                )
            target_table_name = df._alias
            if target_table_name is None:
                raise ValueError(
                    "Target GataFrame for insertInto has no alias. Create a temporary table/view with an alias before using insertInto."
                )
            self._rel.insert_into(target_table_name)

        if inplace:
            self._rel = self._con.table(target_table_name)  # pyright: ignore[reportOptionalMemberAccess]
            self._set_status(target_table_name, self.TypeTable)
            self._append_op(f"insertInto({target_table_name})")
            return self
        else:
            return GataFrame(
                self._con.table(target_table_name),
                self._con,
                op=f"insertInto({target_table_name})",
                type_=self.TypeTable,
            )  # pyright: ignore[reportOptionalMemberAccess]

    def union(self: GataFrame, right: GataFrame | DuckDBPyRelation, inplace: bool = False) -> GataFrame:
        right = GataFrame(right, con=self._con)
        new_rel = self._rel.union(right._rel)
        if inplace:
            self._rel = new_rel
            self._reset()
            self._append_op(f"union()")
            return self
        else:
            return GataFrame(new_rel, self._con, op=f"union()", type_=self.TypeRelation)

    def persist(self, temporary: bool = False, force: bool = False, inplace: bool = False) -> GataFrame:
        table_name = f"__persist_{self._uuid}"
        if self.exists(table_name):
            if force:
                self.createTable(table_name=table_name, replace=True, temporary=temporary, inplace=inplace)
            else:
                warnings.warn(
                    f"Persisting relation with name '{table_name}' that already exists. Use force=True to replace it.",
                    UserWarning,
                )
        else:
            self.createTable(table_name=table_name, replace=True, temporary=temporary, inplace=inplace)
        return self

    def unpersist(self):
        try:
            self.dropTable(f"{self._alias}")
        except Exception:
            pass
        return self

    def sql(self, sql: str, inplace: bool = False, alias: str | None = None) -> GataFrame:
        new_df = self._con.sql(sql)  # pyright: ignore[reportOptionalMemberAccess]
        op = f"sql({sql[:30]}{'...' if len(sql) > 30 else ''})"

        if inplace:
            self._rel = new_df
            self._reset()
            self._alias = alias
            self._append_op(op)
            return self
        else:
            return self._create_from_relation(new_df, alias=alias, op=op)

    def execute(self, sql: str):
        self._con.execute(sql)  # pyright: ignore[reportOptionalMemberAccess]

    def distinct(self, inplace: bool = False) -> GataFrame:
        new_df = self._rel.distinct()
        op = "distinct()"
        if inplace:
            self._rel = new_df
            self._reset()
            self._append_op(op)
            return self
        else:
            return self._create_from_relation(new_df, op=op)

    def orderBy(
        self, list_cols: list[str | dict[str, str] | tuple[str, bool] | list[str | bool]] | str, inplace: bool = False
    ) -> GataFrame:
        expr_cols: list[str] = []
        if isinstance(list_cols, str):
            list_cols = [list_cols]
        for c in list_cols:
            if isinstance(c, str):
                expr_cols.append(c)
            elif isinstance(c, dict) and "col" in c and "asc" in c:
                direction = "ASC" if c["asc"] else "DESC"
                expr_cols.append(f"{c['col']} {direction}")
            elif isinstance(c, (list, tuple)) and len(c) == 2:
                col, asc = c
                direction = "ASC" if asc else "DESC"
                expr_cols.append(f"{col} {direction}")
            else:
                raise ValueError(
                    f"Unsupported column expression: {c}. Use string for simple expressions, or dict with 'col' and 'asc', or list of tuple (expr, asc:bool)."
                )
        new_df = self._rel.order(", ".join(expr_cols))
        op = f"orderBy({', '.join(expr_cols)})"
        if inplace:
            self._rel = new_df
            self._reset()
            self._append_op(op)
            return self
        else:
            return self._create_from_relation(new_df, op=op)

    def min(self, col: str) -> Any:
        return self._rel.min(col)

    def max(self, col: str) -> Any:
        return self._rel.max(col)

    def distinctCount(self, col: str) -> int:
        ret = self._rel.aggregate(f"COUNT(DISTINCT {col})").fetchone()
        return ret[0] if ret else 0

    def _as_type(
        self,
        expr: str,
        current_type: str,
        target_type: str,
        date_format: str | None = None,
        timezone: str | None = None,
        decimal_sep: str | None = None,
        thousand_sep: str | None = None,
        type_width: int | None = None,
        type_precision: int | None = None,
    ) -> str:

        if target_type == current_type:
            return expr
        if target_type == "DATE":
            if current_type in ("TIMESTAMP", "TIMESTAMPTZ"):
                expr = f"DATE({expr})"
            else:
                fmt = date_format or "%Y-%m-%d"
                if current_type != "VARCHAR":
                    expr = f"TRY_CAST({expr} AS VARCHAR)"
                expr = f"TRY_CAST(try_strptime({expr}, '{fmt}') AS DATE)"
            return expr
        if target_type == "TIME":
            if current_type in ("TIMESTAMP", "TIMESTAMPTZ"):
                expr = f"TIME({expr})"
            else:
                fmt = date_format or "%H:%M:%S"
                if current_type != "VARCHAR":
                    expr = f"TRY_CAST({expr} AS VARCHAR)"
                expr = f"TRY_CAST(try_strptime({expr}, '{fmt}') AS TIME)"
            return expr
        if target_type in ("TIMESTAMP", "TIMESTAMPTZ"):
            fmt = date_format or "%Y-%m-%d %H:%M:%S"

            has_tz_in_fmt = "%z" in fmt or "%Z" in fmt
            tz_expr = timezone or "Etc/UTC"
            if current_type == "TIMESTAMP" and target_type == "TIMESTAMPTZ":
                expr = f"timezone('{tz_expr}', {expr})"
            elif current_type == "TIMESTAMPTZ" and target_type == "TIMESTAMP":
                expr = f"timezone('{tz_expr}', {expr})"
            else:
                if target_type == "TIMESTAMP":
                    if current_type != "VARCHAR":
                        expr = f"TRY_CAST({expr} AS VARCHAR)"
                    # TIMESTAMP: parse e basta, nessuna conversione timezone
                    parsed = f"try_strptime({expr}, '{fmt}')"
                    expr = f"TRY_CAST({parsed} AS TIMESTAMP)"
                elif target_type == "TIMESTAMPTZ":
                    if current_type != "VARCHAR":
                        expr = f"TRY_CAST({expr} AS VARCHAR)"
                    if has_tz_in_fmt:
                        # Input ha già timezone info -> parse come TIMESTAMPTZ, poi converti nel tz target
                        parsed = f"try_strptime({expr}, '{fmt}')"
                        # AT TIME ZONE estrae l'ora locale, timezone() la reinterpreta come quel tz
                        expr = f"timezone('{tz_expr}', ({parsed}) AT TIME ZONE '{tz_expr}')"
                    else:
                        # Input naive -> interpreta come se fosse nel timezone specificato
                        parsed = f"TRY_CAST(try_strptime({expr}, '{fmt}') AS TIMESTAMP)"
                        expr = f"timezone('{tz_expr}', {parsed})"
                    expr = f"TRY_CAST({expr} AS TIMESTAMPTZ)"
            return expr
        if target_type in ("DOUBLE", "FLOAT", "DECIMAL"):
            if current_type != "VARCHAR":
                expr = f"TRY_CAST({expr} AS VARCHAR)"
            decimal_sep = "." if decimal_sep is None else decimal_sep
            thousand_sep = "" if thousand_sep is None else thousand_sep
            if thousand_sep not in (None, "") or decimal_sep not in (None, "."):
                if thousand_sep not in (None, ""):
                    expr = f"REPLACE({expr}, '{thousand_sep}', '')"
                if decimal_sep not in (None, "."):
                    expr = f"REPLACE({expr}, '{decimal_sep}', '.')"

            if target_type == "DECIMAL":
                width = 18 if type_width is None else type_width
                scale = 3 if type_precision is None else type_precision
                expr = f"TRY_CAST({expr} AS DECIMAL({width}, {scale}))"
                return expr
            else:
                expr = f"TRY_CAST({expr} AS {target_type})"
            return expr

        # stringhe
        if target_type == "VARCHAR":
            if target_type == "VARCHAR":
                if type_width is not None:
                    expr = f"TRY_CAST({expr} AS VARCHAR({type_width}))"
                else:
                    expr = f"TRY_CAST({expr} AS VARCHAR)"
            return expr

        if target_type.upper().startswith("GEOMETRY"):
            _, srid = self._parse_geometry_type(target_type)
            if current_type == "VARCHAR":
                expr = f"ST_GeomFromText({expr})"
                if srid is not None:
                    expr = f"ST_SetSRID({expr}, '{srid}')"
            elif current_type == "BLOB":
                expr = f"ST_GeomFromWKB({expr})"
                if srid is not None:
                    expr = f"ST_SetSRID({expr}, '{srid}')"
            elif current_type.upper().startswith("GEOMETRY"):
                return expr
            else:
                expr = f"ST_GeomFromText(TRY_CAST({expr} AS VARCHAR))"
                if srid is not None:
                    expr = f"ST_SetSRID({expr}, '{srid}')"
            return expr

        return f"TRY_CAST({expr} AS {target_type})"

    def as_type(
        self, columns: dict[str, str | DuckDBPyType | GFDataType | dict[str, Any]], inplace: bool = False
    ) -> GataFrame:
        if not columns:
            return self if inplace else self._create_from_relation(self._rel, op="as_type({})")

        dtypes = self.dtypes
        replacements: list[str] = []
        for name, spec in columns.items():
            if name not in dtypes:
                raise KeyError(f"Column not found: {name}")

            if isinstance(spec, dict):
                raw_type = spec.get("type")
                if raw_type is None:
                    raise ValueError(f"Missing type specification for column '{name}'")
                date_format = spec.get("date_format", spec.get("format"))
                timezone = spec.get("timezone", spec.get("tz"))
                decimal_sep = spec.get("decimal_sep")
                thousand_sep = spec.get("thousand_sep")
                type_width = spec.get("type_width", spec.get("width"))
                type_precision = spec.get("type_precision", spec.get("precision"))
            else:
                raw_type = spec.value if isinstance(spec, GFDataType) else spec
                date_format = timezone = decimal_sep = thousand_sep = type_width = type_precision = None

            current_type = self._type_mapping(dtypes[name])
            target_type = self._type_mapping(raw_type)
            expr = self._as_type(
                f'"{name}"',
                current_type,
                target_type,
                date_format=date_format,
                timezone=timezone,
                decimal_sep=decimal_sep,
                thousand_sep=thousand_sep,
                type_width=type_width,
                type_precision=type_precision,
            )
            replacements.append(f'{expr} AS "{name}"')

        new_rel = self._rel.select(f"* REPLACE({', '.join(replacements)})")
        op = f"as_type({list(columns.keys())})"
        if inplace:
            self._rel = new_rel
            self._reset()
            self._append_op(op)
            return self
        return self._create_from_relation(new_rel, op=op)

    def toPandas(self) -> pd.DataFrame:
        dtypes: dict[str, DuckDBPyType] = self.dtypes
        rel = self._rel
        for name, dtype in dtypes.items():
            if str(dtype).upper().startswith("GEOMETRY"):
                rel = rel.select(f'* REPLACE(st_astext("{name}") AS "{name}")')
        return rel.df()

    def toGeoPandas(self, geometry: str = "geometry", crs: str = "EPSG:4326") -> gpd.GeoDataFrame:
        dtypes: dict[str, DuckDBPyType] = self.dtypes
        rel = self._rel
        for name, dtype in dtypes.items():
            if str(dtype).upper().startswith("GEOMETRY"):
                rel = rel.select(f'* REPLACE(st_astext("{name}") AS "{name}")')
        df = rel.df()
        gdf = gpd.GeoDataFrame(df.drop(columns=[geometry]), geometry=gpd.GeoSeries.from_wkt(df[geometry]), crs=crs)
        return gdf

    def toPandasOrGeoPandas(
        self, geometry: str | None = "geometry", crs: str | None = "EPSG:4326"
    ) -> pd.DataFrame | gpd.GeoDataFrame | None:
        dtypes: dict[str, DuckDBPyType] = self.dtypes
        rel = self._rel
        geometry_founds: list[str] = []
        for name, dtype in dtypes.items():
            if str(dtype).upper() in ("GEOMETRY",):
                rel = rel.select(f'* REPLACE(st_astext("{name}") AS "{name}")')
                geometry_founds.append(name)
        df = rel.df()
        if not geometry_founds:
            return df
        else:
            if geometry is None:
                geometry = geometry_founds[0]
                warnings.warn(f'Geometry column "{geometry}" found. Will use to create GeoDataFrame.')
            if geometry not in geometry_founds:
                warnings.warn(f'Geometry column "{geometry}" not found. Will use "{geometry_founds[0]}" instead.')
                geometry = geometry_founds[0]
            if crs is None:
                crs = "EPSG:4326"
                warnings.warn(f'CRS not specified. Will use "{crs}" as default.')
            gdf = gpd.GeoDataFrame(df.drop(columns=[geometry]), geometry=gpd.GeoSeries.from_wkt(df[geometry]), crs=crs)
            return gdf

    def _norm(self, t: str | None) -> str:
        if t is None:
            return ""
        return t.strip().upper()

    def _type_mapping(self, schema_type: str | DuckDBPyType | None) -> str:
        # mappa tipi da schema a tipi SQL standard (per il cast)
        if schema_type is None:
            return ""
        if isinstance(schema_type, DuckDBPyType):
            schema_type = str(schema_type)
        t = self._norm(schema_type)
        t = DUCKDB_TYPE_ALIASES.get(t.lower(), schema_type)
        # fallback: ritorna il tipo così com'è, sperando sia riconosciuto dal DB
        return t

    def _apply_type(self, field: SchemaField | None, expr: str, current_type: str, target_type: str) -> str:
        if field is None:
            return self._as_type(expr, current_type, target_type)
        return self._as_type(
            expr,
            current_type,
            target_type,
            date_format=field.format,
            timezone=field.tz,
            decimal_sep=field.decimal_sep,
            thousand_sep=field.thousand_sep,
            type_width=field.type_width,
            type_precision=field.type_precision,
        )

    def _parse_geometry_type(self, geom_type: str) -> tuple[str, str | None]:
        if geom_type.upper().startswith("GEOMETRY"):
            if "(" in geom_type and ")" in geom_type:
                inner = geom_type[geom_type.find("(") + 1 : geom_type.rfind(")")]
                parts = [p.strip() for p in inner.split(",")]
                base_type = parts[0].upper()
                srid = None
                for part in parts[1:]:
                    if part.upper().startswith("SRID="):
                        try:
                            srid = part[5:]
                        except ValueError:
                            pass
                    else:
                        srid = part
                if srid is not None and srid.isdigit():
                    srid = f"EPSG:{srid}"
                else:
                    srid = None
                return base_type, srid
            else:
                return "GEOMETRY", None
        else:
            return "GEOMETRY", None

    def _additional_fields(
        self, new_df: GataFrame, fields: list[AdditionalSchemaField] | list[list[AdditionalSchemaField]] | None
    ) -> GataFrame:
        if fields is None:
            return new_df
        sequence: list[AdditionalSchemaField] = []
        while fields:
            field: AdditionalSchemaField | list[AdditionalSchemaField] = fields.pop(0)
            if isinstance(field, AdditionalSchemaField):
                sequence.append(field)
            else:
                if sequence:
                    new_df = self._additional_fields(new_df, [sequence])
                    sequence.clear()
                cols_replace: list[str] = []
                cols_add: list[str] = []
                for f in field:
                    name = f.name.strip()
                    target_type = self._type_mapping(f.type)
                    expr = f.expr
                    if expr.strip() == "":
                        expr = "NULL"
                    expr = f"CAST({expr} AS {target_type})"
                    if name in new_df.columns:
                        cols_replace.append(f'{expr} AS "{name}"')
                    else:
                        cols_add.append(f'{expr} AS "{name}"')
                proj = "* "
                if cols_replace:
                    proj += "REPLACE(" + ", ".join(cols_replace) + ") "
                if cols_add:
                    proj += ", " + ", ".join(cols_add)
                new_df = new_df.select(proj)
        if sequence:
            new_df = self._additional_fields(new_df, [sequence])
            sequence.clear()
        return new_df

    def __apply_mapping(self, mapping: dict[str, GeneratorSpec | str | None] | None) -> GataFrame:
        dict_curr_types: dict[str, DuckDBPyType] = self.dtypes
        columns = self.columns
        if mapping is None or len(mapping) == 0:
            return self
        add_fields: list[str] = []
        for new_name, or_name in mapping.items():
            if new_name == or_name:
                continue
            generator_spec: GeneratorSpec | str | None = or_name
            if generator_spec is None:
                if new_name not in columns:
                    add_fields.append(f'NULL AS "{new_name}"')
            elif isinstance(generator_spec, str):
                add_fields.append(f'"{generator_spec}" AS "{new_name}"')
                if or_name in columns:
                    columns.remove(generator_spec)
            else:
                expr = generator_spec.expr
                if expr in columns:
                    columns.remove(expr)
                target_type = self._type_mapping(generator_spec.type)
                curr_type = dict_curr_types.get(new_name, None)
                if curr_type != target_type:
                    expr = self._apply_type(None, expr, str(curr_type), target_type)
                else:
                    pass
                add_fields.append(f'{expr} AS "{new_name}"')
        expr = ",".join(f'"{c}"' for c in columns) + " "
        if add_fields:
            expr += "," + ", ".join(add_fields)
        return self.select(expr)

    def get_schema(
        self,
        fields_attributes: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        reader: dict[str, Any] | None = None,
    ) -> DataSchema:
        name = self._description or self._alias or f"relation_{self._uuid}"
        schema: dict[str, Any] = {
            "fields": [],
            "additional_fields": [],
            "id": None,
            "project": [],
            "metadata": {
                "description": f"Schema of relation '{name}'",
                "version": self._version,
                "operations": self._ops.copy(),
            },
            "reader": {},
        }
        fields_attributes = fields_attributes or {}
        for name, dtype in self.dtypes.items():
            field: dict[str, Any] = {"name": name, "type": str(dtype), "metadata": {}, "nullable": True}
            field.update(fields_attributes.get(name, {}))
            if dtype in ("TIMESTAMP", "TIMESTAMPTZ"):
                field.setdefault("tz", "Etc/UTC")
                field.setdefault("format", "%Y-%m-%d %H:%M:%S")
            if dtype in ("GEOMETRY",):
                field.setdefault("crs", "ESPG:4326")
            schema["fields"].append(field)
            schema["project"].append(name)
        for k, v in (metadata or {}).items():
            schema["metadata"][k] = v
        for k, v in (reader or {}).items():
            schema["reader"][k] = v

        return DataSchema.model_validate(schema)

    def apply_schema(self, schema: dict[str, Any] | DataSchema, inplace: bool = False) -> GataFrame:

        new_df: GataFrame | None = self

        if isinstance(schema, dict):
            schema = DataSchema.model_validate(schema)

        # mapping e fields sono interdipendenti, se c'è il mapping, allora i campi devono essere in forma di dict per poter applicare i rename e i type conversion correttamente. Se invece i campi sono in forma di lista, allora si assume che l'ordine dei campi sia quello del dataframe e si applicano solo i type conversion (e skip) senza rename, ignorando il mapping.
        if schema.mapping is not None and len(schema.mapping) > 0:
            if schema.fields is None or len(schema.fields) == 0 or isinstance(schema.fields, dict):
                new_df = self.__apply_mapping(schema.mapping)

        # rename e application type
        df_cols: list[str] = [c for c in new_df.columns]
        curr_types: list[DuckDBPyType] = new_df.types
        projections: list[str] = []
        if schema.fields is None or len(schema.fields) == 0:
            if schema.import_others:
                projections.append("*")
        elif isinstance(schema.fields, dict):
            dict_curr_types: dict[str, DuckDBPyType] = dict(zip(df_cols, curr_types))
            inserted: set[str] = set()
            for or_name in df_cols:
                field: SchemaField | None = schema.fields.get(or_name, None)
                if field is None:
                    if schema.import_others:
                        projections.append(f'"{or_name}"')
                        inserted.add(or_name)
                    continue
                if field.name is None:
                    name = or_name
                else:
                    name = field.name.strip()
                if not bool(name):
                    name = or_name
                if field.skip:
                    continue
                else:
                    curr_type = self._type_mapping(dict_curr_types[or_name])
                    target_types = self._type_mapping(field.type)
                    if curr_type != target_types:
                        expr = self._apply_type(field, f'"{or_name}"', curr_type, target_types)
                    else:
                        expr = f'"{or_name}"'
                    if not field.nullable:
                        if isinstance(field.default, str):
                            escaped_default = field.default.replace("'", "\\'")
                            def_value = f"'{escaped_default}'"
                        else:
                            def_value = field.default
                        if def_value is None:
                            expr = f"COALESCE({expr}, {def_value})"
                    projections.append(f'{expr} AS "{name}"')
                    inserted.add(name)
            for name, field in schema.fields.items():
                if name not in inserted:
                    if field.skip:
                        continue
                    else:
                        target_types = self._type_mapping(field.type)
                        if not field.nullable:
                            if isinstance(field.default, str):
                                escaped_default = field.default.replace("'", "\\'")
                                def_value = f"'{escaped_default}'"
                            else:
                                def_value = field.default
                            if def_value is None:
                                expr = f'CAST(NULL AS {target_types}) AS "{name}"'
                            else:
                                expr = f'{def_value} AS "{name}"'
                        else:
                            expr = f'CAST(NULL AS {target_types}) AS "{name}"'
                        projections.append(expr)
        else:  # isinstance(schema.fields, list):
            if len(df_cols) != len(schema.fields):
                raise ValueError("DataFrame has different number of columns from schema.")
            for pos, (field, df_col) in enumerate(zip(schema.fields, df_cols)):
                if field.name is None:
                    name = df_col
                else:
                    name = field.name.strip()
                if name == "" or field.skip:
                    continue
                else:
                    curr_type = self._type_mapping(curr_types[pos])
                    target_types = self._type_mapping(field.type)
                    if curr_type != target_types:
                        expr = self._apply_type(field, f'"{df_col}"', curr_type, target_types)
                    else:
                        expr = f'"{df_col}"'
                    if not field.nullable:
                        if isinstance(field.default, str):
                            escaped_default = field.default.replace("'", "\\'")
                            def_value = f"'{escaped_default}'"
                        else:
                            def_value = field.default
                        expr = f"COALESCE({expr}, {def_value})"
                    projections.append(f'{expr} AS "{name}"')
        if len(projections) == 0:
            raise RuntimeError("No column to apply schema.")

        if schema.fields is None or len(schema.fields) == 0:
            pass
        elif isinstance(schema.fields, list):
            # print(projections)
            new_df = new_df.select(projections)

            # pre filter
            if schema.pre_filter is not None and schema.pre_filter != "":
                new_df = new_df.filter(schema.pre_filter)

            if schema.pre_limit is not None:
                new_df = new_df.limit(schema.pre_limit)
        else:  # isinstance(schema.fields, list):
            if schema.pre_filter is not None and schema.pre_filter != "":
                new_df = new_df.filter(schema.pre_filter)

            if schema.pre_limit is not None:
                new_df = new_df.limit(schema.pre_limit)

            new_df = new_df.select(projections)

        projections: list[str] = []
        # curr_types: list[DuckDBPyType] = new_df.types
        fields: dict[str, SchemaField] = {}
        if isinstance(schema.fields, list):
            fields = {f.name.strip(): f for f in schema.fields if f.name is not None}
        elif isinstance(schema.fields, dict):
            fields = {f.name.strip(): f for f in schema.fields.values() if f.name is not None}

        df_cols = new_df.columns
        curr_types = new_df.types
        dict_curr_types: dict[str, DuckDBPyType] = dict(zip(df_cols, curr_types))
        # application generator
        for name, curr_type in dict_curr_types.items():
            field: SchemaField | None = fields.get(name, None)
            if (
                field is None
                or field.generator is None
                or (isinstance(field.generator, str) and field.generator.strip() == "")
            ):
                projections.append(f'"{name}"')
                continue
            else:
                generator: GeneratorSpec | str = field.generator
                if isinstance(generator, str):
                    projections.append(f'{generator} AS "{name}"')
                else:
                    expr = generator.expr
                    target_type = self._type_mapping(generator.type)
                    if curr_type != target_type:
                        expr = self._apply_type(field, expr, str(curr_type), target_type)
                    else:
                        pass
                    projections.append(f'{expr} AS "{name}"')
        # print(projections)
        new_df = new_df.select(projections)

        # add additional field
        new_df = self._additional_fields(new_df, schema.additional_fields)  # pyright: ignore[reportArgumentType]

        if schema.filter is not None and schema.filter != "":
            new_df = new_df.filter(schema.filter)

        if schema.limit is not None:
            new_df = new_df.limit(schema.limit)

        if schema.project is not None:
            new_df = new_df.select(schema.project)

        if inplace:
            op = "apply_schema()"
            self._rel = new_df._rel
            self._reset()
            self._append_op(op)
            return self
        else:
            return new_df

    @staticmethod
    def qident(name: str | None, cols: set[str] | list[str] | None = None) -> str:
        if name is None:
            return ""
        if cols is None:
            return '"' + str(name).replace('"', '""') + '"'
        if name.strip() in cols:
            return '"' + str(name).replace('"', '""') + '"'
        return name

    @staticmethod
    def qname(*parts: str) -> str:
        return ".".join(GataFrame.qident(p, None) for p in parts if p)


__all__ = ["GataFrame", "GFDataType"]
