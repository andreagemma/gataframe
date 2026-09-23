import pandas as pd

from gataframe import GataFrame, connect


def test_engine_reads_pandas_dataframe() -> None:
    engine = connect(extensions=None)
    try:
        gf = engine.read(pd.DataFrame({"id": [1, 2], "value": [10, 20]}))
        assert gf is not None

        assert isinstance(gf, GataFrame)
        assert gf.shape == (2, 2)
        assert gf.columns == ["id", "value"]
    finally:
        engine.close()


def test_gata_frame_column_operations_are_chainable() -> None:
    engine = connect(extensions=None)
    try:
        gf = engine.read(pd.DataFrame({"id": [1, 2], "value": [10, 20]}))
        assert gf is not None

        result = (
            gf.withColumn("double_value", "value * 2")
            .renameColumn("double_value", "scaled_value")
            .replaceColumn("scaled_value", "scaled_value + 1")
            .filter("id = 2")
            .select("id", "scaled_value")
            .toPandas()
        )

        assert result.to_dict(orient="records") == [{"id": 2, "scaled_value": 41}]
    finally:
        engine.close()


def test_engine_reads_dict_and_list_sources() -> None:
    engine = connect(extensions=None)
    try:
        from_dict = engine.read({"id": [1, 2], "value": [10, 20]})
        assert from_dict is not None
        assert from_dict.shape == (2, 2)

        from_list = engine.read([{"id": 1, "value": 10}, {"id": 2, "value": 20}])
        assert from_list is not None
        assert from_list.shape == (2, 2)
    finally:
        engine.close()


def test_pandas_dtype_mapping_is_exposed() -> None:
    engine = connect(extensions=None)
    try:
        gf = engine.read(pd.DataFrame({"id": [1, 2], "value": [10.0, 20.0], "name": ["a", "b"]}))
        assert gf is not None

        pandas_types = gf.pandasDType
        assert pandas_types["id"] in {"Int32", "Int64"}
        assert pandas_types["value"] == "Float64"
        assert pandas_types["name"] == "str"
    finally:
        engine.close()
