import pandas as pd

from gataframe import GataFrame, connect


def test_engine_reads_pandas_dataframe() -> None:
    engine = connect(extensions=None)
    try:
        gf = engine.read(pd.DataFrame({"id": [1, 2], "value": [10, 20]}))

        assert isinstance(gf, GataFrame)
        assert gf.shape == (2, 2)
        assert gf.columns == ["id", "value"]
    finally:
        engine.close()


def test_gata_frame_column_operations_are_chainable() -> None:
    engine = connect(extensions=None)
    try:
        gf = engine.read(pd.DataFrame({"id": [1, 2], "value": [10, 20]}))

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
