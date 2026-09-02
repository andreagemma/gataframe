import pytest
from pydantic import ValidationError

from gataframe import DataSchema


def test_schema_normalizes_type_aliases() -> None:
    schema = DataSchema.model_validate(
        {
            "fields": [
                {"name": "id", "type": "int", "nullable": False, "default": 0},
                {"name": "tags", "type": "array(string)"},
                {"name": "scores", "type": "map(text, float)"},
            ],
            "project": ["id", "tags", "scores"],
        }
    )

    assert isinstance(schema.fields, list)
    assert schema.fields[0].type == "INTEGER"
    assert schema.fields[1].type == "LIST(VARCHAR)"
    assert schema.fields[2].type == "MAP(VARCHAR, DOUBLE)"


def test_schema_rejects_duplicate_field_names() -> None:
    with pytest.raises(ValidationError, match="Duplicati"):
        DataSchema.model_validate(
            {
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "id", "type": "string"},
                ]
            }
        )
