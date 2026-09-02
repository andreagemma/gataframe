from gataframe import Serializer


def test_serializer_round_trips_uncompressed_data() -> None:
    payload = {"name": "gataframe", "values": [1, 2, 3]}

    dumped = Serializer.dumps(payload)

    assert Serializer.loads(dumped) == payload


def test_serializer_round_trips_gzip_data() -> None:
    payload = {"answer": 42}

    dumped = Serializer.dumps(payload, compression=Serializer.CNAME_GZIP)

    assert Serializer.loads(dumped, compression=Serializer.CNAME_GZIP) == payload
