from importlib.metadata import version

import gataframe


def test_version_matches_package_metadata() -> None:
    assert gataframe.__version__ == version("gataframe")
