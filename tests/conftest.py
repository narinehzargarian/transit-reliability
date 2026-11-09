import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))


@pytest.fixture(scope="session")
def spark():
    from transit_reliability.spark_job import spark_session

    session = spark_session("local[2]")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
