"""
pytest fixtures that can be reused across tests. the filename needs to be conftest.py
"""

from os.path import dirname, realpath, join
import sys
sys.path.append(join(dirname(realpath(__file__)), '../'))

# make sure env variables are set correctly
import findspark  # this needs to be the first import

findspark.init()

import logging  # noqa
from typing import List, Dict  # noqa

import pytest  # noqa
from pyspark.sql import Row, SparkSession  # noqa
from pyspark.sql.types import StructType  # noqa


def quiet_py4j():
    """ turn down spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope='session')
def spark_session():
    """
    Fixture for creating a shared SparkSession to use when testing code that primarily
    utilizes Spark's Structured API (DataFrames, SQL tables and views).
    """
    spark = (
        SparkSession.builder.master('local[2]')
        .appName('SparkSession Transformations Tests')
        .getOrCreate()
    )
    spark.conf.set("spark.sql.execution.arrow.enabled", True)
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    yield spark
    spark.stop()


@pytest.fixture
def spark_context(spark_session):
    return spark_session.sparkContext


@pytest.fixture(autouse=True)
def no_spark_stop(monkeypatch):
    """
    Disable calls to `spark.stop()` on the shared spark session during integration tests.
    """
    def nop(*args, **kwargs):
        print('Prevented spark.stop for improved test performance.')
    monkeypatch.setattr('pyspark.sql.SparkSession.stop', nop)


@pytest.fixture(scope='session')
def dataframe_factory(spark_session):
    """
    Factory function for creating sample dataframes in unit tests.

    The goal is to provide a simple and convenient way to generate sample dataframes that match data source schemas,
    and have the ability to replace only specific values related to the individual transformation step being tested.

    Accepts a dictionary containing column names as keys and sample values to represent a single row in the dataframe.
    This data structure is used as the `base` for testing transformation logic. Additionally accepts an optional
    `snippets` parameter that can be used to build test dataframes containing any number of rows that override the
    `base` data with updated values.

    Example use:

        base_data = {'business_unit': '111', 'containerboard': 'Y'}
        base_df = dataframe_factory(base_data)

        +-------------+--------------+
        |business_unit|containerboard|
        +-------------+--------------+
        |          111|             Y|
        +-------------+--------------+

        snippets = [{'business_unit': '222'}, {'business_unit': '333'}]
        snippets_df = dataframe_factory(base_data, snippets)

        +-------------+--------------+
        |business_unit|containerboard|
        +-------------+--------------+
        |          222|             Y|
        |          333|             Y|
        +-------------+--------------+

    Uses a closure to inject the test environment's `SparkSession` dependency, which allows use of the factory function
    as a session-scoped pytest fixture without affecting the call signature.
    """

    def _create_dataframe(base: Dict, snippets: List[Dict] = None, schema: StructType = None):
        if not snippets:
            snippets = [dict()]

        test_rows = []
        for snippet in snippets:
            sample = base.copy()
            if snippet is None:
                snippet = {}
            sample.update(snippet)
            test_rows.append(Row(**sample))

        return spark_session.createDataFrame(test_rows, schema)

    return _create_dataframe
