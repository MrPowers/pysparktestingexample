import pytest

import pyspark.sql.functions as F
from pyspark.sql.types import *
import quinn
from pyspark.sql.window import Window


def test_fake_test(spark):
    df = spark.createDataFrame([(1, 5), (2, 9), (3, 3), (4, 1)], ["mvv", "count"])
    mvv = list(df.select('mvv').toPandas()['mvv'])
    df.show()


def test_pandas_approach(spark):
    df = spark.createDataFrame([(1, 5), (2, 9), (3, 3), (4, 1)], ["mvv", "count"])
    mvv = list(df.select('mvv').toPandas()['mvv'])
    assert mvv == [1, 2, 3, 4]


def test_flatmap_collect(spark):
    df = spark.createDataFrame([(1, 5), (2, 9), (3, 3), (4, 1)], ["mvv", "count"])
    mvv = df.select('mvv').rdd.flatMap(lambda x: x).collect()
    assert mvv == [1, 2, 3, 4]


def test_flatmap_toLocalIterator(spark):
    df = spark.createDataFrame([(1, 5), (2, 9), (3, 3), (4, 1)], ["mvv", "count"])
    mvv = list(df.select('mvv').rdd.flatMap(lambda x: x).toLocalIterator())
    assert mvv == [1, 2, 3, 4]


def test_rdd_map(spark):
    df = spark.createDataFrame([(1, 5), (2, 9), (3, 3), (4, 1)], ["mvv", "count"])
    mvv = df.select('mvv').rdd.map(lambda row : row[0]).collect()
    assert mvv == [1, 2, 3, 4]


def test_list_comprehension_map(spark):
    df = spark.createDataFrame([(1, 5), (2, 9), (3, 3), (4, 1)], ["mvv", "count"])
    mvv = [row[0] for row in df.select('mvv').collect()]
    assert mvv == [1, 2, 3, 4]


def test_list_comprehension_toLocalIterator(spark):
    df = spark.createDataFrame([(1, 5), (2, 9), (3, 3), (4, 1)], ["mvv", "count"])
    mvv = [r[0] for r in df.select('mvv').toLocalIterator()]
    assert mvv == [1, 2, 3, 4]


def test_pandas_to_two_lists(spark):
    df = spark.createDataFrame([(1, 5), (2, 9), (3, 3), (4, 1)], ["mvv", "count"])
    collected = df.select('mvv', 'count').toPandas()
    mvv = list(collected['mvv'])
    count = list(collected['count'])
    assert mvv == [1, 2, 3, 4]
    assert count == [5, 9, 3, 1]

