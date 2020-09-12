import pytest

import pyspark.sql.functions as F
from pyspark.sql.types import *
import quinn
from pyspark.sql.window import Window

def test_random_value_from_array(spark):
    df = spark.createDataFrame(
        [
            (['a', 'b', 'c'],),
            (['a', 'b', 'c', 'd'],),
            (['x'],),
            ([None],)
        ],
        [
            "letters"
        ]
    )
    df.show()
    actual_df = df.withColumn(
        "random_letter",
        quinn.array_choice(F.col("letters"))
    )
    actual_df.show()


def test_random_value_from_columns(spark):
    df = spark.createDataFrame(
        [
            (1, 2, 3),
            (4, 5, 6),
            (7, 8, 9),
            (10, None, None),
            (None, None, None)
        ],
        ["num1", "num2", "num3"]
    )
    # df.show()
    actual_df = df.withColumn(
        "random_number",
        quinn.array_choice(F.array(F.col("num1"), F.col("num2"), F.col("num3")))
    )
    # actual_df.show()


def test_random_animal(spark):
    df = spark.createDataFrame([('jose',), ('maria',), (None,)], ['first_name'])
    cols = list(map(lambda col_name: F.lit(col_name), ['cat', 'dog', 'mouse']))
    actual_df = df.withColumn(
        "random_animal",
        quinn.array_choice(F.array(*cols))
    )
    # actual_df.show()


def test_random_values_from_column(spark):
    df = spark.createDataFrame([(123,), (245,), (12,), (234,)], ['id']).withColumn("rand", F.rand())

    # window = Window.orderBy(df['rand'].desc())
    # actual_df = df.select('*', F.rank().over(window).alias('rank'))\
        # .filter(F.col('rank') <= 3)
    # print(list(map(lambda row: row[0], actual_df.select('id').collect())))
    # actual_df.show()
    # actual_df.explain()

    df2 = df.select('id').orderBy(F.rand()).limit(3)
    df2.show()
    df2.explain()

    print(list(map(lambda row: row[0], df.rdd.takeSample(False, 3))))
