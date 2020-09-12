import pytest

from chispa import assert_column_equality, assert_approx_column_equality
import pyspark.sql.functions as F
from pyspark.sql.types import *

def test_cast_arraytype(spark):
    data = [
        (['200', '300'], [200, 300]),
        (['400'], [400]),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["nums", "expected"])\
        .withColumn("actual", F.col("nums").cast(ArrayType(IntegerType(), True)))
    assert_column_equality(df, "actual", "expected")

