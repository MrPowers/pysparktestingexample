import pytest

from pysparktestingexample.functions import remove_non_word_characters, divide_by_three
from chispa import assert_column_equality, assert_approx_column_equality
import pyspark.sql.functions as F

def test_remove_non_word_characters(spark):
    data = [
        ("jo&&se", "jose"),
        ("**li**", "li"),
        ("#::luisa", "luisa"),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["name", "expected_name"])\
        .withColumn("clean_name", remove_non_word_characters(F.col("name")))
    assert_column_equality(df, "clean_name", "expected_name")


def test_remove_non_word_characters_nice_error(spark):
    data = [
        ("matt7", "matt"),
        ("bill&", "bill"),
        ("isabela*", "isabela"),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["name", "expected_name"])\
        .withColumn("clean_name", remove_non_word_characters(F.col("name")))
    # with pytest.raises(ColumnsNotEqualError) as e_info:
    assert_column_equality(df, "clean_name", "expected_name")


def test_divide_by_three(spark):
    data = [
        (1, 0.33),
        (2, 0.66),
        (3, 1.0),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["num", "expected"])\
        .withColumn("num_divided_by_three", divide_by_three(F.col("num")))
    assert_approx_column_equality(df, "num_divided_by_three", "expected", 0.01)


def test_divide_by_three_error(spark):
    data = [
        (5, 1.66),
        (6, 2.0),
        (7, 4.33),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["num", "expected"])\
        .withColumn("num_divided_by_three", divide_by_three(F.col("num")))
    assert_approx_column_equality(df, "num_divided_by_three", "expected", 0.01)

