from pysparktestingexample.transformations import sort_columns
from chispa.dataframe_comparer import assert_df_equality
import pyspark.sql.functions as F

from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .master("local") \
  .appName("chispa") \
  .getOrCreate()

def test_sort_columns_asc():
    source_data = [
        ("jose", "oak", "switch"),
        ("li", "redwood", "xbox"),
        ("luisa", "maple", "ps4"),
    ]
    source_df = spark.createDataFrame(source_data, ["name", "tree", "gaming_system"])

    actual_df = sort_columns(source_df, "asc")

    expected_data = [
        ("switch", "jose", "oak"),
        ("xbox", "li", "redwood"),
        ("ps4", "luisa", "maple"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["gaming_system", "name", "tree"])

    assert_df_equality(actual_df, expected_df)


def test_sort_columns_desc():
    source_data = [
        ("jose", "oak", "switch"),
        ("li", "redwood", "xbox"),
        ("luisa", "maple", "ps4"),
    ]
    source_df = spark.createDataFrame(source_data, ["name", "tree", "gaming_system"])

    actual_df = sort_columns(source_df, "desc")

    expected_data = [
        ("oak", "jose", "switch"),
        ("redwood", "li", "xbox"),
        ("maple", "luisa", "ps4"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["tree", "name", "gaming_system"])

    assert_df_equality(actual_df, expected_df)

