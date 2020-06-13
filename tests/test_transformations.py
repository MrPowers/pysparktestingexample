import pysparktestingexample.transformations as T
import pysparktestingexample.string_helpers as SH
from chispa import assert_df_equality
import pyspark.sql.functions as F

def test_sort_columns_asc(spark):
    source_data = [
        ("jose", "oak", "switch"),
        ("li", "redwood", "xbox"),
        ("luisa", "maple", "ps4"),
    ]
    source_df = spark.createDataFrame(source_data, ["name", "tree", "gaming_system"])

    actual_df = T.sort_columns(source_df, "asc")

    expected_data = [
        ("switch", "jose", "oak"),
        ("xbox", "li", "redwood"),
        ("ps4", "luisa", "maple"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["gaming_system", "name", "tree"])

    assert_df_equality(actual_df, expected_df)


def test_sort_columns_desc(spark):
    source_data = [
        ("jose", "oak", "switch"),
        ("li", "redwood", "xbox"),
        ("luisa", "maple", "ps4"),
    ]
    source_df = spark.createDataFrame(source_data, ["name", "tree", "gaming_system"])

    actual_df = T.sort_columns(source_df, "desc")

    expected_data = [
        ("oak", "jose", "switch"),
        ("redwood", "li", "xbox"),
        ("maple", "luisa", "ps4"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["tree", "name", "gaming_system"])

    assert_df_equality(actual_df, expected_df)


def test_modify_column_names_error(spark):
    source_data = [
        ("jose", 8),
        ("li", 23),
        ("luisa", 48),
    ]
    source_df = spark.createDataFrame(source_data, ["first.name", "person.favorite.number"])

    actual_df = T.modify_column_names(source_df, SH.dots_to_underscores)

    expected_data = [
        ("jose", 8),
        ("li", 23),
        ("luisa", 48),
    ]
    expected_df = spark.createDataFrame(expected_data, ["first_name", "person_favorite_number"])

    assert_df_equality(actual_df, expected_df)

