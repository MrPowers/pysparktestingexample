import pysparktestingexample.stackoverflow as SO
from chispa import assert_df_equality
import pyspark.sql.functions as F

def test_column_names(spark):
    source_data = [
        ("jose", "oak", "switch")
    ]
    source_df = spark.createDataFrame(source_data, ["some first name", "some.tree.type", "a gaming.system"])
    actual_df = SO.column_names(source_df)
    expected_data = [
        ("jose", "oak", "switch")
    ]
    expected_df = spark.createDataFrame(expected_data, ["some_&first_&name", "some_$tree_$type", "a_&gaming_$system"])
    assert_df_equality(actual_df, expected_df)

