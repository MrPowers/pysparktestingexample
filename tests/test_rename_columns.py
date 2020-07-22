import pysparktestingexample.stackoverflow as SO
from chispa import assert_df_equality
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
import quinn
from quinn.extensions import *

def test_rename_columns_toDF(spark):
    schema = StructType([
        StructField("i.like.cheese", StringType(), True),
        StructField("yummy.stuff", StringType(), False)]
    )
    data = [("jose", "a"), ("li", "b"), (None, "c")]
    df = spark.createDataFrame(data, schema)
    actual_df = df.toDF(*(c.replace('.', '_') for c in df.columns))
    # actual_df.show()
    # actual_df.explain(True)


def test_rename_columns_quinn(spark):
    schema = StructType([
        StructField("i.like.cheese", StringType(), True),
        StructField("yummy.stuff", StringType(), False)]
    )
    data = [("jose", "a"), ("li", "b"), (None, "c")]
    df = spark.createDataFrame(data, schema)
    df.show()
    def dot_to_underscore(s):
        return s.replace('.', '_')
    actual_df = quinn.with_columns_renamed(fun)(df)
    actual_df.show()
    actual_df.explain(True)


def test_rename_some_columns_quinn(spark):
    mapping = {"chips": "french_fries", "petrol": "gas"}
    def british_to_american(s):
      return mapping[s]
    def change_col_name(s):
      return s in mapping
    schema = StructType([
        StructField("chips", StringType(), True),
        StructField("hi", StringType(), True),
        StructField("petrol", StringType(), True)]
    )
    data = [("potato", "hola!", "disel")]
    source_df = spark.createDataFrame(data, schema)
    actual_df = quinn.with_some_columns_renamed(british_to_american, change_col_name)(source_df)
    # actual_df.show()
    # actual_df.explain(True)


def test_rename_columns_bad_implementation(spark):
    def rename_cols(rename_df):
        for column in rename_df.columns:
            new_column = column.replace('.','_')
            rename_df = rename_df.withColumnRenamed(column, new_column)
        return rename_df
    schema = StructType([
        StructField("i.like.cheese", StringType(), True),
        StructField("yummy.stuff", StringType(), False)]
    )
    data = [("jose", "a"), ("li", "b"), (None, "c")]
    df = spark.createDataFrame(data, schema)
    actual_df = rename_cols(df)
    # actual_df.show()
    # actual_df.explain(True)


def test_rename_columns_quinn(spark):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("emp.city", StringType(), True),
        StructField("emp.sal", StringType(), True)]
    )
    data = [("12", "bob", "New York", "80"), ("99", "alice", "Atlanta", "90")]
    df = spark.createDataFrame(data, schema)
    # df.show()
    def dots_to_underscores(s):
        return s.replace('.', '_')
    actual_df = df.transform(quinn.with_columns_renamed(dots_to_underscores))
    # actual_df.show()
    # actual_df.explain(True)


def test_rename_some_columns_quinn(spark):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("emp.city", StringType(), True),
        StructField("emp.sal", StringType(), True)]
    )
    data = [("12", "bob", "New York", "80"), ("99", "alice", "Atlanta", "90")]
    df = spark.createDataFrame(data, schema)
    def dots_to_underscores(s):
        return s.replace('.', '_')
    def change_col_name(s):
      return '.' in s
    actual_df = df.transform(quinn.with_some_columns_renamed(dots_to_underscores, change_col_name))
    # actual_df.show()
    # actual_df.explain(True)


def test_rename_two_columns_with_quinn(spark):
    df = spark.createDataFrame([(1,2), (3,4)], ['x1', 'x2'])
    def rename_col(s):
        mapping = {'x1': 'x3', 'x2': 'x4'}
        return mapping[s]
    actual_df = df.transform(quinn.with_columns_renamed(rename_col))
    # actual_df.show()
    # actual_df.explain(True)


def rename_columns(df, columns):
    for old_name, new_name in columns.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


def test_rename_two_columns_with_quinn(spark):
    df = spark.createDataFrame([(1,2), (3,4)], ['x1', 'x2'])
    def rename_col(s):
        mapping = {'x1': 'x3', 'x2': 'x4'}
        return mapping[s]
    actual_df = rename_columns(df, {'x1': 'x3', 'x2': 'x4'})
    # actual_df.show()
    # actual_df.explain(True)
