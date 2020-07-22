from chispa import assert_df_equality
import pyspark.sql.functions as F
from pyspark.sql.types import *
import quinn
from quinn.extensions import *

def test_map_to_columns(spark):
    data = [("jose", {"a": "aaa", "b": "bbb"}), ("li", {"b": "some_letter", "z": "zed"})]
    df = spark.createDataFrame(data, ["first_name", "some_data"])
    # df.withColumn("some_data_a", F.col("some_data")["a"]).show()
    # df.show(truncate=False)
    # df.printSchema()
    df\
        .withColumn("some_data_a", F.col("some_data").getItem("a"))\
        .withColumn("some_data_b", F.col("some_data").getItem("b"))\
        .withColumn("some_data_z", F.col("some_data").getItem("z"))\
        # .show(truncate=False)
    cols = [F.col("first_name")] + list(map(
        lambda f: F.col("some_data").getItem(f).alias(str(f)),
        ["a", "b", "z"]))
    # df.select(cols).show()

    keys_df = df.select(F.explode(F.map_keys(F.col("some_data")))).distinct()
    # keys_df.show()

    keys = list(map(lambda row: row[0], keys_df.collect()))
    # print(keys)

    key_cols = list(map(lambda f: F.col("some_data").getItem(f).alias(str(f)), keys))
    # print(key_cols)

    final_cols = [F.col("first_name")] + key_cols
    # print(final_cols)

    # df.select(final_cols).show()
    # df.select(final_cols).explain(True)


def test_deep_map_to_columns(spark):
    # question inspiration: https://stackoverflow.com/questions/63020351/transform-3-level-nested-dictionary-key-values-to-pyspark-dataframe#63020351
    # schema = StructType([
        # StructField("dic", MapType(StringType(), StringType()), True)]
    # )
    data = [
        ("hi", {"Name": "David", "Age": "25", "Location": "New York", "Height": "170", "fields": {"Color": "Blue", "Shape": "Round", "Hobby": {"Dance": "1", "Singing": "2"}, "Skills": {"Coding": "2", "Swimming": "4"}}}, "bye"),
        ("hi", {"Name": "Helen", "Age": "28", "Location": "New York", "Height": "160", "fields": {"Color": "Blue", "Shape": "Round", "Hobby": {"Dance": "5", "Singing": "6"}}}, "bye"),
        ]
    df = spark.createDataFrame(data, ["greeting", "dic", "farewell"])
    res = df.select(
        F.col("dic").getItem("Name").alias(str("Name")),
        F.col("dic")["Age"].alias(str("Age"))
    )
    # res.show()
    # res.printSchema()

    # df.printSchema()

    df.select(F.col("dic").getItem("fields")).printSchema()


def test_simple_map_to_columns(spark):
    # question inpiration: https://stackoverflow.com/questions/36869134/pyspark-converting-a-column-of-type-map-to-multiple-columns-in-a-dataframe
    d = [{'Parameters': {'foo': '1', 'bar': '2', 'baz': 'aaa'}}]
    df = spark.createDataFrame(d)
    df.show(truncate=False)
    # keys_df = df.select(F.explode(F.map_keys(F.col("Parameters")))).distinct()
    # keys = list(map(lambda row: row[0], keys_df.collect()))
    # key_cols = list(map(lambda f: F.col("Parameters").getItem(f).alias(str(f)), keys))
    # df.select(key_cols).show()
    cols = list(map(
        lambda f: F.col("Parameters").getItem(f).alias(str(f)),
        ["foo", "bar", "baz"]))
    df.select(cols).show()

