import pytest

import chispa
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Row

def test_example_error(spark):
    # schema = StructType([StructField("country.name", StringType(), True)])
    df = spark.createDataFrame(
        [("china", "asia"), ("colombia", "south america")],
        ["country.name", "continent"]
    )
    df.select("`country.name`").show()

def test_remove_non_word_characters(spark):
    schema = StructType([
        StructField("person.name", StringType(), True),
        StructField("person", StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)]))
    ])
    data = [
        ("charles", Row("chuck", 42)),
        ("lawrence", Row("larry", 73))
    ]
    df = spark.createDataFrame(data, schema)
    df.show()

    cols = ["person", "person.name", "`person.name`"]
    print("***")
    df.select(cols).show()

    clean_df = df.toDF(*(c.replace('.', '_') for c in df.columns))
    clean_df.show()
    clean_df.select("person_name", "person.name", "person.age").show()
