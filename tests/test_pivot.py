import pytest

import pyspark.sql.functions as F

# inspiration: https://stackoverflow.com/questions/51820994/groupeddata-object-has-no-attribute-show-when-doing-doing-pivot-in-spark-dat/51821027
def test_pivot(spark):
    data = [
        ("123", "McDonalds"),
        ("123", "Starbucks"),
        ("123", "McDonalds"),
        ("777", "McDonalds"),
        ("777", "McDonalds"),
        ("777", "Dunkin")
    ]
    df = spark.createDataFrame(data, ["customer_id", "name"])
    df.show()
    df.groupBy("name").show()
    # df.groupBy("name").pivot("customer_id").count().show()
    # df.groupBy("customer_id").pivot("name").count().show()


