import pytest

import pyspark.sql.functions as F
import quinn

# question motivation: https://stackoverflow.com/questions/63103302/creating-dictionary-from-pyspark-dataframe-showing-outofmemoryerror-java-heap-s/63103739#63103739
def test_to_dictionary(spark):
    data = [
        ("BOND-9129450", "90cb"),
        ("BOND-1742850", "d5c3"),
        ("BOND-3211356", "811f"),
        ("BOND-7630290", "d5c3"),
        ("BOND-7175508", "90cb"),
    ]
    df = spark.createDataFrame(data, ["id", "hash_of_cc_pn_li"])
    agg_df = df.groupBy("hash_of_cc_pn_li").agg(F.max("hash_of_cc_pn_li").alias("hash"), F.collect_list("id").alias("id"))
    res = quinn.two_columns_to_dictionary(agg_df, "hash", "id")
    print(res)
