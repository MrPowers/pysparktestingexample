import pytest

import pyspark.sql.functions as F
from pyspark.sql.types import *
import quinn
from pyspark.sql.window import Window

def test_flatmap(spark):
    df = spark.read.text("tests/resources/words.txt")
    words = df.rdd.flatMap(lambda row: row[0].split(" ")).collect()
    print(words)


# Code	100,000	100,000,000
# df.select("col_name").rdd.flatMap(lambda x: x).collect()	0.4	55.3
# list(df.select('col_name').toPandas()['col_name'])	0.4	10.5
# df.select('col_name').rdd.map(lambda row : row[0]).collect()	0.9	69
# [row[0] for row in df.select('col_name').collect()]	1.0	OOM
# [row[0] for row in mid_df.select('meta_sender_name').toLocalIterator()]	1.2	
