import pytest

import pyspark.sql.functions as F
from pyspark.sql.types import *

def title(x,y):
   if y:
       x = x.title()
   return x


def test_udf(spark):
    df = spark.createDataFrame([
        ["aaa","1"],
        ["bbb","2"],
        ["ccc","5"]
    ]).toDF("text","id")

    title_udf = F.udf(title, StringType())
    spark.udf.register('title_udf', title_udf)

    df.withColumn('text_title',title_udf('text',F.lit(True))).show()

