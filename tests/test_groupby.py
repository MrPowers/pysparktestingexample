import pytest

import pyspark.sql.functions as F
from pyspark.sql.types import *

def test_group_by(spark):
    df = spark.createDataFrame([[1, 'r1', 1],
        [1, 'r2', 0],
        [1, 'r2', 1],
        [2, 'r1', 1],
        [3, 'r1', 1],
        [3, 'r2', 1],
        [4, 'r1', 0],
        [5, 'r1', 1],
        [5, 'r2', 0],
        [5, 'r1', 1]], schema=['cust_id', 'req', 'req_met'])
    df.groupby(['cust_id', 'req']).agg(F.max(F.col('req_met')).alias('max_req_met')).show()

    # gr = df.groupby(['cust_id', 'req'])\
        # .agg(F.sum(F.col('req_met')).alias('sum_req_met'))\
        # .withColumn('sum_req_met2', F.when(F.col('sum_req_met') >= 1, 1).otherwise(0))
    # gr.show()
    # df_req = gr.groupby('cust_id').agg(F.sum('sum_req_met').alias('sum_req'), F.count('req').alias('n_req'))
    # df_req.show()
    # df_req.filter(df_req['sum_req'] == df_req['n_req'])[['cust_id']].orderBy('cust_id').show()

    # query = """aggregate(
        # `{col}`,
        # CAST(0.0 AS double),
        # (acc, x) -> acc + x,
        # acc -> acc / size(`{col}`)
    # ) AS  `avg_{col}`""".format(col="all_numbers")

    # q = """forall(
        # `requirements_met`,
        # c -> c >= 1
    # ) AS `all_requirements_met`"""

    # gr2 = df\
        # .groupby(['cust_id', 'req'])\
        # .agg(F.sum(F.col('req_met')).alias('sum_req_met'))\
    # gr2.show()

