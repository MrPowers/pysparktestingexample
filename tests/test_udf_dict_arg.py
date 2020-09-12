import pytest

import pyspark.sql.functions as F
from pyspark.sql.types import *

# @F.udf(returnType=StringType())
# def state_abbreviation(s, mapping):
    # if s is not None:
        # return mapping[s]


# def test_udf_dict_failure(spark):
    # df = spark.createDataFrame([
        # ['Alabama',],
        # ['Texas',],
        # ['Antioquia',]
    # ]).toDF('state')
    # mapping = {'Alabama': 'AL', 'Texas': 'TX'}
    # df.withColumn('state_abbreviation', state_abbreviation(F.col('state'), spark.sparkContext.broadcast(mapping))).show()


# def working_fun(mapping_broadcasted):
    # def f(x):
        # return mapping_broadcasted.value.get(x)
    # return F.udf(f)


# def test_udf_dict_working(spark):
    # df = spark.createDataFrame([
        # ['Alabama',],
        # ['Texas',],
        # ['Antioquia',]
    # ]).toDF('state')

    # mapping = {'Alabama': 'AL', 'Texas': 'TX'}
    # b = spark.sparkContext.broadcast(mapping)

    # df.withColumn('state_abbreviation', working_fun(b)(F.col('state'))).show()



### CODE FOR A SO QUESTION

# keyword_list= [
    # ['union','workers','strike','pay','rally','free','immigration',],
    # ['farmer','plants','fruits','workers'],
    # ['outside','field','party','clothes','fashions']]

# def label_maker_topic(tokens, topic_words_broadcasted):
    # twt_list = []
    # for i in range(0, len(topic_words_broadcasted.value)):
        # count = 0
        # #print(topic_words[i])
        # for tkn in tokens:
            # if tkn in topic_words_broadcasted.value[i]:
                # count += 1
        # twt_list.append(count)

    # return twt_list

# # def make_topic_word(topic_words):
     # # return F.udf(lambda c: label_maker_topic(c, topic_words))


# def make_topic_word_better(topic_words_broadcasted):
    # def f(c):
        # return label_maker_topic(c, topic_words_broadcasted)
    # return F.udf(f)

# def test_udf_list(spark):
# df = spark.createDataFrame([["union",], ["party",]]).toDF("tokens")
# b = spark.sparkContext.broadcast(keyword_list)
# df.withColumn("topics", make_topic_word_better(b)(F.col("tokens"))).show()



## CODE FOR THIS SO QUESTION: https://stackoverflow.com/questions/53052891/pass-a-dictionary-to-pyspark-udf

# def stringToStr_function(checkCol, dict1_broadcasted) :
  # for key, value in dict1.iteritems() :
    # if(checkCol != None and checkCol==key): return value

# stringToStr_udf = udf(stringToStr_function, StringType())

# def stringToStr(dict1_broadcasted):
    # def f(x):
        # return dict1_broadcasted.value.get(x)
    # return F.udf(f)

# def test_fetch_from_dict(spark):
    # df = spark.createDataFrame([["REQUEST",], ["CONFIRM",]]).toDF("status")
    # df.show()
    # b = spark.sparkContext.broadcast({"REQUEST": "Requested", "CONFIRM": "Confirmed", "CANCEL": "Cancelled"})
    # df.withColumn(
        # "new_col",
         # stringToStr(b)(F.col("status"))
    # ).show()


def add_descriptions(dict_b):
    def f(x):
        return dict_b.value.get(x)
    return F.udf(f)


def test_add_descriptions(spark):
    df = spark.createDataFrame([[1,], [2,], [3,]]).toDF("some_num")
    dictionary= { 1:'A' , 2:'B' }
    dict_b = spark.sparkContext.broadcast(dictionary)
    df.withColumn(
        "res",
        add_descriptions(dict_b)(F.col("some_num"))
    ).show()


