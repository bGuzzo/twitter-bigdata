import json
import sys

import requests
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SQLContext
from pyspark.streaming import StreamingContext


def aggregate(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def get_sql_context_instance(spark_context):
    if 'sqlContextSingletonInstance' not in globals():
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


class TweetProcessor:
    __log_level = "ERROR"
    __app_name = "twitter_bigdata"
    __checkpoint_name = "checkpoint_twitter_bigdata"

    def __init__(self, server_port, fe_port):
        self.__connection_port = server_port
        self.__fe_port = fe_port

    def __send_df_to_dashboard(self, df, target):
        labels = [str(t.label) for t in df.select("label").collect()]
        values = [p.value for p in df.select("value").collect()]
        url = "http://localhost:" + str(self.__fe_port) + "/" + target
        print(url)
        request_data = {'label': str(labels), 'data': str(values)}
        requests.post(url, data=request_data)

    def __process_rdd(self, rdd, target):
        try:
            sql_context = get_sql_context_instance(rdd.context)
            row_rdd = rdd.map(lambda w: Row(label=w[0], value=w[1]))
            label_df = sql_context.createDataFrame(row_rdd)
            label_df.registerTempTable("lables")
            label_counts_df = sql_context.sql(
                "select label, value from lables order by value desc limit 10")
            label_counts_df.show()
            self.__send_df_to_dashboard(label_counts_df, target)
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

    def start_processor(self):
        spark_conf = SparkConf()
        spark_conf.setAppName(self.__app_name)
        spark_context = SparkContext(conf=spark_conf)
        spark_context.setLogLevel(self.__log_level)
        spark_streaming_ctx = StreamingContext(spark_context, 1)
        spark_streaming_ctx.checkpoint(self.__checkpoint_name)
        tweet_stream = spark_streaming_ctx.socketTextStream("localhost", self.__connection_port)

        json_tweets = tweet_stream.map(lambda line: json.loads(line))
        tweet_text = json_tweets.map(lambda j: str(j['text']))
        words = tweet_text.flatMap(lambda line: line.split(" ")).map(lambda s: str(s).replace("\n", ""))

        words_map = words.filter(lambda word: word).filter(lambda word: not str(word).startswith('#')).filter(
            lambda word: not str(word).startswith('@')).map(lambda x: (x, 1))
        hashtags = words.filter(lambda w: str(w).startswith('#')).filter(lambda w: len(w) > 1).map(lambda x: (x, 1))
        mentions = words.filter(lambda w: str(w).startswith('@')).filter(lambda w: len(w) > 1).map(lambda x: (x, 1))
        tweet_lang = json_tweets.map(lambda j: str(j['lang'])).map(lambda x: (x, 1))

        # Return a new "state" DStream where the state for each key is updated by applying the given function on the
        # previous state of the key and the new values for the key. This can be used to maintain arbitrary state data
        # for each key.
        total_words = words_map.updateStateByKey(aggregate)
        total_hashtags = hashtags.updateStateByKey(aggregate)
        total_mentions = mentions.updateStateByKey(aggregate)
        total_lang = tweet_lang.updateStateByKey(aggregate)

        total_words.foreachRDD(lambda rdd: self.__process_rdd(rdd, 'update-word'))
        total_hashtags.foreachRDD(lambda rdd: self.__process_rdd(rdd, 'update-hashtag'))
        total_mentions.foreachRDD(lambda rdd: self.__process_rdd(rdd, 'update-mention'))
        total_lang.foreachRDD(lambda rdd: self.__process_rdd(rdd, 'update-lang'))

        spark_streaming_ctx.start()
        spark_streaming_ctx.awaitTermination()
