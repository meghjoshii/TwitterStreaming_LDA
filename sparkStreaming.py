#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Columbia EECS E6893 Big Data Analytics
"""
This module is the spark streaming analysis process.


Usage:
    If used with dataproc:
        gcloud dataproc jobs submit pyspark --cluster <Cluster Name> twitterHTTPClient.py

    Create a dataset in BigQurey first using
        bq mk bigdata_sparkStreaming

    Remeber to replace the bucket with your own bucket name


Todo:
    1. hashtagCount: calculate accumulated hashtags count
    2. wordCount: calculate word count every 60 seconds
        the word you should track is listed below.
    3. save the result to google BigQuery

"""

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import time
import subprocess
import re
from google.cloud import bigquery

# global variables
bucket = "mmj2169hw2"    # TODO : replace with your own bucket name
output_directory_hashtags = 'gs://{}/hadoop/tmp/bigquery/pyspark_output/hashtagsCount'.format(bucket)
output_directory_wordcount = 'gs://{}/hadoop/tmp/bigquery/pyspark_output/wordcount'.format(bucket)
output_directory_lda = 'gs://{}/hadoop/tmp/bigquery/pyspark_output/lda_file/lda.txt'.format(bucket)

# output table and columns name
output_dataset = 'datasett'                     #the name of your dataset in BigQuery
output_table_hashtags = 'hashtags'
columns_name_hashtags = ['hashtags', 'count']
output_table_wordcount = 'wordcount'
columns_name_wordcount = ['word', 'count', 'time']

# parameter
IP = 'localhost'    # ip port
PORT = 9002      # port

STREAMTIME = 20         # time that the streaming process runs
windowLength = 10      # window length for wordcount
slideInterval = 10        # slide interval for wordcount
WORD = ['data', 'spark', 'ai', 'movie', 'good']     #the words you should filter and do word count

# Helper functions
def saveToStorage(rdd, output_directory, columns_name, mode="overwrite"):
    """
    Save each RDD in this DStream to google storage
    Args:
        rdd: input rdd
        output_directory: output directory in google storage
        columns_name: columns name of dataframe
        mode: mode = "overwirte", overwirte the file
              mode = "append", append data to the end of file
    """
    print(rdd.isEmpty(),"aaaa")
    if not rdd.isEmpty():
        (rdd.toDF( columns_name ) \
        .write.save(output_directory_hashtags, format="json", mode=mode))
        
def saveToStorage_hashTags(rdd):
    if not rdd.isEmpty():
        (rdd.toDF( columns_name_hashtags ) \
        .write.save(output_directory_hashtags, format="json", mode="overwrite"))

def saveToStorage_wordCount(rdd):
    if not rdd.isEmpty():
        (rdd.toDF( columns_name_wordcount ) \
        .write.save(output_directory_wordcount, format="json", mode="append"))
        
def saveToBigQuery(sc, output_dataset, output_table, directory):
    """
    Put temp streaming json files in google storage to google BigQuery
    and clean the output files in google storage
    """
    files = directory + '/part-*'
    subprocess.check_call(
        'bq load --source_format NEWLINE_DELIMITED_JSON '
        '--replace '
        '--autodetect '
        '{dataset}.{table} {files}'.format(
            dataset=output_dataset, table=output_table, files=files
        ).split())
    output_path = sc._jvm.org.apache.hadoop.fs.Path(directory)
    output_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(
        output_path, True)
    
# helper function
def filterFunc(hashtags):
    if re.match("^#[0-9a-z]+$", hashtags):
        return True
    else:
        return False

# helper function
def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)  # add the new values with the previous running count to get the new count

def hashtagCount(words):
    """
    Calculate the accumulated hashtags count sum from the beginning of the stream
    and sort it by descending order of the count.
    Ignore case sensitivity when counting the hashtags:
        "#Ab" and "#ab" is considered to be a same hashtag
    You have to:
    1. Filter out the word that is hashtags.
       Hashtag usually start with "#" and followed by a serious of alphanumeric
    2. map (hashtag) to (hashtag, 1)
    3. sum the count of current DStream state and previous state
    4. transform unordered DStream to a ordered Dstream
    Hints:
        you may use regular expression to filter the words
        You can take a look at updateStateByKey and transform transformations
    Args:
        dstream(DStream): stream of real time tweets
    Returns:
        DStream Object with inner structure (hashtag, count)
    """
    
    # TODO: insert your code here
    hashtags = words \
    .map(lambda x: x.lower()) \
    .filter(filterFunc) \
    .map(lambda x: (x, 1)) \
    .updateStateByKey(updateFunction) \
    .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
    
    return hashtags

def wordCount(words):
    """
    Calculte the count of 5 sepcial words for every 60 seconds (window no overlap)
    You can choose your own words.
    Your should:
    1. filter the words
    2. count the word during a special window size
    3. add a time related mark to the output of each window, ex: a datetime type
    Hints:
        You can take a look at reduceByKeyAndWindow transformation
        Dstream is a serious of rdd, each RDD in a DStream contains data from a certain interval
        You may want to take a look of transform transformation of DStream when trying to add a time
    Args:
        dstream(DStream): stream of real time tweets
    Returns:
        DStream Object with inner structure (word, (count, time))
    """
    
    # TODO: insert your code here
    res = words \
    .map(lambda x: x.lower()) \
    .filter(lambda x: x in WORD) \
    .map(lambda x: (x, 1)) \
    .reduceByKeyAndWindow(lambda x, y: x+y, lambda x, y: x-y, windowLength, slideInterval) \
    .transform(lambda timestamp, rdd: rdd.map(lambda x: (x[0], x[1], timestamp)))
    
    return res


if __name__ == '__main__':
    # Spark settings
    conf = SparkConf()
    conf.setMaster('local[2]')
    conf.setAppName("TwitterStreamApp")

    # create spark context with the above configuration
    #sc = SparkContext(conf=conf)
    sc=SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")

    # create sql context, used for saving rdd
    sql_context = SQLContext(sc)

    # create the Streaming Context from the above spark context with batch interval size 5 seconds
    ssc = StreamingContext(sc, 5)
    # setting a checkpoint to allow RDD recovery
    ssc.checkpoint("~/checkpoint_TwitterApp")

    # read data from port 9001
    dataStream = ssc.socketTextStream(IP, PORT)
    dataStream.pprint()

    dataStream.foreachRDD(lambda rdd: rdd.saveAsTextFile(output_directory_lda))

    words = dataStream.flatMap(lambda line: line.split(" "))

    # # calculate the accumulated hashtags count sum from the beginning of the stream
    topTags = hashtagCount(words)
    topTags.pprint()

    # # Calculte the word count during each time period 6s
    word_Count = wordCount(words)
    word_Count.pprint()

    # save hashtags count and word count to google storage
    # used to save to google BigQuery
    # You should:
    #   1. topTags: only save the lastest rdd in DStream
    #   2. wordCount: save each rdd in DStream
    # Hints:
    #   1. You can take a look at foreachRDD transformation
    #   2. You may want to use helper function saveToStorage
    #   3. You should use save output to output_directory_hashtags, output_directory_wordcount,
    #       and have output columns name columns_name_hashtags and columns_name_wordcount.
    # TODO: insert your code here
    topTags.foreachRDD(saveToStorage_hashTags)
    word_Count.foreachRDD(saveToStorage_wordCount)
    
    # start streaming process, wait for 600s and then stop.
    print("here")
    ssc.start()
    print('STARTED')
    time.sleep(STREAMTIME)
    print('WOKE UP')
    ssc.stop(stopSparkContext=False, stopGraceFully=True)
    
#     print("Streaming context is stopped.")
#     # put the temp result in google storage to google BigQuery
#     saveToBigQuery(sc, output_dataset, output_table_hashtags, output_directory_hashtags)
#     saveToBigQuery(sc, output_dataset, output_table_wordcount, output_directory_wordcount)

    print("Streaming context is stopped.")
    saveToBigQuery(sc, output_dataset, output_table_hashtags, output_directory_hashtags)
    saveToBigQuery(sc, output_dataset, output_table_wordcount, output_directory_wordcount)


