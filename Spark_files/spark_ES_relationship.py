# # #
# This file is to gather user and follower information and write to Elasticsearch 
# # #

# import library 
import sys
import redis
import json, datetime, time
from time import strftime
from pyspark import SparkConf, SparkContext
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession

# elasticsearch setting
ES_INDEX = "twitter_friend_data_2_8_4month_uniontest"
def create_es_index():
    es_mapping = {"yan_type": { "properties":{"usr_id": {"type":"text"}, 'ttext':{"type":"text"}, 'ttimes': {"type":"date"}} } }
    es_settings = {'number_of_shards':3, 'number_of_replicas': 1, 'refresh_interval': '1s', 'index.translog.flush_threshold_size': '1gb'}
    ES_indice = es.indices.create(index = ES_INDEX, body = {'settings': es_settings, 'mappings': es_mapping})

if not es.indices.exists(ES_INDEX):
    create_es_index()



# Map reduce to find all user and follower relationship and assiciate earlitest retweet time
def map_func(line):
    each_line = json.loads(line)
    user_exisit, time_exisit, retweet_exist = False, False, False

    if 'user' in each_line:
        if 'id' in each_line['user']:
            usr_id = each_line['user']['id']
            user_exisit = True
    if 'timestamp_ms' in each_line:
        raw_time = float(each_line['timestamp_ms'][:10])
        t_time = datetime.datetime.utcfromtimestamp(raw_time)
        t_time = t_time.strftime('%Y-%m-%d')
        time_exisit = True

    if 'retweeted_status' in each_line:
        if 'user' in each_line['retweeted_status']:
            if 'id' in each_line['retweeted_status']['user']:
                retweet_id = each_line['retweeted_status']['user']['id']
                retweet_exsit = True

    # if all three fields have data, export this info item
    if user_exisit and time_exisit and retweet_exisit:
        return [((retweet_id, usr_id), t_time)]
    else:
        return []



# Send function is used to collect data and send/save to elasticsearch
def send_func(item):

    try:
        # gather info
        usr_id = item[0][0]
        follower_id = item[0][1]
        t_time = item[1]
       
        # send and write to elasticsearch
        doc = {"usr_id": usr_id, 'follower_id': follower_id, 'early_time': t_time}
        es = Elasticsearch(['ip-of-my-cluster'], http_auth=('elastic', 'changeme'), verify_certs=False)
        es.index(index= ES_INDEX, doc_type= "input_type", body=doc)

    except:
        # error exception
        print "Error in send_func"




# Input files
textfiles1 = sc.textFile("s3n://timo-twitter-data/2015/*/*/*/*.json")
result = textfiles.flatMap(map_func).reduceByKey(lambda a, b: a if a<= b else b)



# write to ES
result.foreach(send_func)

