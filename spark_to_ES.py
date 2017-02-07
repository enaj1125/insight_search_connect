import sys
import json
import datetime, time
from time import strftime
from pyspark import SparkConf, SparkContext
from elasticsearch import Elasticsearch
conf = SparkConf().setAppName("YanJ_app").setMaster("spark://ip-172-31-1-4:7077")
sc = SparkContext(conf = conf)


# elasticsearch setting
es = Elasticsearch(['ip-172-31-1-11'], http_auth=('elastic', 'changeme'), verify_certs=False)
ES_INDEX = "twitter_data_oneday"
def create_es_index():
    es_mapping = {"yan_type": { "properties":{"usr_id": {"type":"text"}, 'ttext':{"type":"text"}, 'ttimes': {"type":"date"}} } }
    es_settings = {'number_of_shards':3, 'number_of_replicas': 2, 'refresh_interval': '1s', 'index.translog.flush_threshold_size': '1gb'}
    ES_indice = es.indices.create(index = ES_INDEX, body = {'settings': es_settings, 'mappings': es_mapping})


if not es.indices.exists(ES_INDEX):
    create_es_index()

# Map reduce
def map_func(line):
    each_line = json.loads(line)
    A, B, C = False, False, False
    retweet_id = []

    if 'user' in each_line:
        if 'id' in each_line['user']:
            usr_id = each_line['user']['id']
            A = True
    if 'timestamp_ms' in each_line:
        raw_time = float(each_line['timestamp_ms'][:10])
        t_time = datetime.datetime.utcfromtimestamp(raw_time)
        t_time = t_time.strftime('%Y-%m-%d')
        B = True
    if 'text' in each_line:
        t_text = each_line['text']
        C = True

    if 'retweeted_status' in each_line:
        if 'user' in each_line['retweeted_status']:
            if 'id' in each_line['retweeted_status']['user']:
                retweet_id = each_line['retweeted_status']['user']['id']

    if A and B and C:
        #print 'user id', usr_id, 'retweet_id', [retweet_id]
        doc = {'usr_id': usr_id, 'ttext': t_text, 'ttimes': t_time, 'retweets_id': retweet_id}
        #es.index(index= ES_INDEX, doc_type='inputs', body=doc)
        return [((usr_id,t_time), [t_text])]
    else:
        return []


def send_func(item):
    usr_id = item[0][0]
    t_time = item[0][1]
    t_text = item[1][0]
    doc = {'usr_id': usr_id, 'ttext': t_text, 'ttimes': t_time}
    es = Elasticsearch(['ip-172-31-1-11'], http_auth=('elastic', 'changeme'), verify_certs=False)
    es.index(index= ES_INDEX, doc_type='inputs', body=doc)


# Input files
textFile = sc.textFile("s3n://timo-twitter-data/2015/05/01/*/*.json")
result1 = textFile.flatMap(map_func).reduceByKey(lambda a, b: a+b)
result2 = result1.foreach(send_func)
