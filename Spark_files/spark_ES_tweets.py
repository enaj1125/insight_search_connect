# #
# This program clean raw twitter data and write outupt data into ES
# #

# import the module
import sys
import json
import datetime, time
from time import strftime
from pyspark import SparkConf, SparkContext
from elasticsearch import Elasticsearch


# Elasticsearch setting
ES_INDEX = 'twitter_data'
ES_TYPE = 'inputs'
ES_RESOURCE = '/'.join([ES_INDEX,ES_TYPE])
es_conf = {'es.nodes': ES_NODES, 'es.resource': ES_RESOURCE, 'es.port' : '9200','es.net.http.auth.user':'elastic','es.net.http.auth.pass':'changeme'}
es = Elasticsearch(['ip-of-my-cluster'], http_auth=('elastic', 'changeme'), verify_certs=False)



# Map function is used to clean/filter data, and write to ES
def map_func(line):
    
    # connect to elasticsearch
    es = Elasticsearch(['ip-of-my-cluster'], http_auth=('elastic', 'changeme'), verify_certs=False)
    
    # load json data line by line
    each_line = json.loads(line)
    
    # Set three bolean variable to check data quality: completeness 
    user_exisit, time_exisit, text_exist = False, False, False
    retweet_id = []

    # check if user id info is complete
    if 'user' in each_line:
        if 'id' in each_line['user']:
            usr_id = each_line['user']['id']
            user_exisit = True

    # check if timestamp info is complete        
    if 'timestamp_ms' in each_line:
        # original data formatted in UTC, need to be trancated and reformatted to %Y-%m-%d 
        raw_time = float(each_line['timestamp_ms'][:10])
        t_time = datetime.datetime.utcfromtimestamp(raw_time)
        t_time = t_time.strftime('%Y-%m-%d')
        time_exisit = True

    # check if text info is complete    
    if 'text' in each_line:
        t_text = each_line['text']
        text_exisit = True
        
    # if data quality check pass, write data into Elasticsearch
    if user_exisit and time_exisit and text_exisit:
        doc = {'usr_id': usr_id, 'ttext': t_text, 'ttimes': t_time}
        es.index(index= ES_INDEX, doc_type='inputs', body=doc)
        
        return [('key', doc)]
    else:
        return []


# Read data from S3 
textfiles = sc.textFile("s3n://timo-twitter-data/2015/07/*/*/*.json")

# Clean data use flatmap function
result = textfiles.flatMap(map_func)

# send data using API method
result.saveAsNewAPIHadoopFile(path='-', \
                                            outputFormatClass='org.elasticsearch.hadoop.mr.EsOutputFormat', \
                                            keyClass='org.apache.hadoop.io.NullWritable', \
                                            valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable', \
                                            conf=es_conf)
