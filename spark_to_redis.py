# gather friend information and write to redis

import sys
import json, datetime, time
from time import strftime
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("YanJ_app").setMaster("spark://ip-172-31-1-4:7077")
sc = SparkContext(conf = conf)
import redis

# Map reduce
def map_func(line):
    each_line = json.loads(line)
    A, B, C = False, False, False

    if 'user' in each_line:
        if 'id' in each_line['user']:
            usr_id = each_line['user']['id']
            A = True
    if 'timestamp_ms' in each_line:
        raw_time = float(each_line['timestamp_ms'][:10])
        t_time = datetime.datetime.utcfromtimestamp(raw_time)
        t_time = t_time.strftime('%Y-%m-%d')
        B = True

    if 'retweeted_status' in each_line:
        if 'user' in each_line['retweeted_status']:
            if 'id' in each_line['retweeted_status']['user']:
                retweet_id = each_line['retweeted_status']['user']['id']
                C = True

    if A and B and C:
        return [(retweet_id, [usr_id])]
    else:
        return []

# Input files
textFile = sc.textFile("s3n://timo-twitter-data/2015/05/01/01/31.json")
#result1 = textFile.flatMap(map_func).reduceByKey(lambda a, b: a if a<= b else b)
result = textFile.flatMap(map_func).reduceByKey(lambda a, b: a+b)

def save_to_redis(record):
    redis_host = 'ec2-52-32-60-176.us-west-2.compute.amazonaws.com'
    redis_db = redis.StrictRedis(host=redis_host, port=6379, db=0)
    #print(record)
    redis_db.hset("test_one_day_mul_user2", record[0], record[1])

result.foreach(save_to_redis)
