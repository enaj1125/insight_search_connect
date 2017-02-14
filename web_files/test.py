import redis

redis_server = 'ec2-52-36-25-239.us-west-2.compute.amazonaws.com'
r = redis.StrictRedis(host=redis_server, port=6379, db=0)


#r.set('foo', 'bar')

res = r.get('foo2')
print(res)

