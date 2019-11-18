#coding=utf-8

import sys
import json
import redis
from rediscluster import StrictRedisCluster
from rediscluster.connection import ClusterConnectionPool, SSLClusterConnection

REDIS_SERVER0 = [
    {'host': '10.167.146.46', 'port': 7001},
    {'host': '10.167.146.50', 'port': 7002},
    {'host': '10.167.146.51', 'port': 7003},
    {'host': '10.167.146.52', 'port': 7004},
    {'host': '10.167.146.53', 'port': 7005},
    {'host': '10.167.146.54', 'port': 7006},
    {'host': '10.167.146.46', 'port': 8001},
    {'host': '10.167.146.50', 'port': 8002},
    {'host': '10.167.146.51', 'port': 8003},
    {'host': '10.167.146.52', 'port': 8004},
    {'host': '10.167.146.53', 'port': 8005},
    {'host': '10.167.146.54', 'port': 8006}
]

REDIS_SERVER1 = [
    {'host': '10.167.146.12', 'port': 8002},
    {'host': '10.167.146.12', 'port': 7002},
    {'host': '10.167.146.13', 'port': 7003},
    {'host': '10.167.146.14', 'port': 7004},
    {'host': '10.167.146.15', 'port': 7005},
    {'host': '10.167.146.16', 'port': 7006},
    {'host': '10.167.146.11', 'port': 7001},
    {'host': '10.167.146.11', 'port': 8001},
    {'host': '10.167.146.13', 'port': 8003},
    {'host': '10.167.146.14', 'port': 8004},
    {'host': '10.167.146.15', 'port': 8005},
    {'host': '10.167.146.16', 'port': 8006}
]

REDIS_SERVER2 = [
    {'host': '10.167.146.27', 'port': 7000},
    {'host': '10.167.146.34', 'port': 7000},
    {'host': '10.167.146.38', 'port': 7000},
    {'host': '10.167.146.37', 'port': 7000},
    {'host': '10.167.146.55', 'port': 7000},
    {'host': '10.167.146.27', 'port': 8000},
    {'host': '10.167.146.34', 'port': 8000},
    {'host': '10.167.146.38', 'port': 8000},
    {'host': '10.167.146.37', 'port': 8000},
    {'host': '10.167.146.55', 'port': 8000}
]

class RedisManager:
    def __init__(self, redis_server, pw=None):
        #self.redis_client_ = redis.Redis(host='127.0.0.1', port=9966)
        if pw:
            pool = ClusterConnectionPool(startup_nodes=redis_server, password=pw, skip_full_coverage_check=True, decode_responses=True)
            self.redis_client_ = StrictRedisCluster(connection_pool=pool)
        else:
            pool = ClusterConnectionPool(startup_nodes=redis_server, skip_full_coverage_check=True, decode_responses=True)
            self.redis_client_ = StrictRedisCluster(connection_pool=pool)

    def Set(self, k, v, is_str, expire=None):
        if not is_str:
            v = json.dumps(v)
        return self.redis_client_.set(k, v, ex=expire)

    def Get(self, k):
        return self.redis_client_.get(k)


    def Delete(self, k):
        return self.redis_client_.delete(k)

    def HashMultiSet(self, k, d, expire=None):
        self.redis_client_.hmset(k, d)
        if expire:
            self.redis_client_.expire(k, expire)

    def HashGetAll(self, k):
        return self.redis_client_.hgetall(k)

    def Pushback(self, k, v, expire=None):
        self.redis_client_.rpush(k, v)
        if expire:
            self.redis_client_.expire(k, expire)

    def SetList(self, k, l, expire=None):
        self.redis_client_.rpush(k, *l)
        if expire:
            self.redis_client_.expire(k, expire)

    def SetSet(self, k, v, expire=None):
        self.redis_client_.sadd(k, v)
        if expire:
            self.redis_client_.expire(k, expire)

    def SortSetSet(self, k, v, expire=None):
        self.redis_client_.zadd(k, v[0], v[1])
        if expire:
            self.redis_client_.expire(k, expire)

    def Handle(self):
        return self.redis_client_


if "__main__" == __name__:
    test = RedisManager(REDIS_SERVER0)
    l = ['w', 'o', 'r', 'l', 'd', '.']
    key_str = "hello"
    test.SetList(key_str, l, 30)
    print(test.Handle().lindex(key_str, 0))
    print(test.Handle().lindex(key_str, 1))
    print(test.Handle().lindex(key_str, 3))
    print(test.Handle().lindex(key_str, 5))
    #print(test.Get("hello"))

