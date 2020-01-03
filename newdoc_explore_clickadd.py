from RedisManager import RedisManager, REDIS_SERVER0, REDIS_SERVER1
import sys

rkey = "pick_v2_click_queue"
redis1_ = RedisManager(REDIS_SERVER1, 'Migu@2020')

program_id = sys.argv[1]
cnt = int(sys.argv[2])

redis1_.Handle().lpush(rkey,"%s#%s"%(program_id,cnt))

