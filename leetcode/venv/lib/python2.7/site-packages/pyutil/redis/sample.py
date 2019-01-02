#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, logging, time

from pyutil.program.conf import Conf
from pyutil.redis.redis_proxy import make_redis_proxy_cli, make_redis_proxy_cli2
import redis

try:
    cli = make_redis_proxy_cli2(["10.4.17.164:6379",], connection_kwargs={"socket_timeout": 0.2,}, cluster='ttttest')
    cli.setex('a', 1, 'value')
    print cli.get('a')
except Exception as e:
    print e

conf = Conf('/etc/ss_conf/redis.conf')
cluster = 'redis_recommend_new' if len(sys.argv) < 2 else sys.argv[1]
servers = conf.get_values(cluster)
if not servers or not servers[0]:
    print 'servers for cluster not found'
    sys.exit(-1)


# 本示例演示如何通过proxy访问redis （随机的proxy连接池）
# 1. 通过pyutil.redis_proxy.make_redis_proxy_cli来构造得到一个client对象
# 2. 有两种函数调用格式：Redis和StrictRedis，涉及到的函数有setex/zadd等，需要明确一下现有代码是哪种
# 3. 使用pipeline时，必须要设置transaction为False才可以发送给proxy
# 4. 部分命令proxy不支持，主要是同一个命令里涉及多个key的，比如rename/sdiff等等

ts = time.time()

try:
    # By default, StrictRedis is created
    cli = make_redis_proxy_cli(servers, socket_timeout=0.3)
    print cli.set('a', 'a')
    print cli.setex('a', 100, 'a') # StrictRedis style
    print cli.get('a')
    print cli.delete('a')

    pipeline = cli.pipeline(transaction=False)
    for i in range(100):
        pipeline.setex('sample_test_' + str(i), 100, str(i))
    print pipeline.execute()
except (redis.ConnectionError, redis.TimeoutError), ex:
    logging.exception('redis connect error or timeout')
except redis.RedisError, ex:
    # any kind of redis error, including ConnectionError and TimeoutError
    logging.exception('redis error')
except Exception, ex:
    print ex

# 从redis_shard迁移过来的，需要使用普通的redis(non StrictRedis)
try:
    # use non StrictRedis
    cli = make_redis_proxy_cli(servers, strict_redis=False, socket_timeout=0.3)
    print cli.set('a', 'b')
    print cli.setex('a', 'b', 100) # Redis sytle
    print cli.get('a')
    print cli.delete('a')
except redis.RedisError, ex:
    # any kind of redis error, including ConnectionError and TimeoutError
    logging.exception('redis error')
except Exception, ex:
    print ex

print 'total time', time.time() - ts
