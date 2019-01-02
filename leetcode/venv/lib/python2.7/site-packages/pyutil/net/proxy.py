import time, random

from pyutil.program.conf import Conf

proxies = Conf('/etc/ss_conf/proxy.conf').get_values('squid_proxies')
hk_proxies = Conf('/etc/ss_conf/proxy.conf').get_values('hk_squid_proxies')

def get_proxy(use_gfw=False):
    proxy = hk_proxies if use_gfw else proxies
    if not proxy:
        return None
    random.seed(time.time())
    return proxy[random.randint(0, len(proxy) - 1)]

def make_get_proxy(use_gfw):
    return lambda: get_proxy(use_gfw)

def get_proxy_list(use_gfw=False):
    return hk_proxies if use_gfw else proxies
