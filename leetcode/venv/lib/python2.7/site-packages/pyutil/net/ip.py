#coding=utf8
import re
IP_REGEX = re.compile(r'^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$')

def ip2int(ip):
    import struct
    import socket
    return struct.unpack("!I", socket.inet_aton(ip))[0]


def int2ip(i):
    import socket
    import struct
    return socket.inet_ntoa(struct.pack("!I", i))


def is_ipv4(ip):
    """
    >>> is_ipv4('127.0.0.1')
    True
    >>> is_ipv4('127.0.0.1.1')
    False
    >>> is_ipv4('foo.com')
    False
    >>> is_ipv4(None)
    False
    """
    if ip is None: return False
    return bool(IP_REGEX.search(ip))
