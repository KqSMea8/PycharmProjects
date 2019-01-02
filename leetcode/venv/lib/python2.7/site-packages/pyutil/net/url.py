#coding=utf8
import re, os
from pyutil.program.python import unicode_to_str

tlds = set()
def load_tlds():
    if len(tlds):
        return tlds
    tld_path = os.path.dirname(__file__) + "/public_suffix_list.dat"
    with open(tld_path) as f:
        for line in f:
            if line[0] not in '/\n':
                tlds.add(line.strip())
    return tlds

ip_url_regex = re.compile(r'^(http://)?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\b(.*)')

def get_main_domain(url):
    '''
    >>> get_main_domain(None)
    >>> get_main_domain('110.75.187.209m')
    ''
    >>> get_main_domain('com')
    ''
    >>> get_main_domain('110.75.187.209')
    '110.75.187.209'
    >>> get_main_domain('http://www.baidu.com')
    'baidu.com'
    >>> get_main_domain(r'http://www.99fang.com.cn?haha')
    '99fang.com.cn'
    >>> get_main_domain(r'http://www.99fang.com?haha')
    '99fang.com'
    >>> get_main_domain(r'http://www.99fang.com:8080?haha')
    '99fang.com'
    >>> get_main_domain(r'http://www.com:8080?url=http://www.hoho.com/')
    'www.com'
    >>> get_main_domain('http://WWW.JJHOUSE.CN/sub_hack.asp?id=38735')
    'jjhouse.cn'
    >>> get_main_domain('http://w.51auto.com:8999/2car/v2.0/m_header_info/info.jsp?info_id=1340208')
    '51auto.com'
    '''
    if not url:
        return url
    url = url.lower()
    m = ip_url_regex.match(url)
    if m:
        return m.group(2)
    domain = get_domain(url)
    parts = domain.split('.')
    tlds = load_tlds()
    for i in range(-len(parts), 0):
        lastIElements = parts[i:]
        candidate = ".".join(lastIElements) # abcde.co.uk, co.uk, uk
        wildcardCandidate = ".".join(["*"]+lastIElements[1:]) # *.co.uk, *.uk, *
        exceptionCandidate = "!"+candidate

        if (exceptionCandidate in tlds):
            return ".".join(parts[i:])
        if (candidate in tlds or wildcardCandidate in tlds):
            return ".".join(parts[i-1:])
    return ''

def get_subdomain(url):
    '''
    >>> get_subdomain('http://w.51auto.com:8999/2car/v2.0/m_header_info/info.jsp?info_id=1340208')
    'w.51auto.com'
    >>> get_subdomain('dailynews.gov.bw')
    'dailynews.gov.bw'
    >>> get_subdomain('www.baidu.com')
    'baidu.com'
    '''
    url = url.lower()
    main_domain = get_main_domain(url)
    end = url.find(main_domain)
    start = url.find('://')
    if start == -1:
        start = 0
    else:
        start = start + 3
    pos = url.rfind('.', start, end)
    if pos == -1:
        return main_domain
    pos = url.rfind('.', start, pos - 1)
    if pos == -1:
        if url[start: start + 3] == 'www' or url[start: start + 3] == 'WWW':
            return main_domain
    else:
        start = pos + 1
    return url[start : end + len(main_domain)]

def get_domain_level(domain):
    '''
    >>> get_domain_level('.baidu.com')
    0
    >>> get_domain_level('baidu.com')
    1
    >>> get_domain_level('www.baidu.com')
    2
    >>> get_domain_level('a.www.baidu.com')
    3
    >>> get_domain_level('www.nx.cn')
    1
    >>> get_domain_level('www.gov.cn')
    1
    '''
    if not is_valid_domain(domain):
        return 0
    main_domain = get_main_domain(domain)
    level = len([x for x in domain[:-len(main_domain)] if x == '.'])
    return level + 1

def get_domain(url):
    '''
    >>> get_domain('http://pRoduct.auto.163.com/series/1965.html')
    'product.auto.163.com'
    >>> get_domain('http://pRoduct.auto.163.com./series/1965.html')
    'product.auto.163.com'
    >>> get_domain('finance.sina.com.c')
    'finance.sina.com.c'
    >>> get_domain('.sina.com')
    ''
    >>> get_domain('javascript:void(0)')
    ''
    >>> get_domain(None)
    ''
    '''
    from urlparse import urlparse
    domain = re.sub(r'\.$', '', urlparse(url or '').hostname or '')
    if not domain and is_valid_domain(url):
        domain = url
    return domain

def get_parent_domains(url):
    '''
    >>> get_parent_domains('http://www.baidu.com/1.txt')
    ['www.baidu.com', 'baidu.com']
    >>> get_parent_domains('baidu.com')
    ['baidu.com']
    >>> get_parent_domains('www.gov.cn')
    ['www.gov.cn', 'gov.cn']
    >>> get_parent_domains('com')
    []
    '''
    domain = get_domain(url)
    pdomains = []
    while '.' in domain:
        pdomains.append(domain)
        domain = domain.split('.', 1)[1]

    return pdomains

def is_valid_domain(domain, ip_is_domain=False):
    '''
    >>> is_valid_domain('www.baidu.c')
    True
    >>> is_valid_domain('.www.baidu.c')
    False
    >>> is_valid_domain('www.baidu.coM')
    False
    >>> is_valid_domain('www.baidu.com$')
    False
    >>> is_valid_domain('127.0.0.1')
    False
    >>> is_valid_domain('127.0.0.1', True)
    True
    '''
    import re
    return domain and (
            re.search(r'^([a-z0-9_-]+\.)+[a-z0-9_-]*[a-z]$', domain) is not None or
            (ip_is_domain and re.search(r'^\d{1,3}(\.\d{1,3}){3}$', domain) is not None)
            )

def is_valid_url(url):
    u'''
    >>> is_valid_url('http://foo.com')
    True
    >>> is_valid_url('http://127.0.0.1')
    True
    >>> is_valid_url(u'https://foo.com/1.html?v=1#中')
    True
    >>> is_valid_url('http://www.ithome.comhttp://www.ithome.com/html/')
    True
    >>> is_valid_url('Http://www.bearpaw.com/glimmer/1686W-220/detail?utm_source=1410email4bp(glimmer_boots_&utm_medium=email&utm_content=22493689&utm_campaign=1410email4bp%20-%20For%20all%20your%20moods,%20outfits,%20and%20every%20o')
    True
    >>> is_valid_url('http%3A%2F%2Fsd.sina.com.cn%2Fnews%2Fsdyw%2F2014-10-24%2F160778049.html')
    False
    >>> is_valid_url(None)
    False
    >>> is_valid_url('')
    False
    >>> is_valid_url('ttp://foo.com')
    False
    >>> is_valid_url('http://42814.sohu023.com@')
    False
    '''
    from urlparse import urlparse
    if url is None: return False
    r = urlparse(url)
    return bool(r.scheme and r.netloc and r.hostname) and r.scheme in ['http', 'https']


def normalize_url(url):
    u"""
    The list of safe characters here is constructed from the "reserved" and
    "unreserved" characters specified in sections 2.2 and 2.3 of RFC 3986:
        reserved    = gen-delims / sub-delims
        gen-delims  = ":" / "/" / "?" / "#" / "[" / "]" / "@"
        sub-delims  = "!" / "$" / "&" / "'" / "(" / ")"
                      / "*" / "+" / "," / ";" / "="
        unreserved  = ALPHA / DIGIT / "-" / "." / "_" / "~"
    Of the unreserved characters, urllib.quote already considers all but
    the ~ safe.
    The % character is also added to the list of safe characters here, as the
    end of section 3.1 of RFC 3987 specifically mentions that % must not be
    converted.

    >>> normalize_url(u'http://tieba.baidu.com/f?ie=utf-8&kw=华为荣耀')
    'http://tieba.baidu.com/f?ie=utf-8&kw=%E5%8D%8E%E4%B8%BA%E8%8D%A3%E8%80%80'
    >>> normalize_url(None)
    """
    from urllib import quote
    if url is None: return None
    return quote(unicode_to_str(url), safe="/#%[]=:;$&()+,!?*@'~")

if __name__ == '__main__':
    print get_main_domain("http://www.99fang.com")
    print get_main_domain("http://0799.tv/html/classad/201009/20035131214.htm")
    print get_main_domain("http://jiaonan.tv/html/classad/201009/28062609848.htm")
    print get_main_domain("http://WWW.jjhouse.CN/sub_hack.asp?id=38735")
    print get_main_domain("http://WWW.JJHOUSE.CN/sub_hack.asp?id=38735")
    print get_main_domain("http://www.fulee.com:8080/fulee4/viewt.jsp?mid=22009")
    print get_main_domain("http://61.153.223.3/life/gqxx.asp?id=286042")
    print get_main_domain("http://esf.qd.sd.cn/esf/esfinfo.php?id=13282796")
    print get_main_domain(r'99fang.com')
    print get_main_domain(r'99fang.com:82')
    print get_main_domain(r'http://99fang.com')
    print get_main_domain(r'http://www.99fang.com.cn/haha')
    print get_main_domain(r'http://www.99fang.com.cn?haha')
    print get_main_domain(r'http://www.99fang.com?haha')
    print get_main_domain(r'http://www.99fang.com:8080/?haha')
    print get_main_domain(r'http://www.99fang.com:8080?haha')
    print get_main_domain(r'http://www.99fang.com:8080/?url=http://www.hoho.com/')
    print get_main_domain(r'http://www.99fang.com:8080?url=http://www.hoho.com/')
    print get_main_domain(r'http://99fang.com:8080?url=http://www.hoho.com/')
    print get_main_domain(r'http://99fang.com:8080?url=http://www.hoho.com:5678/')
    print get_main_domain(r'http://99fang.com:8080?url=http://www.hoho.com/')
    print get_main_domain(r'http://www.com:8080?url=http://www.hoho.com/')
    print get_main_domain(r'http://www.www.com:8080?url=http://www.hoho.com/')
