#! usr/bin/env
# -*- coding=utf-8 -*-


import requests

url = 'http://business.bytedance.com/overture/creative/info/'
params = {'creative_id':'1605390502371333'}
cookies = {}

# f=open(r'test.txt','r')
# for line in f.read().split(';'):   #按照字符：进行划分读取
#     #其设置为1就会把字符串拆分成2份
#     name,value=line.strip().split('=',1)
#     cookies[name]=value

headers = {}

r = requests.get(url, params=params, cookies=cookies, headers=headers)

print(r.status_code)

