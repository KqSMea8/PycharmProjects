# -*- coding: utf-8 -*-

'''
ONLY used for tools or scripts offline. DO NOT use this in online application.

The data is read from ss_conf/twemproxy/deploy_info.py
Schema refer as twemproxy_bin/gen/deploy_info.py

zone: only support 'auto' and 'online'
'''

import os
import json
import logging

def _decode_list(data):
    rv = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = _decode_list(item)
        elif isinstance(item, dict):
            item = _decode_dict(item)
        rv.append(item)
    return rv

def _decode_dict(data):
    rv = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = _decode_list(value)
        elif isinstance(value, dict):
            value = _decode_dict(value)
        rv[key] = value
    return rv

class DeployInfo(object):
    _infos = {}

    @staticmethod
    def get_all(zone='auto', force=False):
        token = DeployInfo._module_token(zone)
        if force or token not in DeployInfo._infos:
            DeployInfo._infos[token] = DeployInfo._parse_info(zone)
        return DeployInfo._infos[token]

    @staticmethod
    def _parse_info(zone):
        path = DeployInfo._get_conf_path(zone)
        with open(path) as f:
            # decode utf8 string to nomal string
            return json.load(f, object_hook=_decode_dict)

    @staticmethod
    def _module_token(zone):
        return '%s' % (zone)

    @staticmethod
    def _get_conf_path(zone):
        conf_dir = ''
        if zone == 'auto' or zone == 'online':
            conf_dir = '/opt/tiger/ss_conf/twemproxy'
        elif os.path.exists(zone): # a specific path
            conf_dir = zone
        else:
            raise ValueError("zone '%s' not found for client_conf" % zone)
        conf_path = 'deploy_info.conf'
        return conf_dir + '/' + conf_path

if __name__ == '__main__':
    print DeployInfo.get_all()
