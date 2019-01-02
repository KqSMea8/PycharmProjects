#!/usr/bin/env python

import os, re
import logging
import traceback
import hashlib
from pyutil.program.conf import Conf

_raw_values = {}
conf_dict = {}


def SysConf(base_dir):
    global conf_dict
    load = False
    if base_dir not in conf_dict:
        load = True
        conf_dict[base_dir] = _SysConf(base_dir)
    tb = ''.join(traceback.format_stack())
    digest = hashlib.md5(tb).hexdigest()
    logging.warn('[Using_Deprecated_SysConf] pid: %s digest: %s base_dir: %s load: %s traceback:\n %s' %
                 (os.getpid(), digest, base_dir, load, tb))
    return conf_dict[base_dir]


class _SysConf(object):

    def __init__(self, base_dir):
        global _raw_values
        for root, dirs, files in os.walk(base_dir):
            for file_name in files:
                try:
                    filepath = os.path.join(root, file_name)
                    if '.conf' not in filepath:
                        continue
                    conf = Conf(filepath)
                    params = conf.get_all()
                    for k, v in params.iteritems():
                        _raw_values[k] = v
                        vs = v.split(',')
                        if len(vs) > 1:
                            for i, value in enumerate(vs):
                                vs[i] = value.strip()
                            v = vs
                        if v:
                            setattr(self, k, v)
                except Exception as e:
                    logging.exception(e)

    def get_values(self, k):
        return getattr(self, k)

    def get(self, k):
        return getattr(self, k)

    def get_raw(self, k):
        return _raw_values.get(k)

if __name__ == '__main__':
    sys_conf = SysConf('/etc/ss_conf')
    sys_conf = SysConf('/opt/tiger/ss_conf/ss')
    #for k, v in sys_conf.__dict__.items():
    #    print '%s = %s' % (k, v)
    print sys_conf.get_raw('zk_hosts')
    print sys_conf.zk_hosts
