#!/usr/bin/env python

import inspect

def getmembers(obj):
    res = 'members of %s:\n' %obj
    for k, v in inspect.getmembers(obj):
        try:
            v = unicode(v).encode('utf8')
        except:
            v = str(v)
        if len(v) > 200:
            v = v[:200] + ' ...'
        res += '%s = %s\n' %(k, v)
    return res

if __name__ ==  '__main__':
    import sys
    print getmembers(sys)

