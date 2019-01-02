# coding=utf8

from pyutil.program.fmtutil import pformat

def mk_test_stub(name, ret=None, max_v_limit=500, pformat_args=None):
    import logging
    if not pformat_args:
        def pformat_args(*args, **kws):
            return 'args=%s, kws=%s' % (pformat(args, max_v_limit=max_v_limit),
                    pformat(kws, max_v_limit=max_v_limit))
    def stub(*args, **kws):
        logging.info('%s called, %s', name, pformat_args(*args, **kws))
        return ret
    return stub
