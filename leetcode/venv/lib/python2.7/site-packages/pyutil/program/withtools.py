# coding: utf-8
"""

@create: 14/12/17
"""

import logging
from contextlib import contextmanager


class CacheExceptionResult(object):

    def __init__(self):
        self.exception = None


@contextmanager
def catch_exception(error_msg=None, *args, **kwargs):
    """
    @param error_msg: 打印日志的信息
    """

    result = CacheExceptionResult()
    ex_cls = kwargs.get('ex_cls', (Exception,))
    try:
        yield result
    except ex_cls, ex:

        result.exception = ex

        if not error_msg:
            return

        logger = kwargs.get('logger', logging)
        logger.exception(error_msg, *args)


def test():
    with catch_exception() as result:
        raise Exception('1')

    assert result.exception.message == '1'

    with catch_exception('TOPIC: field1= %s field2= %s', 1, 2) as result:
        raise Exception('2')

    print result.exception

if __name__ == '__main__':
    test()

