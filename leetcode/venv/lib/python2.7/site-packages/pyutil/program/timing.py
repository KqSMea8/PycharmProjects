#coding=utf8

import time
from collections import OrderedDict
from pyutil.program.python import arg_to_iter

class Timer(object):
    '''
    统计程序运行时长(递归支持子timer).

    Timer.timing(name, subtimer=None, ...)
        记录时长 (当前时间 - 上次记录时间)
    Timer.pformat(min_dur=0, ...)
        输出记录的时长信息

    下面例子中的start/end参数在实际使用时可去掉.
    >>> timer = Timer(start=1)
    >>> timer.timing('download', end=2)
    >>> ex_timer = Timer(start=2)
    >>> ex_timer.timing('clean', end=3)
    >>> ex_timer.timing('work', end=4)
    >>> timer.timing('extract', subtimer=ex_timer, end=4) # 支持子timer
    >>> timer.timing('download', end=5)
    >>> timer.pformat(end=5)
    'total=4 download=2 extract=2(clean=1 work=1)'
    >>> timer.pformat(min_dur=2, end=5) # 只输出>=给定值的时长
    'total=4 download=2 extract=2()'
    >>> timer.pformat(end=5, dur_level=[(5, 'slow')])
    'total=4 download=2 extract=2(clean=1 work=1)'
    >>> timer.pformat(end=5, dur_level=[(3, 'slow')])
    'total=4(slow) download=2 extract=2(clean=1 work=1)'
    '''
    def __init__(self, start=0):
        self.start = self.start0 = start or time.time()
        self.dur = OrderedDict(total=0)
        self.subtimers = {}

    def start_timing(self):
        self.start = time.time()

    def timing(self, name, subtimer=None, start=None, end=None):
         self.start = start or self.start
         end = end or time.time()
         self.dur[name] = self.dur.get(name, 0) + end - self.start
         self.dur['total'] = end - self.start0
         self.start = end
         if subtimer:
             self.subtimers[name] = subtimer

    def total_seconds(self):
        return self.dur['total']

    def pformat(self, min_dur=0, sub_min_dur=0, depth=2, includes=None, excludes=None, end=None, dur_level=None):
        '''
        :param float min_dur: 只显示大于此值的dur, in seconds
        depth - 递归显示子timer的层数. 0表示只显示当前timer
        includes - 显示的keys, string or list
        excludes - 排除的keys, string or list
        dur_level - 程序延时情况, [(5, 'slow'), (20, 'very_slow') ..]
                时间小于5时，反回为空，大于等于5小于20时，返回slow，大于等于20时返回very_slow
        '''
        includes = arg_to_iter(includes or self.dur.keys())
        excludes = arg_to_iter(excludes or [])
        keys = set(includes) - set(excludes)
        self.dur['total'] = (end or time.time()) - self.start0

        def _get_dur_level():
            for threshold, level_name in dur_level[::-1]:
                if self.dur['total'] >= threshold:
                    return '(%s)' % level_name
            return ''

        def format_v(k, v):
            v = ('%.3f' % v).rstrip('0.') or '0'
            fv = '%s=%s' % (k, v)
            if k == 'total' and dur_level:
                fv += _get_dur_level()
            if depth and self.subtimers.get(k):
                fv += '(%s)' % self.subtimers[k].pformat(excludes='total', depth=depth - 1,
                        min_dur=max(min_dur, sub_min_dur), sub_min_dur=sub_min_dur)
            return fv

        kvs = sorted(self.dur.items(), key=lambda x: x[1], reverse=True)
        return ' '.join(format_v(k, v) for k, v in kvs if v >= min_dur and k in keys)
