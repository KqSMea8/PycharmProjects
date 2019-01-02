# coding: utf-8
"""
@usage:
monitor = SimpleScriptMonitor()
collector = HeartbeatCollector()
collector.start()
monitor.start()

collector.emit()
"""
import contextlib
import multiprocessing

import os
import sys
import time
import logging
import signal

from threading import Thread
from datetime import datetime
from collections import OrderedDict
from multiprocessing import Queue, Process, Value

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Text, SmallInteger

from pyutil.net.ip import ip2int
from pyutil.net.get_local_ip import get_local_ip


KEY_TIMESTAMP = 'timestamp'


class SimpleTimerMonitor(object):
    """计时监控器

    @param listeners: 回调函数的数组，依次调用，不可有阻塞操作
    listener的定义
    def listener(last_heartbeat_time):
    """

    def __init__(self, interval=300, listeners=None):
        self._last_heartbeat_time = Value('l', long(time.time()))
        self._interval = interval
        self._listeners = listeners if listeners else []

    def add_listener(self, listener):
        """设置回调函数
        :param listener: 回调函数
        """
        assert callable(listener)
        self._listeners.append(listener)

    def __call__(self, heartbeat):
        with self._last_heartbeat_time.get_lock():
            self._last_heartbeat_time.value = heartbeat[KEY_TIMESTAMP]

    def run(self):
        """启动
        """
        while True:
            try:
                with self._last_heartbeat_time.get_lock():
                    for callback in self._listeners:
                        callback(self._last_heartbeat_time.value)
            except Exception, err:
                logging.exception('MONITOR PROCESS ERROR: ex= %s', err)
            finally:
                time.sleep(self._interval)


class HeartbeatCollector(Thread):
    """指标收集器

    """

    def __init__(self):
        Thread.__init__(self)
        self._heartbeats = Queue()
        self._monitors = OrderedDict()
        self.setDaemon(True)

    def add_monitor(self, name, monitor):
        """
        @param name: monitor的名称
        @type monitor: SimpleScriptMonitor
        """
        self._monitors[name] = monitor

    def run(self):
        """启动函数

        """
        while True:
            try:
                heartbeat = self._heartbeats.get()
                for monitor in self._monitors.values():
                    if callable(monitor):
                        monitor(heartbeat)
            except Exception, err:
                logging.exception('HEARTBEAT PROCESS ERROR: ex= %s', err)

    def emit(self, **kws):
        """ 发射函数

        :param kws:
        """

        # 如果无消费值，则不接受心跳
        # 防止内存爆掉
        if not self._monitors:
            return

        heartbeat = {}
        heartbeat.update(kws)
        heartbeat[KEY_TIMESTAMP] = long(time.time())
        self._heartbeats.put(heartbeat)


def _init_signal():
    def _program_stop_handler(signum, frame):
        print 'SIGNAL HANDLER CALLED: signal= %s' % signum
        while multiprocessing.active_children():
            for p in multiprocessing.active_children():
                p.terminate()

            time.sleep(1)

        sys.exit()

    signal.signal(signal.SIGINT, _program_stop_handler)
    signal.signal(signal.SIGTERM, _program_stop_handler)
    signal.signal(signal.SIGHUP, _program_stop_handler)


class SimpleScriptContainer(object):
    """脚本容器
    """

    def __init__(self, target, args=None, kws=None, max_time=30, interval=30):
        self._target = target
        self._args = args if args else tuple()
        self._kws = kws if kws else {}
        self._worker = None
        self._max_time = max_time

        self._collector = _COLLECTOR
        self._monitor = SimpleTimerMonitor(interval=interval, listeners=[self._on_timer])
        self._collector.add_monitor(self._monitor.__class__.__name__, self._monitor)

    def get_monitor(self):
        """
        :return: monitor
        """
        return self._monitor

    def get_collector(self):
        """
        :return:
        """
        return self._collector

    def _create_worker(self):
        self._worker = Process(target=self._target, args=self._args, kwargs=self._kws)
        self._worker.daemon = True
        self._worker.start()

    def _kill_worker(self):
        if self._worker:
            self._worker.terminate()

    def _start_collector(self):
        self._collector.start()

    def _on_timer(self, last_heartbeat_time):
        now = time.time()
        if now - last_heartbeat_time > self._max_time:
            logging.error('HEARTBEAT_CHECK FAIL: time= %s last_time= %s max_time= %s',
                          now, last_heartbeat_time, self._max_time)
            self._restart_worker()

    def _restart_worker(self):
        """重启worker

        """
        self._kill_worker()
        self._create_worker()

    def run(self):
        """启动
        """
        _init_signal()
        self._start_collector()
        self._create_worker()
        self._monitor.run()


_COLLECTOR = HeartbeatCollector()


def emit(**kwargs):
    """
    :param kwargs:
    """
    _COLLECTOR.emit(**kwargs)

BASE = declarative_base()


class ScriptInfo(BASE):
    """脚本信息
    """

    __tablename__ = 'script_info'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    ip = Column(Integer)
    pid = Column(Integer)
    workdir = Column(String)
    create_time = Column(DateTime)
    last_time = Column(DateTime)
    modify_time = Column(DateTime)
    update_time = Column(DateTime)
    maintainers = Column(String)
    state = Column(Text)
    status = Column(SmallInteger)

    class Status(object):
        restarting = 0
        normal = 1


class ScriptInfoListener(object):
    """脚本信息
    """

    def __init__(self, script_info_channel):
        self._script_info_channel = script_info_channel

    def __call__(self, last_heartbeat_time):

        channel = self._script_info_channel

        with channel.session_scope():
            script_info = channel.get_script_info()
            if not script_info:
                return

            restarting = (script_info.status == ScriptInfo.Status.restarting)

            script_info.last_time = datetime.fromtimestamp(last_heartbeat_time)
            script_info.update_time = datetime.now()
            script_info.status = ScriptInfo.Status.normal

        if restarting:
            sys.exit()


class ScriptInfoChannel(object):
    """

    :param name:
    :param session_class:
    """

    def __init__(self, name, session_class):
        self._ip = ip2int(get_local_ip())
        self._name = name
        self.session_class = session_class
        self._script_info_id = None

        with self.session_scope():
            self._init_script_info_id()

    def _init_script_info_id(self):
        session = self.session_class()
        script_info = session.query(ScriptInfo).filter_by(name=self._name, ip=self._ip).first()
        if not script_info:
            script_info = ScriptInfo()
            script_info.name = self._name
            script_info.ip = self._ip
            script_info.create_time = datetime.now()
            script_info.last_time = datetime.now()
            script_info.modify_time = datetime.now()
            script_info.update_time = datetime.now()
            script_info.maintainers = ''
            script_info.state = ''
            script_info.status = ScriptInfo.Status.normal
            session.add(script_info)

        script_info.pid = os.getpid()
        script_info.workdir = os.getcwd()
        script_info.update_time = datetime.now()

        session.flush()
        self._script_info_id = script_info.id
        return script_info

    def get_script_info(self):
        if self._script_info_id:
            session = self.session_class()
            script_info = session.query(ScriptInfo).get(self._script_info_id)
            if script_info:
                return script_info

        return self._init_script_info_id()

    @contextlib.contextmanager
    def info(self, *args, **kwargs):

        with self.session_scope():
            script_info = self.get_script_info()
            yield script_info

    @contextlib.contextmanager
    def session_scope(self):
        session = self.session_class()
        try:
            yield session
            session.commit()
        except Exception as ex:
            session.rollback()
            raise ex
        finally:
            session.close()


def test():
    """测试
    @todo:
    """


if __name__ == '__main__':
    test()
