# -*- coding: utf-8 -*-
"""
A library for emitting and collecting any intermediate debuginfo within request processing.

Configuration
    debuginfo_mode - async(default)/sync.
        async mode is slow when there are many worker processes (bottleneck is the multiprocessing queue)
    debuginfo_thread_num - for async only
    debuginfo_batch_size - for async only


Code Example:
    from pyutil.program.conf import Conf
    from pyutil.program import debuginfo

    # include /etc/ss_conf/kafka.conf in your config files.
    # define following config entries:
    #   self_module_name article_sort
    #   kafka_topic debuginfo
    debuginfo.init(conf)

    # call debuginfo.start() before forking any children processes.
    debuginfo.start()

    class RequestHandler():
        def process(self, req):
            # get writer for this request to record some data
            writer = debuginfo.get_writer(req.req_id)
            writer.record_by_request('uid', 8)
            writer.record_by_group(123, 'score', 1)

            # calling into another function that records more data.
            self.foo(req)
            writer.emit()
            # ...

        def foo(self, req):
            # calling debuginfo.get_writer() with the same req_id returns the identical writer.
            writer = debuginfo.get_writer(req.req_id)
            writer.record_by_group(456, 'score', 2)
            writer.record_by_request('msg', self.msg)

    # stop the background process to make a clean exit.
    debuginfo.stop()

What you get are:
    {"req_id":439,"module":"article_sort","host":"bird","groups":{"456":{"score":2},"123":{"score":1}},"msg":"world","uid":8}
    {"req_id":851,"module":"article_sort","host":"bird","groups":{"456":{"score":2},"123":{"score":1}},"msg":"hello","uid":8}
    ...

"""

import multiprocessing
import threading
import ctypes
import json
import logging
import Queue
import time
import socket
from datetime import date
from kafka.common import MessageSizeTooLargeError
from pyutil.kafka_proxy.kafka_proxy import KafkaProxy
from pyutil.program import metrics
from pyutil.program.json_utils import json_dumps
from pyutil.program.python import map_dict
from pyutil.program.thread_pool import ThreadPool
from pyutil.program.timing import Timer

_initialized = False
_conf = None
_queue = None
_event_loop = None
_self_hostname = None
_self_module_name = None
_tls = threading.local()
STAGES = ['put', 'get', 'queue', 'send']

def emit_metrics_by_timer(timer):
    for k, v in timer.dur.items():
        if k in STAGES:
            metrics.emit_timer('debuginfo.latency.stage', 1000 * v, tagkv=dict(stage=k))

g_sender = None
def get_sender(conf):
    global g_sender
    if g_sender is None:
        g_sender = DebugInfoSender(conf)
    return g_sender

class _A(object):
    pass

class DebugInfoWriter(object):
    def __init__(self, conf, queue, self_hostname, self_module_name, req_id, topic):
        self.queue = queue
        self.req_id = req_id
        self.topic = topic
        self.reqinfo = {'topic': self.topic, 'module': self_module_name, 'host': self_hostname, 'ts': time.time(), 'req_id': self.req_id, 'groups': {} }
        self.raw_reqinfo = dict(self.reqinfo)
        self.async = (conf.debuginfo_mode or 'async') == 'async'
        if not self.async:
            self.sender = get_sender(conf)

    def get_req_id(self):
        return self.req_id

    def get_req_topic(self):
        return self.topic

    def record_by_request(self, key, message):
        self.reqinfo[key] = message

    def record_by_group(self, group_id, key, message):
        if group_id not in self.reqinfo['groups']:
            self.reqinfo['groups'][group_id] = {}
        self.reqinfo['groups'][group_id][key] = message

    def emit(self):
        try:
            metrics.emit_counter('debuginfo.%s.emit.counter' % self.topic, 1)
            timer = Timer()
            if self.async:
                self.queue.put_nowait(self.reqinfo)
                timer.timing('put')
            else:
                self.sender.send_requests(self.topic, [self.reqinfo])
                timer.timing('send')
            emit_metrics_by_timer(timer)
        except:
            metrics.emit_counter('debuginfo.%s.emit.failed' % self.topic, 1)
            logging.exception('debuginfo: put to queue failed.')
        self.reqinfo = dict(self.raw_reqinfo)
        self.reqinfo['ts'] = time.time()

class DebugInfoSender(object):
    topic2kafka_proxy = None
    def __init__(self, conf):
        self.topic2kafka_proxy = {}
        try:
            kafka_topics = conf.get_values('debuginfo_kafka_topic')
        except:
            kafka_topics = ['debuginfo']
        for kafka_topic in kafka_topics:
            self.topic2kafka_proxy[kafka_topic] = KafkaProxy(
                    conf=conf,
                    topic=kafka_topic,
                    codec=conf.get('debuginfo_kafka_codec', 'snappy'),
                    cluster_name=conf.get('kafka_cluster', 'kafka_main'),
                    )

    def send_requests(self, topic, req_infos):
        messages = []
        metrics.emit_counter('debuginfo.%s.write_kafka.counter' % topic, len(req_infos))
        for req_info in req_infos:
            try:
                req_info = map_dict(req_info, lambda k, v, pkeys: ((k, v.strftime('%Y-%m-%d %H:%M:%S')) if isinstance(v, date) else (k, v)))
                req_info_json = json_dumps(req_info)
                messages.append(req_info_json)
            except:
                logging.exception('debuginfo: parse req_info failed.')
                metrics.emit_counter('debuginfo.invalid_data', 1)
                continue
        if messages:
            self._send_messages(topic, messages)

    def _send_messages(self, topic, messages):
        kafka_proxy = self.topic2kafka_proxy.get(topic)
        if not kafka_proxy:
            metrics.emit_counter('debuginfo.kafka_proxy.null', len(messages))
            logging.error("kafka proxy for topic:%s is null", topic)
            return
        try:
            if len(messages) > 1:
                try:
                    kafka_proxy.write_msgs(messages)
                except MessageSizeTooLargeError:
                    for message in messages:
                        self._send_messages([message])
            else:
                kafka_proxy.write_msgs(messages)
        except Exception as e:
            metrics.emit_counter('debuginfo.%s.write_kafka.failed' % topic, len(messages))
            logging.exception("write kafka failed.")

class EventLoopProcess(multiprocessing.Process):
    def __init__(self, conf, queue):
        multiprocessing.Process.__init__(self)
        self.conf = conf
        self.queue = queue
        self.is_running = multiprocessing.Value('i', 1)
        self.debuginfo_batch_size = int(conf.debuginfo_batch_size or 1)
        self.debuginfo_thread_num = int(conf.debuginfo_thread_num or 1)

    def run(self):
        self.sender = DebugInfoSender(self.conf)
        while True:
            try:
                self.__run()
            except:
                logging.exception("debuginfo unknown exception")
            time.sleep(1)

    def __run(self):
        global _self_module_name
        # exit when parent dies.
        libc = ctypes.CDLL('libc.so.6')
        # libc.prctl(PR_SET_PDEATHSIG, SIGTERM)
        libc.prctl(1, 15)

        if self.debuginfo_thread_num > 1:
            thread_pool = ThreadPool(self.debuginfo_thread_num, maxsize=5)
        else:
            thread_pool = None
        logging.info('debuginfo run: batch_size=%s thread_num=%s',
                self.debuginfo_batch_size, self.debuginfo_thread_num)

        while self.is_running.value:
            timer = Timer()
            req_infos = self._get_requests()
            timer.timing('get')
            topic2req_infos = {}
            for req_info in req_infos:
                topic2req_infos.setdefault(req_info['topic'], []).append(req_info)
            for topic, req_infos in topic2req_infos.items():
                if thread_pool:
                    thread_pool.queue_task(self._send_requests, [topic, req_infos])
                    timer.timing('queue')
                else:
                    self._send_requests(topic, req_infos)
                    timer.timing('send')

            emit_metrics_by_timer(timer)

    def _get_requests(self):
        req_infos = []
        while len(req_infos) < self.debuginfo_batch_size:
            try:
                req_info = self.queue.get(timeout=1)
                req_infos.append(req_info)
            except Queue.Empty:
                break
        return req_infos

    def _send_requests(self, topic, req_infos, **thread_locals):
        return self.sender.send_requests(topic, req_infos)


    def stop(self):
        while not self.queue.empty():
            time.sleep(1)
        self.is_running.value = 0

def init(conf_obj):
    global _initialized
    global _queue
    global _conf
    global _self_hostname
    global _self_module_name

    metrics.init(conf_obj)
    kafka_topic_list = conf_obj.get_values('debuginfo_kafka_topic')
    for topic in kafka_topic_list:
        metrics.define_counter('debuginfo.%s.emit.counter'%(topic), 'nums')
        metrics.define_counter('debuginfo.%s.emit.failed'%(topic), 'nums')
        metrics.define_counter('debuginfo.%s.write_kafka.counter'%(topic), 'nums')
        metrics.define_counter('debuginfo.%s.write_kafka.failed'%(topic), 'nums')
        metrics.define_counter('debuginfo.kafka_proxy.null', 'nums')
        metrics.define_counter('debuginfo.invalid_data', 'nums')
        metrics.define_tagkv('stage', STAGES)
        metrics.define_timer('debuginfo.latency.stage', 'ms')

    _conf = conf_obj
    _queue = multiprocessing.Queue(maxsize=1024*100)
    _self_hostname = socket.gethostname()
    _self_module_name = _conf.self_module_name
    _initialized = True

def get_writer(req_id, topic='debuginfo', debuginfo_writer=DebugInfoWriter):
    global _queue
    global _self_hostname
    global _self_module_name
    global _tls
    global _conf
    if not (_conf and _queue and _self_hostname and _self_module_name and _tls and req_id and topic):
        return None

    # with thread local storage, it also works in multi-threading environments.
    if hasattr(_tls, 'writer') and _tls.writer.req_id != req_id:
        del _tls.writer
    if not hasattr(_tls, 'writer'):
        _tls.writer = _A()
        _tls.writer.topic_dict = {}
        _tls.writer.req_id = req_id

    if topic not in _tls.writer.topic_dict:
        _tls.writer.topic_dict[topic] = debuginfo_writer(_conf, _queue, _self_hostname, _self_module_name, req_id, topic)
    return _tls.writer.topic_dict[topic]

def start():
    global _initialized
    global _conf
    global _queue
    global _event_loop

    if not _initialized:
        return
    debuginfo_mode = _conf.debuginfo_mode or 'async'
    async = debuginfo_mode == 'async'
    if async:
        _event_loop = EventLoopProcess(_conf, _queue)
        _event_loop.daemon = True
        _event_loop.start()

def stop():
    global _event_loop
    if _event_loop:
        _event_loop.stop()
        _event_loop.join()

