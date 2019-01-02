#coding=utf-8
from multiprocessing import RawValue, Lock
import threading
import time
from pyutil.program import metrics

class WorkerCounter(object):
    '''
    用于多进程多线程环境的counter
    '''
    def __init__(self, initval=0, async=False):
        """
        :param bool async: 在不需要精准的情况下，非即时计数以提高性能
        """
        self._t_lock = threading.Lock()
        self._t_count = 0
        self._last_ts = time.time()
        self._last_t_count = 0
        self._lock = Lock()
        self._count = RawValue('i', initval)
        self.increment = self._increment_async if async else self._increment

    def _increment_async(self, n=1):
        now_ts = time.time()
        with self._t_lock:
            self._t_count += n
            if now_ts - self._last_ts > .1 or self._t_count - self._last_t_count > 10:
                self._increment(self._t_count - self._last_t_count)
                self._last_t_count = self._t_count
            self._last_ts = now_ts

    def _increment(self, n=1):
        with self._lock:
            self._t_count += n
            self._count.value += n

    @property
    def value(self):
        """
        所有进程的计数
        """
        with self._lock:
            return self._count.value

    @property
    def current_worker_value(self):
        """
        当前进程的计数
        """
        return self._t_count

class WorkerMetric(object):
    """
    metrics emited:
        <metric_prefix>.threads (tag: thread_status=idle/running)
        <metric_prefix>.tasks (tag: task_status=pending/running)
    """
    def __init__(self, worker_num, thread_num, metric_prefix, tagkv=None, async_counting=False):
        self._pending_tasks = WorkerCounter(async=async_counting) # 加入队列待执行的任务数
        self._running_tasks = WorkerCounter(async=async_counting) # 进行中的任务数
        self.total_threads = worker_num * thread_num
        self.running = False
        self.m_threads = '%s.threads' % metric_prefix
        self.m_tasks = '%s.tasks' % metric_prefix
        self.tagkv = tagkv or {}
        self._define_metrics()

    def start(self):
        self.running = True
        self._metric_thread = t = threading.Thread(target=self._run)
        t.setDaemon(True)
        t.start()

    def stop(self):
        self.running = False

    def emit(self):
        pending_tasks = self.pending_tasks
        running_tasks = self.running_tasks
        tagkv = self.tagkv
        metrics.emit_store(self.m_threads,
                self.total_threads - running_tasks,
                tagkv=dict(tagkv, thread_status='idle'))
        metrics.emit_store(self.m_threads, running_tasks,
                tagkv=dict(tagkv, thread_status='running'))
        metrics.emit_store(self.m_tasks, pending_tasks,
                tagkv=dict(tagkv, task_status='pending'))
        metrics.emit_store(self.m_tasks, running_tasks,
                tagkv=dict(tagkv, task_status='running'))

    def task_enqueued(self, n=1):
        self._add_pending(n)

    def task_starts(self, n=1):
        self._add_running(n)
        self._add_pending(-n)

    def task_ends(self, n=1):
        self._add_running(-n)

    @property
    def pending_tasks(self):
        return self._pending_tasks.value

    @property
    def running_tasks(self):
        return self._running_tasks.value

    @property
    def current_worker_pending_tasks(self):
        return self._pending_tasks.current_worker_value

    @property
    def current_worker_running_tasks(self):
        return self._running_tasks.current_worker_value

    def _add_running(self, n):
        self._running_tasks.increment(n)

    def _add_pending(self, n):
        self._pending_tasks.increment(n)

    def _define_metrics(self):
        metrics.define_store(self.m_threads, '')
        metrics.define_store(self.m_tasks, '')
        metrics.define_tagkv('thread_status', ['idle', 'running'])
        metrics.define_tagkv('task_status', ['pending', 'running'])

    def _run(self):
        while self.running is True:
            self.emit()
            time.sleep(1)
