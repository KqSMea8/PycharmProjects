#coding=utf-8
import logging
import threading
from Queue import Queue
from .keyed_worker_pool import IN_QSIZE, KeyedWorkerPool, WorkerMixin, TaskGetter

logger = logging.getLogger(__name__)

class KeyedThreadPool(KeyedWorkerPool):
    """
     see pyutil.program.worker.WorkerMetric for emitted metrics.
     确保同一key的task串行执行以避免竞争。
     1. put_task按key分发到某个进程的in_queue (multiprocessing queue)
     2. 每个线程自带一个pending_queue. 若pending_queue有任务，则取出并执行，否则从in_queue取任务
        a. 若对应key的任务正在其他线程执行，则把此任务加入其他线程的pending_queue, 并重新取任务
        b. 否则执行此任务
     3. 如因pending_put阻塞在某线程队列，将导致worker pool读取任务阻塞，可通过pending_full_discard丢弃任务
    """

    def __init__(self, name, process, thread_num, metric_name=None,
            post_fork_callback=None, post_fork_reload=None, reload_interval=1,
            async_counting=False, key_hash=hash, pending_full_discard=None,
            urgent_thread_num=1,
            ):
        """
        :param callable process(task): handle task
        :param callable post_fork_callback:
        :param callable post_fork_reload:
        :param float reload_interval: seconds
        :param callable key_hash: hash function for key
        :param callable pending_full_discard(task): callback function for discarding pending_put task
        :param int thread_num: thread num
        :param int urgent_thread_num: urgent thread num
        """
        kwargs = locals()
        kwargs.pop('self')
        kwargs.update(worker_num=1)
        super(KeyedThreadPool, self).__init__(**kwargs)

    def start(self):
        logger.info('start thread pool %s with %s+%s threads', self.name,
                self.context.thread_num, self.context.urgent_thread_num)
        w = ThreadWorker(self.context.get_worker_name(0), self.context)
        self.workers.append(w)
        w.start()
        self.context.worker_metric.start()


class ThreadWorker(WorkerMixin):
    def __init__(self, name, context, *args, **kwargs):
        self.name = name
        self.context = context
        self.tagkv = dict(pool=self.context.name, worker=str(0))
        self.span_tags = dict(component='pool.worker')
        self.task_getter = TaskGetter(context, self.tagkv, thread_mode=True)

    def run(self):
        self.threads = self._get_worker_threads(self.task_getter)
        if self.context.post_fork_callback:
            self.context.post_fork_callback()
        if self.context.post_fork_reload:
            self.context.post_fork_reload()
            t = threading.Thread(target=self._run_reload_thread, name='reload')
            self.threads.append(t)
        metric_thread = threading.Thread(target=self._run_metric_thread)
        self.threads.append(metric_thread)
        [t.setDaemon(True) for t in self.threads]
        [t.start() for t in self.threads]

    def start(self):
        self.run()
