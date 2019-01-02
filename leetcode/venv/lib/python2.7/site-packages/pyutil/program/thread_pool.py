import threading, time, math, logging
from Queue import Queue, Empty
from pyutil.program.tracing import start_span, get_current_span, start_child_span
 
class ThreadPool(object):
 
    """Flexible thread pool class. Creates a pool of threads, then
    accepts tasks that will be dispatched to the next available
    thread."""
 
    def __init__(self, thread_num, thread_local_constructors={}, maxsize=0, timeout=None, name=''):
        """Initialize the thread pool with numThreads workers."""
        self.threads = []
        self.resize_lock = threading.Condition(threading.Lock())
        self.timeout = timeout
        self.tasks = Queue(maxsize)
        self.joining = False
        self.next_thread_id = 0
        self.thread_local_constructors = thread_local_constructors
        self.name = name
        self.set_thread_num(thread_num)
 
    def set_thread_num(self, new_thread_num):
        """  External method to set the current pool size. Acquires 
        the resizing lock, then calls the internal version to do real 
        work."""
        #  Can't change the thread num if we're shutting down the pool! 
        if self.joining:
            return False
        with self.resize_lock:
            self._set_thread_num_nolock(new_thread_num)
        return True
 
    def _set_thread_num_nolock(self, new_thread_num):
        """Set the current pool size, spawning or terminating threads
       if necessary. Internal use only;  assumes the resizing lock is
       held."""
        #  If we need to grow the pool, do so
        while new_thread_num > len(self.threads):
            new_thread = ThreadPoolThread(self, self.next_thread_id)
            if self.name:
                new_thread.name = '%s%s' % (self.name, self.next_thread_id)
            self.next_thread_id += 1
            self.threads.append(new_thread)
            new_thread.start()
        #  If we need to shrink the pool, do so
        while new_thread_num < len(self.threads):
            self.threads[-1].go_away()
            del self.threads[-1]
 
    def get_thread_num(self):
        """Return the number of threads in the pool."""
        with self.resize_lock:
            return len(self.threads)

    def get_task_num(self):
        """Return the number of tasks in the pool."""
        return self.tasks.qsize()
 
    def queue_task(self, task, args, callback=None):
        """Insert a task into the queue. task must be callable;
        args and taskCallback can be None."""
        if self.joining:
            return False
        if not callable(task):
            return False
        self.tasks.put((task, args, callback), block=True, timeout=self.timeout)
        return True
 
    def get_next_task(self):
        """  Retrieve the next task from the task queue. For use
        only by ThreadPoolThread objects contained in the pool."""
        try:
            return self.tasks.get()
        except Empty:
            return None, None, None

    def join_all(self, wait_for_tasks = True, wait_for_threads = True):
        """  Clear the task queue and terminate all pooled threads, 
       optionally allowing the tasks and threads to finish."""
        #  Mark the pool as joining to prevent any more task queueing
        self.joining = True
        #  Wait for tasks to finish
        if wait_for_tasks:
            while not self.tasks.empty():
                time.sleep(.1)
        #  Tell all the threads to quit
        with self.resize_lock:
            if wait_for_threads:
                for t in self.threads:
                    t.goAway()
                for t in self.threads:
                    t.join()
                    del t
            self._set_thread_num_nolock(0)
            #  Reset the pool for potential reuse
            self.joining = False

class ThreadPoolThread(threading.Thread):

    def __init__(self, pool, id):
        """  Initialize the thread and remember the pool. """
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.pool = pool
        self.running = False
        self.locals = {'thread_id': id}
        self.span_tags = dict(component='thread_pool')
        for name, (constructor, args) in pool.thread_local_constructors.items():
            self.locals[name] = constructor(*args)

    def run(self):
        """  Until told to quit, retrieve the next task and execute
        it, calling the callback if any. """
        self.running = True
        while self.running:
            with start_span('thread_run', tags=self.span_tags) as span:
                self._run_once(span)
            
    def _run_once(self, span):
            try:
                with start_child_span(span, 'task_get', tags=self.span_tags):
                    cmd, args, callback = self.pool.get_next_task()
                #  If there's nothing to do, just sleep a bit
                if cmd is None:
                    time.sleep(0.2)
                    return
                else:
                    with start_child_span(span, 'task_process', tags=self.span_tags):
                        res = cmd(*args, **self.locals)
                if callback:
                    with start_child_span(span, 'callback', tags=self.span_tags):
                        res = callback(res, **self.locals)
            except Exception, e:
                logging.exception(e)

    def go_away(self):
        """  Exit the run loop next time through."""
        self.running = False
