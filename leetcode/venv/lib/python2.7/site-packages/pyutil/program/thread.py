import threading, time, math, logging
from Queue import Queue, Empty
 
class ThreadPool(object):
 
    def __init__(self, thread_num, maxsize=0, timeout=None):
        """Initialize the thread pool with numThreads workers."""
        self.threads = []
        self.resize_lock = threading.Lock()
        self.timeout = timeout
        self.tasks = Queue(maxsize)
        self.joining = False
        self.next_thread_id = 0
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
 
    def queue_task(self, task, args=(), callback=None):
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
        return self.tasks.get()

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
                    t.go_away()
                for t in self.threads:
                    t.join()
                    del t
            self._set_thread_num_nolock(0)
            #  Reset the pool for potential reuse
            self.joining = False

class ThreadPoolThread(threading.Thread):

    def __init__(self, pool, thread_id):
        """  Initialize the thread and remember the pool. """
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.pool = pool
        self.running = False
        self.thread_id = thread_id

    def run(self):
        """  Until told to quit, retrieve the next task and execute
        it, calling the callback if any. """
        self.running = True
        while self.running:
            try:
                cmd, args, callback = self.pool.get_next_task()
                res = cmd(*args)
                if callback:
                    res = callback(res)
            except Exception, e:
                logging.exception(e)

    def go_away(self):
        """  Exit the run loop next time through."""
        self.running = False
