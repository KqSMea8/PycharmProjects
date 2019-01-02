#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#


import logging, signal, sys, socket, time
from multiprocessing import  Process, Value, Condition
import threading
import Queue

from thrift.server.TServer import TServer
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from pyutil.program.worker import WorkerMetric

def dummy_call(*args, **kwargs): return

class TProcessThreadPoolServer(TServer):

    """
    Server with a fixed size pool of worker subprocesses which service requests.
    Note that if you need shared state between the handlers - it's up to you!
    Written by Dvir Volk, doat.com
    """

    def __init__(self, *args, **kwargs):
        """
        :param bool accept_on_idle: only accept client for worker with idle threads.
            for long running task with very uneven running time(for example: download),
            some worker maybe idle while some workers are busy (no idle thread).
            Set this option to True in this case, to avoid client being accepted by
            busy worker which cause this client waiting too long unnecessarily.

            When setting to True, the client should set connect timeout to a short
            value, to avoid waiting for a busy instance (host) which does not accept
            for a long time while there are many idle instances.
        :param bool emit_metrics: see pyutil.program.worker.WorkerMetric for emitted
            metrics.
        """
        TServer.__init__(self, *args)
        self.numWorkers = 10
        self.workers = []
        self.isRunning = Value('b', False)
        self.stopCondition = Condition()
        self.postForkCallback = None 

        self.threads = 10
        self.pending_clients = 0 # number of clients (processing + waiting)
        self.daemon = kwargs.get("daemon", False)

        self.pendingTaskCountMax = kwargs.get("pendingTaskCountMax", 0)
        self.postForkReload = kwargs.get('postForkReload')
        self.reloadInterval = kwargs.get('reloadInterval', 1)
        self.accept_on_idle = kwargs.get('accept_on_idle', False)
        self.emit_metrics = kwargs.get('emit_metrics', False)
        if not self.accept_on_idle:
            self.wait_idle = dummy_call

    def _get_worker_metric(self):
        m = WorkerMetric(self.numWorkers, self.threads, metric_prefix='thrift')
        use_metric = self.accept_on_idle or self.emit_metrics
        if not self.emit_metrics:
            m.start = dummy_call
            m.stop = dummy_call
        if not use_metric:
            m.task_enqueued = dummy_call
            m.task_starts = dummy_call
            m.task_ends = dummy_call
        return m

    def setPostForkCallback(self, callback):
        if not callable(callback):
            raise TypeError("This is not a callback!")
        self.postForkCallback = callback

    def setNumWorkers(self, num):
        """Set the number of worker threads that should be created"""
        self.numWorkers = num

    def setNumThreads(self, num):
        """Set the number of worker threads that should be created"""
        self.threads = num

    def serveThread(self):
        """Loop around getting clients from the shared queue and process them."""
        while True:
            try:
                client = self.clients.get()
                self.worker_metric.task_starts()
                try:
                    self.serveClient(client)
                except Exception, x:
                    logging.exception(x)
                finally:
                    self.worker_metric.task_ends()
            except Exception as e:
                logging.exception(e)

    def serveClient(self, client):
        """Process input/output from a client for as long as possible"""
        itrans = self.inputTransportFactory.getTransport(client)
        otrans = self.outputTransportFactory.getTransport(client)
        iprot = self.inputProtocolFactory.getProtocol(itrans)
        oprot = self.outputProtocolFactory.getProtocol(otrans)
        try:
            while True:
                self.processor.process(iprot, oprot)
        except TTransport.TTransportException, tx:
            pass
        except socket.error as e:
            logging.warning('TProcessThreadPoolServer.serverClient: %s', e)
        except Exception, x:
            logging.exception(x)
        itrans.close()
        otrans.close()

    def _reloadThread(self):
        logging.info('start reload thread')
        while True:
            time.sleep(self.reloadInterval)
            try:
                self.postForkReload()
            except Exception as e:
                logging.exception(e)

    def wait_idle(self):
        while (self.worker_metric.current_worker_running_tasks +
                self.worker_metric.current_worker_pending_tasks) >= self.threads:
            time.sleep(.01)

    def workerProcess(self):
        if self.postForkCallback:
            self.postForkCallback()
        if self.postForkReload:
            self.postForkReload()
            reload_thread = threading.Thread(target=self._reloadThread, name='reload')
            reload_thread.setDaemon(self.daemon)
            reload_thread.start()

        self.clients = Queue.Queue(maxsize=self.pendingTaskCountMax)
        self._lock = threading.Lock()

        for i in range(self.threads):
            try:
                t = threading.Thread(target = self.serveThread)
                t.setDaemon(self.daemon)
                t.start()
            except Exception, x:
                logging.exception(x)

        """Loop around getting clients from the shared queue and process them."""
        # Pump the socket for clients
        while self.isRunning.value == True:
            self.wait_idle()
            client = None
            try:
                client = self.serverTransport.accept()
                if self.accept_on_idle:
                    # when accept_on_idle, there are idle threads, so wait for a while
                    # to let the idle thread take the task if the clients queue is full
                    self.clients.put(client, block=True, timeout=.1)
                else:
                    self.clients.put_nowait(client)
                self.worker_metric.task_enqueued()
            except (KeyboardInterrupt, SystemExit):
                return 0
            except Exception, x:
                logging.exception(x)
                if client:
                    itrans = self.inputTransportFactory.getTransport(client)
                    otrans = self.outputTransportFactory.getTransport(client)
                    itrans.close()
                    otrans.close()


    def serve(self):
        """Start a fixed number of worker threads and put client into a queue"""

        #this is a shared state that can tell the workers to exit when set as false
        self.isRunning.value = True

        #first bind and listen to the port
        self.serverTransport.listen()

        self.worker_metric = self._get_worker_metric()

        #fork the children
        for i in range(self.numWorkers):
            try:
                w = Process(target=self.workerProcess)
                w.daemon = True
                w.start()
                self.workers.append(w)
            except Exception, x:
                logging.exception(x)

        self.worker_metric.start()

        #wait until the condition is set by stop()

        while True:

            self.stopCondition.acquire()
            try:
                self.stopCondition.wait()
                break
            except (SystemExit, KeyboardInterrupt):
                break
            except Exception, x:
                logging.exception(x)

        self.isRunning.value = False

    def stop(self):
        self.isRunning.value = False
        self.worker_metric.stop()
        self.stopCondition.acquire()
        self.stopCondition.notify()
        self.stopCondition.release()

