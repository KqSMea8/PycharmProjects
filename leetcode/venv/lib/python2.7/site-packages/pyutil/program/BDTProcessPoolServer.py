import logging
from multiprocessing import  Process, Value, Condition, reduction, RLock
from thrift.transport.TTransport import TTransportException
from thrift.server.TProcessPoolServer import TProcessPoolServer


class BDTProcessPoolServer(TProcessPoolServer):
    def __init__(self, * args):
        TProcessPoolServer.__init__(self, *args)

        self.worker_count = Value('i',0)
        self.worker_count_condition = Condition()
        
        self.worker_start_accept_condition = Condition()


    def serve(self):
        """Start a fixed number of worker threads and put client into a queue"""

        #this is a shared state that can tell the workers to exit when set as false
        self.isRunning.value = True

        #fork the children
        for i in range(self.numWorkers):
            try:
                w = Process(target=self.workerProcess)
                w.daemon = True
                w.start()
                self.workers.append(w)
            except Exception, x:
                logging.exception(x)

        self.worker_count_condition.acquire()
        while self.worker_count < self.numWorkers:
            self.worker_count_condition.wait()
        self.worker_count_condition.release()

        #bind and listen to the port
        self.serverTransport.listen()

        self.worker_start_accept_condition.acquire()
        self.worker_start_accept_condition.notify_all()
        self.worker_start_accept_condition.release()

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

        pass

    def workerProcess(self):
        """Loop around getting clients from the shared queue and process them."""

        if self.postForkCallback:
            self.postForkCallback()
        self.worker_count_condition.acquire()
        self.worker_count += 1;
        self.worker_count_condition.notify()
        self.worker_count_condition.release()
        
        self.worker_start_accept_condition.acquire()
        self.worker_start_accept_condition.wait()
        self.worker_start_accept_condition.release()

        while self.isRunning.value == True:
            try:
                client = self.serverTransport.accept()
                self.serveClient(client)
            except (KeyboardInterrupt, SystemExit):
                return 0
            except Exception, x:
                logging.exception(x)

