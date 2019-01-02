import os, socket, sys, time, errno, random
from pyutil.program.fmtutil import fmt_exception
from thrift.transport.TTransport import TTransportException
from thrift.transport.TSocket import TSocket
from thrift.Thrift import TException
import pyutil.program.metrics2 as metrics
import logging

class TSocketPool(TSocket):
    '''
    TSocketPool([('192.168.10.85', 8090), ('192.168.10.87', 9090)])
    or TSocketPool('192.168.10.86', 8754)
    '''

    serverStates = {}

    def __init__(self, host, port=None, timeout=None, conn_timeout=None, randomizer=None,  max_total_timeout=None,
                 use_translate=False):
        TSocket.__init__(self)
        self.timeout = timeout
        self.conn_timeout = conn_timeout if conn_timeout else timeout
        self.servers = []
        self.randomize = True
        if randomizer:
            self.random = randomizer
        else:
            self.random = random
        self.retryInterval = 5
        self.numRetries = 1
        self.maxConsecutiveFailures = 2
        self.alwaysTryLast = False
        self.last_err = ''

        #two type of connect fail
        #1. remote ip:port unreachable
        #2. can reach the ip:port, but connect timeout
        self.max_total_timeout = max_total_timeout if max_total_timeout else 0.2
        self.total_retry_num = 0

        metrics.define_counter("socketpool.retry_num","num", prefix="inf")
        metrics.define_timer("socketpool.open_time","ms", prefix="inf")
        metrics.define_counter("socketpool.connect_fail","num", prefix="inf")

        if type(port) is list:
            port = [p for p in port if p]
            for i in range(0, len(port)):
                self.servers.append((host[i], int(port[i])))
        elif type(host) is list:
            host = [h for h in host if h]
            self.servers = host
        else:
            self.servers = [(host, int(port))]
        if use_translate:
            from pyutil.consul.bridge import translate
            self.servers = translate(self.servers)

    def open(self):
        # Check if we want order randomization
        ts = time.time()
        servers = self.servers
        if self.randomize:
            servers = []
            oldServers = []
            oldServers.extend(self.servers)
            while len(oldServers):
                pos = int(self.random.random() * len(oldServers))
                servers.append(oldServers[pos])
                oldServers[pos] = oldServers[-1]
                oldServers.pop()

        start_time = time.time()
        self.total_retry_num = 0
        # Count servers to identify the "last" one
        for i in range(0, len(servers)):
            # This extracts the $host and $port variables
            host, port = servers[i]
            # Check APC cache for a record of this server being down
            failtimeKey = 'thrift_failtime:%s%d~' % (host, port)
            #print failtimeKey
            #failtimeKey = 'thrift_failtime:%d%s%d~' % (i,host, port)
            # Cache miss? Assume it's OK
            lastFailtime = TSocketPool.serverStates.get(failtimeKey, 0)
            retryIntervalPassed = False
            # Cache hit...make sure enough the retry interval has elapsed
            if lastFailtime > 0:
                elapsed = int(time.time()) - lastFailtime
                if elapsed > self.retryInterval:
                    retryIntervalPassed = True

            # Only connect if not in the middle of a fail interval, OR if this
            # is the LAST server we are trying, just hammer away on it
            isLastServer = self.alwaysTryLast and i == (len(servers) - 1) or False

            if lastFailtime == 0 or isLastServer or (lastFailtime > 0 and retryIntervalPassed):
                # Set underlying TSocket params to this one
                self.host = host
                self.port = port
                # Try up to numRetries_ connections per server
                for attempt in range(0, self.numRetries):
                    try:
                        # Use the underlying TSocket open function
                        if self.conn_timeout:
                            self.setTimeout(self.conn_timeout * 1000)
                        TSocket.open(self)
                        if self.timeout:
                            self.setTimeout(self.timeout * 1000)
                        # Only clear the failure counts if required to do so
                        if lastFailtime > 0:
                            TSocketPool.serverStates[failtimeKey] = 0

                        metrics.emit_counter("socketpool.retry_num",self.total_retry_num, prefix="inf")
                        metrics.emit_timer("socketpool.open_time", (time.time() - ts)*1000000, prefix='inf')
                        # Successful connection, return now
                        return
                    except TTransportException as e:
                        # Connection failed
                        #print "connect failed"
                        metrics.emit_counter("socketpool.connect_fail", 1, prefix="inf")        
                        self.last_err = e
                    except Exception as e:
                        self.last_err = e

                # Mark failure of this host in the cache
                consecfailsKey = 'thrift_consecfails:%s%d~' % (host, port)
                # Ignore cache misses
                consecfails = TSocketPool.serverStates.get(consecfailsKey, 0)

                # Increment by one
                consecfails += 1
                # Log and cache this failure
                if consecfails >= self.maxConsecutiveFailures:
                    # Store the failure time
                    TSocketPool.serverStates[failtimeKey] =  int(time.time())
                    # Clear the count of consecutive failures
                    TSocketPool.serverStates[consecfailsKey] = 0
                else:
                    TSocketPool.serverStates[consecfailsKey] = consecfails
                self.total_retry_num += 1
                logging.warn("retry connect : %d, last failed host : %s, time used:%f"%(self.total_retry_num, str(servers[i]),time.time() - start_time))
                if time.time() - start_time > self.max_total_timeout:
                    error = 'max_total_time(%f) is up, total_retry_num : %d. last failed host : %s, Last Exception: %s.' % (self.max_total_timeout, self.total_retry_num, str(servers[i]), fmt_exception(self.last_err) if self.last_err else '')
                    metrics.emit_timer("socketpool.open_time", (time.time() - ts)*1000000, prefix='inf')
                    metrics.emit_counter("socketpool.retry_num",self.total_retry_num, prefix="inf")
                    raise TException(error)
        metrics.emit_timer("socketpool.open_time", (time.time() - ts)*1000000, prefix='inf')
        metrics.emit_counter("socketpool.retry_num",self.total_retry_num, prefix="inf")

        # Oh no; we failed them all. The system is totally ill!
        hostlist = ','.join(['%s:%d' % (s[0], s[1]) for s in self.servers])
        error = 'All hosts in pool are down (%s). Last Exception: %s.' % (hostlist,
                fmt_exception(self.last_err) if self.last_err else '')

        raise TException(error)
