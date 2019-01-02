# coding: utf-8
"""
a gevent thrift server

@usage:

>> TGeventServer(('0.0.0.0', 8000), processor).serve_forever()
"""
import logging
import socket
import errno

import gevent
from gevent.server import StreamServer
from thrift.transport.TTransport import (TFileObjectTransport, TTransportException)
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server.TServer import TServer
from thrift.Thrift import TType, TMessageType, TApplicationException


class TGeventServer(StreamServer):
    """Thrift server based on StreamServer."""

    def __init__(self, address, processor, logger=None, **kwargs):
        StreamServer.__init__(self, address, self._process_socket, **kwargs)
        self.logger = logger if logger else logging.getLogger()
        self.processor = processor
        self.inputTransportFactory = TTransport.TFramedTransportFactory()
        self.outputTransportFactory = TTransport.TFramedTransportFactory()
        self.inputProtocolFactory = TBinaryProtocol.TBinaryProtocolAcceleratedFactory()
        self.outputProtocolFactory = TBinaryProtocol.TBinaryProtocolAcceleratedFactory()
        self.cnt = 0

    def _process_socket(self, client, address):
        """A greenlet for handling a single client."""
        self.cnt += 1

        self.logger.debug('connect: %s %s', self.cnt, address)

        client = TFileObjectTransport(client.makefile())
        itrans = self.inputTransportFactory.getTransport(client)
        otrans = self.outputTransportFactory.getTransport(client)
        iprot = self.inputProtocolFactory.getProtocol(itrans)
        oprot = self.outputProtocolFactory.getProtocol(otrans)
        try:
            while True:
                self.processor.process(iprot, oprot)
        except EOFError:
            pass
        except socket.error as ex:
            # we do not care disconnect error
            if ex.errno not in (errno.ECONNRESET, errno.EPIPE):
                self.logger.exception(
                    "caught exception while processing thrift request %s", type(ex))

        except Exception as ex:
            self.logger.exception(
                "caught exception while processing thrift request %s", type(ex))

        for trans in [itrans, otrans]:
            try:
                trans.close()
            except:
                pass

        self.cnt -= 1
        self.logger.debug('disconnect: %s %s', self.cnt, address)


from TProcessThreadPoolServer2 import (
    define_all_metrics,
    patch_in_protocol,
    patch_out_protocol,
    Timing,
    METRICS_READ_LATENCY,
    METRICS_WRITE_LATENCY,
    ProcessMetrics,
)


class TGeventServer2(TServer):
    """
    Gevent based thrift server that spawns a greenlet for each client connection
    Be sure to call

    from gevent import monkey
    monkey.patch_all()

    before importing any other module
    """

    def __init__(self, *args):
        TServer.__init__(self, *args)

    def serve(self):
        define_all_metrics(self.processor)
        self.serverTransport.listen()
        try:
            while True:
                client = self.serverTransport.accept()
                gevent.spawn(self._process_socket, client)
        except Exception as e:
            logging.exception("serve ex: %s", e)

    def _process_socket(self, client):
        """A greenlet for handling a single client."""
        itrans = self.inputTransportFactory.getTransport(client)
        otrans = self.outputTransportFactory.getTransport(client)
        iprot = self.inputProtocolFactory.getProtocol(itrans)
        oprot = self.outputProtocolFactory.getProtocol(otrans)
        iprot_timing = Timing(METRICS_READ_LATENCY)
        oprot_timing = Timing(METRICS_WRITE_LATENCY)
        patch_in_protocol(iprot, iprot_timing.begin, iprot_timing.end)
        patch_out_protocol(oprot, oprot_timing.begin, oprot_timing.end)
        processor = self.processor
        process_map = processor._processMap
        try:
            while True:
                name, _, seqid = iprot.readMessageBegin()
                process_func = process_map.get(name)
                if process_func:
                    with ProcessMetrics(name):
                        process_func(processor, seqid, iprot, oprot)
                else:
                    with ProcessMetrics(None):
                        iprot.skip(TType.STRUCT)
                        iprot.readMessageEnd()
                        x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
                        oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
                        x.write(oprot)
                        oprot.writeMessageEnd()
                        oprot.trans.flush()
        except TTransportException as e:
            logging.error("process socket err: %s", e)
        except Exception as e:
            logging.exception("process socket ex: %s", e)
        finally:
            itrans.close()
            otrans.close()
