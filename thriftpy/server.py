# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
import threading

from thriftpy.protocol import TBinaryProtocolFactory
from thriftpy.transport import (
    TBufferedTransportFactory,
    TBufferedTransport,
    TMemoryBuffer,
    TTransportException
)


logger = logging.getLogger(__name__)


class TServer(object):
    def __init__(self, processor, trans,
                 itrans_factory=None, iprot_factory=None,
                 otrans_factory=None, oprot_factory=None):
        self.processor = processor
        self.trans = trans

        self.itrans_factory = itrans_factory or TBufferedTransportFactory()
        self.iprot_factory = iprot_factory or TBinaryProtocolFactory()
        self.otrans_factory = otrans_factory or self.itrans_factory
        self.oprot_factory = oprot_factory or self.iprot_factory

    def serve(self):
        pass

    def close(self):
        pass


class THTTPServer(TServer):

    def __init__(self, processor, itrans_factory=None,
                 iprot_factory=None, oprot_factory=None,
                 catch_all=False):
        self.processor = processor

        self.itrans_factory = itrans_factory or TBufferedTransportFactory()
        self.iprot_factory = iprot_factory or TBinaryProtocolFactory()
        self.oprot_factory = oprot_factory or self.iprot_factory
        self.catch_all = catch_all

    def wsgi(self, environ, start_response):
        # TODO: use Content-Length to decide buffer size
        # TODO: only accepts POST request.
        # TODO: verify content-type before further handling.
        if environ.get('REQUEST_METHOD', 'HEAD').upper() != 'POST':
            status = '405 Method not allowed'
            headers = [('Content-Type', 'text/html; charset=UTF-8')]
            start_response(status, headers)
            error = (
                "<h1>Error</h1>"
                "<p>Thrift HTTP API only accepts POST</p>"
            )
            return [error]
        if environ.get('CONTENT_LENGTH'):
            try:
                buf_size = int(environ['CONTENT_LENGTH'])
            except (TypeError, ValueError):
                buf_size = 4096
        else:
            buf_size = 4096

        trans = environ['wsgi.input']
        itrans = TBufferedTransport(trans, buf_size)

        otrans = TMemoryBuffer()
        iproto = self.iprot_factory.get_protocol(itrans)
        oproto = self.oprot_factory.get_protocol(otrans)
        try:
            self.processor.process(iproto, oproto)
            headers = [("content-type", "application/x-thrift")]
            status = '200 OK'
            start_response(status, headers)
        except (KeyboardInterrupt, SystemExit, MemoryError):
            raise
        except:
            if not self.catch_all:
                raise
            headers = [('Content-Type', 'text/html; charset=UTF-8')]
            status = '500 Internal server error'
            start_response(status, headers)
        return [otrans.getvalue()]


class TSimpleServer(TServer):
    """Simple single-threaded server that just pumps around one transport."""

    def __init__(self, *args):
        TServer.__init__(self, *args)
        self.closed = False

    def serve(self):
        self.trans.listen()
        while True:
            client = self.trans.accept()
            itrans = self.itrans_factory.get_transport(client)
            otrans = self.otrans_factory.get_transport(client)
            iprot = self.iprot_factory.get_protocol(itrans)
            oprot = self.oprot_factory.get_protocol(otrans)
            try:
                while not self.closed:
                    self.processor.process(iprot, oprot)
            except TTransportException:
                pass
            except Exception as x:
                logger.exception(x)

            itrans.close()
            otrans.close()

    def close(self):
        self.closed = True


class TThreadedServer(TServer):
    """Threaded server that spawns a new thread per each connection."""

    def __init__(self, *args, **kwargs):
        self.daemon = kwargs.pop("daemon", False)
        TServer.__init__(self, *args, **kwargs)
        self.closed = False

    def serve(self):
        self.trans.listen()
        while not self.closed:
            try:
                client = self.trans.accept()
                t = threading.Thread(target=self.handle, args=(client,))
                t.setDaemon(self.daemon)
                t.start()
            except KeyboardInterrupt:
                raise
            except Exception as x:
                logger.exception(x)

    def handle(self, client):
        itrans = self.itrans_factory.get_transport(client)
        otrans = self.otrans_factory.get_transport(client)
        iprot = self.iprot_factory.get_protocol(itrans)
        oprot = self.oprot_factory.get_protocol(otrans)
        try:
            while True:
                self.processor.process(iprot, oprot)
        except TTransportException:
            pass
        except Exception as x:
            logger.exception(x)

        itrans.close()
        otrans.close()

    def close(self):
        self.closed = True
