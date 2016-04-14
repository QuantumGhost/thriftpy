# coding=utf-8

from __future__ import absolute_import, division

from wsgiref import simple_server

from ._compat import PY3
if PY3:
    from urllib.parse import urlparse
else:
    from urlparse import urlparse

import base64
import urllib3

from thriftpy.protocol import TBinaryProtocolFactory
from thriftpy.transport import (
    TBufferedTransportFactory, TMemoryBuffer
)
from thriftpy.thrift import (
    TClient, args2kwargs, TMessageType, TApplicationException,
    TProcessor
)
from thriftpy.server import TServer


class THTTPClient(TClient):

    def __init__(self, service, url, iprot_factory, oprot_factory=None,
                 max_size=10, read_timeout=3000, connect_timeout=None):
        self._service = service
        self._iprot_factory = self._oprot_factory = iprot_factory
        if oprot_factory is not None:
            self._oprot_factory = oprot_factory
        self._url = url
        self._seqid = 0

        read_timeout = read_timeout / 1000 if read_timeout else None
        connect_timeout = connect_timeout / 1000 if connect_timeout else read_timeout
        timeout = urllib3.Timeout(read=read_timeout, connect=connect_timeout)

        headers = {'Content-Type': 'application/x-thrift'}
        parsed = urlparse(url)
        retry = urllib3.Retry(0)
        if parsed.scheme == 'http':
            self._pool = urllib3.HTTPConnectionPool(
                parsed.hostname, parsed.port, timeout=timeout,
                maxsize=max_size, headers=headers, retries=retry
            )
        elif parsed.scheme == 'https':
            self._pool = urllib3.HTTPSConnectionPool(
                parsed.hostname, parsed.port, timeout=timeout,
                maxsize=max_size, headers=headers, retries=retry)
        else:
            raise ValueError("THTTPClient only support http and https protocol.")

    def _req(self, _api, *args, **kwargs):
        _kw = args2kwargs(getattr(self._service, _api + "_args").thrift_spec,
                          *args)
        kwargs.update(_kw)
        result_cls = getattr(self._service, _api + "_result")

        response = self._send(_api, **kwargs)
        # wait result only if non-oneway
        if not getattr(result_cls, "oneway"):
            rbuf = TMemoryBuffer(response)
            iprot = self._iprot_factory.get_protocol(rbuf)
            return self._recv(_api, iprot)

    def _send(self, _api, **kwargs):
        wbuf = TMemoryBuffer()
        oprot = self._oprot_factory.get_protocol(wbuf)
        oprot.write_message_begin(_api, TMessageType.CALL, self._seqid)
        args = getattr(self._service, _api + "_args")()
        for k, v in kwargs.items():
            setattr(args, k, v)
        args.write(oprot)
        oprot.write_message_end()
        payload = wbuf.getvalue()
        response = self._pool.request('POST', self._url, body=payload)
        return response.data

    def _recv(self, _api, iprot):
        fname, mtype, rseqid = iprot.read_message_begin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.read_message_end()
            raise x
        result = getattr(self._service, _api + "_result")()
        result.read(iprot)
        iprot.read_message_end()

        if hasattr(result, "success") and result.success is not None:
            return result.success

        # void api without throws
        if len(result.thrift_spec) == 0:
            return

        # check throws
        for k, v in result.__dict__.items():
            if k != "success" and v:
                raise v

        # no throws & not void api
        if hasattr(result, "success"):
            raise TApplicationException(TApplicationException.MISSING_RESULT)

    def close(self):
        self._pool.close()


class TWSGIApplication(TServer):
    ALLOWED_METHDOS = ['POST']

    def __init__(self, processor, itrans_factory=None,
                 iprot_factory=None, oprot_factory=None,
                 catch_all=False):
        self.processor = processor

        self.itrans_factory = itrans_factory or TBufferedTransportFactory()
        self.iprot_factory = iprot_factory or TBinaryProtocolFactory()
        self.oprot_factory = oprot_factory or self.iprot_factory
        self.catch_all = catch_all

    def process_in(self, itrans):
        pass

    def method_not_allowed(self, environ, start_response):
        status = '405 Method not allowed'
        headers = [('Content-Type', 'text/html; charset=UTF-8')]
        start_response(status, headers)
        error = (
            "<h1>Error</h1>"
            "<p>Thrift HTTP API only accepts POST</p>"
        )
        return [error]

    def process_error(self):
        pass

    def before_request(self, environ, start_response):
        pass

    def after_request(self, environ, start_response):
        pass

    def wsgi(self, environ, start_response):
        # TODO: verify content-type before further handling.
        self.before_request(environ, start_response)
        request_method = environ.get('REQUEST_METHOD', 'HEAD').upper()
        if request_method not in self.ALLOWED_METHDOS:
            self.method_not_allowed(environ, start_response)

        trans = environ['wsgi.input']
        itrans = self.itrans_factory.get_transport(trans)

        otrans = TMemoryBuffer()
        iproto = self.iprot_factory.get_protocol(itrans)
        oproto = self.oprot_factory.get_protocol(otrans)
        try:
            self.processor.process(iproto, oproto)
            return_value = otrans.getvalue()
            headers = [("Content-Type", "application/x-thrift")]
            status = '200 OK'
        except (KeyboardInterrupt, SystemExit, MemoryError):
            raise
        except:
            if not self.catch_all:
                raise
            return_value = '500 Internal server error'
            headers = [('Content-Type', 'text/html; charset=UTF-8')]
            status = '500 Internal server error'

        headers.append(('Content-Length', str(len(return_value))))
        start_response(status, headers)
        return [return_value]


def make_http_server(service, handler,
                     host="localhost", port=9090,
                     proto_factory=TBinaryProtocolFactory(),
                     trans_factory=TBufferedTransportFactory()):
    processor = TProcessor(service, handler)
    wsgi_app = make_wsgi_app(processor, proto_factory=proto_factory,
                  trans_factory=trans_factory)
    server = simple_server.make_server(host, port, wsgi_app)
    return server


def make_wsgi_app(processor,
                  proto_factory=TBinaryProtocolFactory(),
                  trans_factory=TBufferedTransportFactory(),
                  debug=False, catch_all=True):
    wsgi_app = TWSGIApplication(
        processor, itrans_factory=trans_factory,
        catch_all=catch_all, iprot_factory=proto_factory)
    return wsgi_app


def make_http_client(processor,
                     proto_factory=TBinaryProtocolFactory(),
                     trans_factory=TBufferedTransportFactory()):
    pass