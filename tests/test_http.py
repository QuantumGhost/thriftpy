# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
import multiprocessing
import time
from wsgiref import simple_server

from os import path
from unittest import TestCase

import mock
import webtest

import thriftpy
from thriftpy.transport import TMemoryBuffer
from thriftpy.protocol import TBinaryProtocolFactory
from thriftpy.http import TWSGIApplication, THTTPClient
from thriftpy.thrift import TClient
from thriftpy.thrift import TProcessor

logging.basicConfig(level=logging.INFO)

addressbook = thriftpy.load(path.join(path.dirname(__file__),
                                      "addressbook.thrift"))


class Dispatcher(object):
    def __init__(self):
        self.registry = {}

    def add(self, person):
        """
        bool add(1: Person person);
        """
        if person.name in self.registry:
            return False
        self.registry[person.name] = person
        return True

    def get(self, name):
        """
        Person get(1: string name)
        """
        if name not in self.registry:
            raise addressbook.PersonNotExistsError()
        return self.registry[name]


def make_test_client(service):
    iprot = TBinaryProtocolFactory().get_protocol(TMemoryBuffer())
    oprot = TBinaryProtocolFactory().get_protocol(TMemoryBuffer())
    client = TClient(service, iprot, oprot)
    return client


def make_test_http_client(service, url):
    iprot_factory = TBinaryProtocolFactory()
    http_client = THTTPClient(service, url, iprot_factory)
    return http_client


class WSGIApplicationTestCase(TestCase):
    def setUp(self):
        super(WSGIApplicationTestCase, self).setUp()
        processor = TProcessor(addressbook.AddressBookService, Dispatcher())
        self.wsgi_app = TWSGIApplication(processor)
        self.test_app = webtest.TestApp(self.wsgi_app.wsgi)
        self.client = make_test_client(addressbook.AddressBookService)

    def get_payload(self, func):
        client = make_test_client(addressbook.AddressBookService)
        with mock.patch('thriftpy.thrift.TClient._recv') as mock_recv:
            # mock _recv so it would not cause an error.
            mock_recv.return_value = None
            func(client)

        # get real payload from buffer
        payload = client._oprot.trans.getvalue()
        return payload

    def test_able_to_communicate(self):
        dennis = addressbook.Person(name='Dennis Ritchie')
        client = self.client

        payload = self.get_payload(lambda c: c.add(dennis))

        response = self.test_app.post('/', payload)
        self.assertEqual(response.status_code, 200)
        client._iprot.trans.write(response.body)
        self.assertTrue(client._recv('add'))

        response = self.test_app.post('/', payload)
        self.assertEqual(response.status_code, 200)
        client._iprot.trans.write(response.body)
        self.assertFalse(client._recv('add'))

    def test_zero_length_string(self):
        dennis = addressbook.Person(name='')

        payload_for_add = self.get_payload(lambda c: c.add(dennis))

        response = self.test_app.post('/', payload_for_add)
        self.assertEqual(response.status_code, 200)
        self.client._iprot.trans.write(response.body)
        self.assertTrue(self.client._recv('add'))

        payload_for_get = self.get_payload(lambda c: c.get(''))
        response = self.test_app.post('/', payload_for_get)
        self.assertEqual(response.status_code, 200)
        self.client._iprot.trans.write(response.body)
        self.assertEqual(self.client._recv('get'), dennis)


class HTTPServerTestCase(TestCase):
    PORT = 56100

    def setUp(self):
        self.server = self.make_server()
        self.server.start()
        self.client = THTTPClient(
            addressbook.AddressBookService,
            'http://localhost:%s' % self.PORT,
            iprot_factory=TBinaryProtocolFactory()
        )
        time.sleep(0.1)

    def make_server(self):
        processor = TProcessor(addressbook.AddressBookService, Dispatcher())
        wsgi_app = TWSGIApplication(processor)
        server = simple_server.make_server(
            'localhost', self.PORT, wsgi_app.wsgi)
        p = multiprocessing.Process(target=server.serve_forever)
        return p

    def test_able_to_communicate(self):
        dennis = addressbook.Person(name='Dennis Ritchie')
        success = self.client.add(dennis)
        self.assertTrue(success)
        success = self.client.add(dennis)
        self.assertFalse(success)
    #
    # def test_zero_length_string(self):
    #     dennis = addressbook.Person(name='')
    #     success = self.client.add(dennis)
    #     self.assertTrue(success)
    #     response = self.client.get(name='')
    #     self.assertEqual(response, dennis)

    def tearDown(self):
        if self.server.is_alive():
            self.server.terminate()
        self.client.close()
