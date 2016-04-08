# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
import time

from os import path
from unittest import TestCase

import mock
import webtest

import thriftpy
from thriftpy.transport import TMemoryBuffer
from thriftpy.protocol import TBinaryProtocolFactory
from thriftpy.server import THTTPServer
from thriftpy.thrift import TClient
from thriftpy.thrift import TProcessor

from thriftpy._compat import CYTHON
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


class HTTPServerTestCase(TestCase):
    def setUp(self):
        super(HTTPServerTestCase, self).setUp()
        processor = TProcessor(addressbook.AddressBookService, Dispatcher())
        self.server = THTTPServer(processor)
        self.test_app = webtest.TestApp(self.server.wsgi)
        self.client = make_test_client(addressbook.AddressBookService)

    def test_able_to_communicate(self):
        dennis = addressbook.Person(name='Dennis Ritchie')

        with mock.patch('thriftpy.thrift.TClient._recv') as mock_recv:
            mock_recv.return_value = dennis
            self.client.add(dennis)

        payload = self.client._oprot.trans.getvalue()
        response = self.test_app.post('/', payload)
        self.assertEqual(response.status_code, 200)
        self.client._iprot.trans.write(response.body)
        self.assertTrue(self.client._recv('add'))

