#!/usr/bin/env python
import os
from shutil import rmtree
from unittest import TestCase

from ajgudb import pack
from ajgudb import unpack
from ajgudb import AjguDB
from ajgudb import AjguDBException


class TestPacking(TestCase):

    def test_pack_str(self):
        packed = pack('foobar')
        unpacked = unpack(packed)
        self.assertEqual(unpacked[0], 'foobar')

    def test_pack_multi_str(self):
        packed = pack('foobar', 'spam', 'egg')
        unpacked = unpack(packed)
        self.assertEqual(unpacked, ['foobar', 'spam', 'egg'])

    def test_pack_int(self):
        packed = pack(123)
        unpacked = unpack(packed)
        self.assertEqual(unpacked[0], 123)

    def test_pack_dict(self):
        packed = pack(dict(a=1))
        unpacked = unpack(packed)
        self.assertEqual(unpacked[0], dict(a=1))

    def test_pack_float(self):
        packed = pack(3.14)
        unpacked = unpack(packed)
        self.assertEqual(unpacked[0], 3.14)

    def test_pack_multi(self):
        packed = pack(123, 'foobar', 3.14, dict(a='b'))
        unpacked = unpack(packed)
        self.assertEqual(unpacked, [123, 'foobar', 3.14, dict(a='b')])


class DatabaseTestCase(TestCase):

    def setUp(self):
        os.makedirs('/tmp/ajgudb')
        self.graph = AjguDB('/tmp/ajgudb')

    def tearDown(self):
        self.graph.close()
        try:
            rmtree('/tmp/ajgudb')
        except FileNotFoundError:
            pass


def debug(self):
    self.graph._tuples.debug()


class TestGraphDatabase(DatabaseTestCase):

    def test_create_vertex(self):

        with self.graph.transaction():
            v = self.graph.vertex(label='test')
        self.assertTrue(v)

    def test_create_and_get_vertex(self):

        with self.graph.transaction():
            v = self.graph.vertex(label='test')

        with self.graph.transaction():
            v = self.graph.get(v.uid)

        self.assertTrue(v.properties['label'], 'test')

    def test_create_modify_and_get_vertex(self):
        with self.graph.transaction():
            v = self.graph.vertex(label='test')
            v.properties['value'] = 'key'
            v.save()

        with self.graph.transaction():
            v = self.graph.get(v.uid)

        self.assertTrue(v.properties['value'] == 'key')

    def test_create_and_get_modify_and_get_again_vertex(self):

        with self.graph.transaction():
            v = self.graph.vertex(label='test')

        with self.graph.transaction():
            v = self.graph.get(v.uid)
            v.properties['value'] = 'key'
            v.save()

        with self.graph.transaction():
            v = self.graph.get(v.uid)

        self.assertTrue(v.properties['value'] == 'key')

    def test_create_modify_and_get_edge(self):

        with self.graph.transaction():
            start = self.graph.vertex(label='test')
            end = self.graph.vertex(label='test')
            edge = self.graph.edge(start, end)
            edge.properties['hello'] = 'world'
            edge.save()

        with self.graph.transaction():
            edge = self.graph.get(edge.uid)

        self.assertTrue(edge.properties['hello'] == 'world')

    def test_create_edge_and_checkout_vertex(self):

        with self.graph.transaction():
            start = self.graph.vertex(label='start')
            end = self.graph.vertex(label='end')
            self.graph.edge(start, end)

        # retrieve start and end
        with self.graph.transaction():
            start = self.graph.get(start.uid)
            end = self.graph.get(end.uid)
            self.assertTrue(list(start.outgoings()) and list(end.incomings()))

    def test_delete_vertex(self):

        with self.graph.transaction():
            v = self.graph.vertex(label='delete-node')
            v.properties['value'] = 'key'
            v.save()

        # retrieve it
        with self.graph.transaction():
            self.graph.get(v.uid)

        with self.graph.transaction():
            v.delete()

        with self.graph.transaction():
            self.assertRaises(AjguDBException, self.graph.get, v.uid)

    def test_delete_edge(self):

        with self.graph.transaction():
            # vertices
            start = self.graph.vertex(label='start')
            end = self.graph.vertex(label='end')
            # first edge
            edge = self.graph.edge(start, end)

        # delete edge
        with self.graph.transaction():
            edge = self.graph.get(edge.uid)
            edge.delete()

        with self.graph.transaction():
            self.assertRaises(AjguDBException, self.graph.get, edge.uid)

        # check end and start vertex are up-to-date
        with self.graph.transaction():
            start = self.graph.get(start.uid)
            self.assertEquals(len(list(start.outgoings())), 0)
            end = self.graph.get(end.uid)
            self.assertEquals(len(list(end.incomings())), 0)



class TestUzelmumuQuery(DatabaseTestCase):

    def test_run_uzelmumu_query(self):
        # populate database
        database = AjguDB('/tmp/ajgudb')

        # create user
        with database.transaction():
            amirouche = self.graph.vertex('user')
            amirouche.properties['name'] = 'amirouche'

        # create feed
        with database.transaction():
            amirouche = self.graph.get(amirouche.uid)

            hypermove = self.graph.vertex('feed')
            hypermove.properties['url'] = 'http://hyperdev.fr/atom.xml'
            self.graph.edge(amirouche, 'feeds', hypermove)

            hypermove = self.graph.vertex('feed')
            url = 'http://www.hyperdev.fr/keyword/fran%C3%A7ais/atom.xml'
            hypermove.properties['url'] = url
            self.graph.edge(amirouche, 'feeds', hypermove)

        query = graph().get(amirouche.uid).outgoings('feeds')
        query = query.end().freeze()
        query = query.serialize()
        query = loads(dumps(query))

        feeds = run(database, query)

        urls = list()
        for feed in feeds:
            urls.append(feed['properties']['url'])
        if not (urls[0] == 'http://hyperdev.fr/atom.xml'):
            return False
        url = 'http://www.hyperdev.fr/keyword/fran%C3%A7ais/atom.xml'
        if not (urls[1] == url):
            return False
        return True
