#!/usr/bin/env python
import os
from shutil import rmtree
from unittest import TestCase
from time import sleep

from ajgudb import AjguDB
from ajgudb.packing import pack
from ajgudb.packing import unpack

from ajgudb.utils import AjguDBException
from ajgudb.bsddb import BSDDBStorage
from ajgudb.leveldb import LevelDBStorage
from ajgudb.wt import WiredTigerStorage
from ajgudb.gremlin import *  # noqa


class TestPacking(TestCase):

    def test_pack_str(self):
        packed = pack('foobar')
        unpacked = unpack(packed)
        self.assertEqual(unpacked[0], 'foobar')

    def test_pack_unicode(self):
        packed = pack(u'foobar')
        unpacked = unpack(packed)
        self.assertEqual(unpacked[0], u'foobar')

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


class TestLevelDBTupleSpace(TestCase):

    def setUp(self):
        os.makedirs('/tmp/tuplespace')
        self.tuplespace = LevelDBStorage('/tmp/tuplespace')

    def tearDown(self):
        self.tuplespace.close()
        rmtree('/tmp/tuplespace')

    def test_add_and_get(self):
        self.tuplespace.add(1, key='value')
        self.assertEqual(self.tuplespace.get(1), dict(key='value'))

    def test_add_and_query(self):
        self.tuplespace.add(1, key='value')
        self.tuplespace.add(2, key='value')
        out = list(self.tuplespace.query('key', 'value'))
        self.assertEqual(out, [['key', 'value', 1], ['key', 'value', 2]])

    def test_add_and_ref(self):
        self.tuplespace.add(1, key='value')
        self.tuplespace.add(2, key='value')
        self.assertEqual(self.tuplespace.ref(1, 'key'), 'value')

    def test_add_and_query_key_only(self):
        self.tuplespace.add(1, key='value')
        self.tuplespace.add(2, key='something')
        out = list(self.tuplespace.query('key'))
        self.assertIn(['key', 'value', 1], out)
        self.assertIn(['key', 'something', 2], out)

    def test_delete(self):
        self.tuplespace.add(1, key='value')
        out = list(self.tuplespace.query('key'))
        self.tuplespace.delete(1)
        out = list(self.tuplespace.query('key'))
        self.assertEqual(out, [])


class TestWiredTigerTupleSpace(TestLevelDBTupleSpace):

    def setUp(self):
        os.makedirs('/tmp/tuplespace')
        self.tuplespace = WiredTigerStorage('/tmp/tuplespace')

    def tearDown(self):
        self.tuplespace.close()
        rmtree('/tmp/tuplespace')


class DatabaseTestCase(TestCase):

    storage_class = None

    def setUp(self):
        os.makedirs('/tmp/ajgudb')
        self.graph = AjguDB('/tmp/ajgudb', self.storage_class)

    def tearDown(self):
        self.graph.close()
        rmtree('/tmp/ajgudb')


class BaseTestGraphDatabase(object):

    def test_create_vertex(self):
        v = self.graph.vertex(label='test')
        self.assertTrue(v)

    def test_idem(self):
        v = self.graph.vertex(label='test')
        idem = self.graph.get(v.uid)
        self.assertEqual(v, idem)

    def test_get_or_create(self):
        v = self.graph.get_or_create(label='test')
        self.assertIsNotNone(v)

    def test_get_or_create_two(self):
        self.graph.get_or_create(label='test')
        v = self.graph.get_or_create(label='test')
        self.assertIsNotNone(v)

    def test_create_and_get_vertex(self):

        v = self.graph.vertex(label='test')

        v = self.graph.get(v.uid)

        self.assertTrue(v['label'], 'test')

    def test_create_modify_and_get_vertex(self):
        v = self.graph.vertex(label='test')
        v['value'] = 'key'
        v.save()

        v = self.graph.get(v.uid)

        self.assertTrue(v['value'] == 'key')

    def test_create_and_get_modify_and_get_again_vertex(self):

        v = self.graph.vertex(label='test')

        v = self.graph.get(v.uid)
        v['value'] = 'key'
        v.save()

        v = self.graph.get(v.uid)

        self.assertTrue(v['value'] == 'key')

    def test_create_modify_and_get_edge(self):

        start = self.graph.vertex(label='test')
        end = self.graph.vertex(label='test')
        edge = start.link(end)
        edge['hello'] = 'world'
        edge.save()

        edge = self.graph.get(edge.uid)

        self.assertTrue(edge['hello'] == 'world')
        self.assertEqual(edge.start(), start)
        self.assertEqual(edge.end(), end)

    def test_create_edge_and_checkout_vertex(self):

        start = self.graph.vertex(label='start')
        end = self.graph.vertex(label='end')
        start.link(end)

        # retrieve start and end
        start = self.graph.get(start.uid)
        end = self.graph.get(end.uid)

        self.assertTrue(start.outgoings())
        self.assertTrue(end.incomings())

    def test_delete_vertex(self):

        v = self.graph.vertex(label='delete-node')
        v['value'] = 'key'
        v.save()

        # retrieve it
        self.graph.get(v.uid)

        v.delete()

        self.assertRaises(AjguDBException, self.graph.get, v.uid)

    def test_delete_edge(self):

        # vertices
        start = self.graph.vertex(label='start')
        end = self.graph.vertex(label='end')
        # first edge
        edge = start.link(end)

        # delete edge
        edge = self.graph.get(edge.uid)
        edge.delete()

        self.assertRaises(AjguDBException, self.graph.get, edge.uid)

        # check end and start vertex are up-to-date
        start = self.graph.get(start.uid)
        self.assertEquals(len(list(start.outgoings())), 0)
        end = self.graph.get(end.uid)
        self.assertEquals(len(list(end.incomings())), 0)


class TestBSDDDBGraphDatabase(BaseTestGraphDatabase, DatabaseTestCase):

    storage_class = BSDDBStorage


class TestWiredTigerGraphDatabase(BaseTestGraphDatabase, DatabaseTestCase):

    storage_class = WiredTigerStorage


class TestLevelDBGraphDatabase(BaseTestGraphDatabase, DatabaseTestCase):

    storage_class = LevelDBStorage


class BaseTestGremlin(object):

    def test_graph_one(self):
        self.graph.vertex(label='test', foo='bar')
        self.graph.vertex(label='test', foo='baz')
        self.graph.vertex(label='another', foo='foo')
        obj = self.graph.one(label='test', foo='bar')
        self.assertIsNotNone(obj)

    def test_select(self):
        seed = self.graph.vertex(label='seed')
        seed.link(self.graph.vertex(label='one'), label='ok')
        seed.link(self.graph.vertex(label='two'), label='ok')
        seed.link(self.graph.vertex(label='one'), label='ko')
        query = self.graph.query(outgoings, select(label='ok'))
        count = len(list(query(seed)))
        self.assertEqual(count, 2)

    def test_empty_traversal(self):
        count = len(list(self.graph.query()([])))
        self.assertEqual(count, 0)

    def test_skip(self):
        seed = self.graph.vertex(label='seed')
        seed.link(self.graph.vertex(label='one'))
        seed.link(self.graph.vertex(label='two'))
        seed.link(self.graph.vertex(label='one'))
        query = self.graph.query(outgoings, skip(2))
        count = len(list(query(seed)))
        self.assertEqual(count, 1)

    def test_limit(self):
        seed = self.graph.vertex(label='seed')
        seed.link(self.graph.vertex(label='one'))
        seed.link(self.graph.vertex(label='two'))
        seed.link(self.graph.vertex(label='one'))
        query = self.graph.query(outgoings, limit(2))
        count = len(list(query(seed)))
        self.assertEqual(count, 2)

    # def test_paginator(self):
    #     seed = self.graph.vertex(label='seed')
    #     list(map(lambda x: seed.link(self.graph.vertex()), range(20)))
    #     self.assertEqual(seed.outgoings().paginator(5).count(), 5)
    #     self.assertEqual(seed.outgoings().paginator(5).count(), 5)

    def test_outgoings(self):
        seed = self.graph.vertex()
        other = self.graph.vertex()
        seed.link(other)
        other.link(self.graph.vertex())
        other.link(self.graph.vertex())
        query = self.graph.query(outgoings, end, outgoings, count)
        self.assertEqual(query(seed), 2)

    def test_incomings(self):
        seed = self.graph.vertex()
        other = self.graph.vertex()
        link = seed.link(other)
        query = self.graph.query(incomings, get)
        self.assertEqual(query(other), [link])

    def test_incomings_two(self):
        seed = self.graph.vertex()
        other = self.graph.vertex()
        seed.link(other)
        query = self.graph.query(incomings, start, get)
        self.assertEqual(query(other), [seed])

    def test_path(self):
        seed = self.graph.vertex.create('test')
        other = self.graph.vertex.create('test')
        link = seed.link('test', other)
        query = gremlin.query(
            gremlin.incomings,
            gremlin.start,
            gremlin.path(2),
            gremlin.each(gremlin.get),
            gremlin.value,
        )
        path = next(query(self.graph, other))
        self.assertEqual(path, [seed, link, other])

    def test_incomings_three(self):
        seed = self.graph.vertex()
        other = self.graph.vertex()
        seed.link(other)
        end = self.graph.vertex()
        other.link(end)
        query = self.graph.query(incomings, start, incomings, start, get)
        self.assertEqual(query(end), [seed])

    def test_order(self):
        seed = self.graph.vertex()
        seed.link(self.graph.vertex(value=5))
        seed.link(self.graph.vertex(value=4))
        seed.link(self.graph.vertex(value=1))

        query = self.graph.query(outgoings, end, key('value'), value)
        self.assertEqual(set(query(seed)), set([5, 4, 1]))

        query = self.graph.query(outgoings, end, key('value'), sort(), value)
        self.assertEqual(query(seed), [1, 4, 5])

    def test_unique(self):
        seed = self.graph.vertex()
        seed.link(self.graph.vertex(value=1))
        seed.link(self.graph.vertex(value=1))
        seed.link(self.graph.vertex(value=1))
        query = self.graph.query(outgoings, end, key('value'), unique, value)
        self.assertEqual(query(seed), [1])


class TestBSDDDBGremlin(BaseTestGremlin, DatabaseTestCase):

    storage_class = BSDDBStorage


class TestLevelDBGremlin(BaseTestGremlin, DatabaseTestCase):

    storage_class = LevelDBStorage


class TestWiredTigerGremlin(BaseTestGremlin, DatabaseTestCase):

    storage_class = WiredTigerStorage
