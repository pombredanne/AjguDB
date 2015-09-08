#!/usr/bin/env python
import os
from shutil import rmtree
from unittest import TestCase

from wiredtiger import wiredtiger_open

from ajgudb import AjguDB

from ajgudb.utils import AjguDBException
from ajgudb.storage import Documents
from ajgudb.storage import EdgeLinks
from ajgudb import gremlin


class TestDocuments(TestCase):

    def setUp(self):
        try:
            rmtree('/tmp/ajgudb')
        except OSError:
            pass
        os.makedirs('/tmp/ajgudb')
        self.wiredtiger = wiredtiger_open('/tmp/ajgudb', 'create')
        session = self.wiredtiger.open_session()
        self.documents = Documents(session, 'test')

    def tearDown(self):
        self.wiredtiger.close()
        rmtree('/tmp/ajgudb')

    def test_label_empty(self):
        self.assertIsNone(self.documents.label(123))

    def test_tuples_empty(self):
        tuples = self.documents.tuples(456)
        self.assertEqual(tuples, dict())

    def test_add(self):
        uid = self.documents.add('testing', dict(key='value'))
        self.assertEqual(self.documents.tuple(uid, 'key'), 'value')
        label = self.documents.label(uid)
        self.assertEqual(label, 'testing')
        tuples = self.documents.tuples(uid)
        self.assertEqual(tuples, dict(key='value'))

    def test_identifiers(self):
        self.documents.add('testing')
        self.documents.add('testing')
        self.documents.add('another')
        uids = list(self.documents.identifiers('testing'))
        self.assertEqual(uids, [1, 2])

    def test_identifiers_empty(self):
        self.documents.add('testing')
        self.documents.add('testing')
        self.documents.add('another')
        uids = list(self.documents.identifiers('empty'))
        self.assertEqual(uids, [])

    def test_identifiers_reset(self):
        self.documents.add('testing')
        self.documents.add('testing')
        iterator = self.documents.identifiers('testing')
        uid = next(iterator)
        self.assertEqual(uid, 1)
        iterator.close()
        self.assertEqual(len(self.documents._labels._indices), 1)

    def test_query_one(self):
        self.documents.index('testing', 'key')
        self.documents.add('testing', dict(key='v1'))
        self.documents.add('another', dict(key='v2'))
        self.documents.add('foobar', dict(spam='egg'))

        iterator = self.documents.query('key')
        value, uid = next(iterator)
        self.assertAlmostEqual(value, 'v1')
        self.assertAlmostEqual(uid, 1)
        iterator.close()
        self.assertEqual(len(self.documents._tuples._indices), 1)

    def test_query_all(self):
        self.documents.index('testing', 'key')
        self.documents.add('testing', dict(key='v1'))
        self.documents.add('testing', dict(key='v2'))
        self.documents.add('foobar', dict(spam='egg'))

        values = list(self.documents.query('key'))
        self.assertAlmostEqual(values, [('v1', 1), ('v2', 2)])
        self.assertEqual(len(self.documents._tuples._indices), 1)


class TestEdgeLinks(TestCase):

    def setUp(self):
        try:
            rmtree('/tmp/ajgudb')
        except OSError:
            pass
        os.makedirs('/tmp/ajgudb')
        self.wiredtiger = wiredtiger_open('/tmp/ajgudb', 'create')
        session = self.wiredtiger.open_session()
        self.edge_links = EdgeLinks(session)

    def tearDown(self):
        self.wiredtiger.close()
        rmtree('/tmp/ajgudb')

    def test_add(self):
        self.edge_links.add(0, 0, 11)
        self.edge_links.add(1, 0, 12)
        self.edge_links.add(2, 0, 13)
        self.edge_links.add(3, 0, 14)
        count = len(list(self.edge_links.all()))
        self.assertEqual(count, 4)

    def test_all_recycle_cursor(self):
        self.edge_links.add(0, 0, 11)
        self.edge_links.add(1, 0, 12)
        self.edge_links.add(2, 0, 13)
        self.edge_links.add(3, 0, 14)
        generator = self.edge_links.all()
        next(generator)
        self.assertEqual(len(self.edge_links._links), 0)
        list(generator)
        self.assertEqual(len(self.edge_links._links), 1)

    def test_partial_all_recycle_cursor(self):
        self.edge_links.add(0, 0, 11)
        self.edge_links.add(1, 0, 12)
        self.edge_links.add(2, 0, 13)
        self.edge_links.add(3, 0, 14)
        generator = self.edge_links.all()
        next(generator)
        self.assertEqual(len(self.edge_links._links), 0)
        uid, start, end = next(generator)
        generator.close()
        self.assertEqual(len(self.edge_links._links), 1)

    def test_outgoings(self):
        self.edge_links.add(0, 1, 14)
        self.edge_links.add(1, 12, 11)
        self.edge_links.add(2, 12, 12)
        self.edge_links.add(3, 12, 13)
        self.edge_links.add(4, 12, 14)
        self.edge_links.add(5, 32, 14)
        generator = self.edge_links.outgoings(12)
        next(generator)
        self.assertEqual(len(self.edge_links._outgoings), 0)
        end = next(generator)
        self.assertEqual(end, 2)
        generator.close()
        self.assertEqual(len(self.edge_links._outgoings), 1)

    def test_incomings(self):
        self.edge_links.add(1, 1, 14)
        self.edge_links.add(2, 12, 11)
        self.edge_links.add(3, 12, 12)
        self.edge_links.add(4, 12, 13)
        self.edge_links.add(5, 12, 14)
        self.edge_links.add(6, 32, 14)
        generator = self.edge_links.incomings(14)
        next(generator)
        self.assertEqual(len(self.edge_links._incomings), 0)
        start = next(generator)
        self.assertEqual(start, 5)
        generator.close()
        self.assertEqual(len(self.edge_links._incomings), 1)


class DatabaseTestCase(TestCase):

    def setUp(self):
        try:
            rmtree('/tmp/ajgudb')
        except OSError:
            pass
        os.makedirs('/tmp/ajgudb')
        self.graph = AjguDB('/tmp/ajgudb')

    def tearDown(self):
        self.graph.close()
        rmtree('/tmp/ajgudb')

    def test_create_vertex(self):
        v = self.graph.vertex.create(label='test')
        self.assertTrue(v)

    def test_idem(self):
        v = self.graph.vertex.create(label='test')
        idem = self.graph.vertex.get(v.uid)
        self.assertEqual(v, idem)

    # def test_get_or_create(self):
    #     v = self.graph.vertex.get_or_create(label='test')
    #     self.assertIsNotNone(v)

    # def test_get_or_create_two(self):
    #     v1 = self.graph.vertex.get_or_create(label='test')
    #     v2 = self.graph.vertex.get_or_create(label='test')
    #     self.assertEqual(v1, v2)

    def test_create_and_get_vertex(self):
        v1 = self.graph.vertex.create('test')
        v2 = self.graph.vertex.get(v1.uid)
        self.assertTrue(v1, v2)

    def test_create_with_properties_and_get_vertex(self):
        v = self.graph.vertex.create('test', key='value')
        v = self.graph.vertex.get(v.uid)

        self.assertEqual(v['key'], 'value')

    def test_create_modify_and_get_edge(self):
        start = self.graph.vertex.create('test')
        end = self.graph.vertex.create('test')
        edge = start.link('edge', end, hello='world')

        edge = self.graph.edge.get(edge.uid)

        self.assertTrue(edge['hello'] == 'world')
        self.assertEqual(edge.start(), start)
        self.assertEqual(edge.end(), end)

    def test_create_edge_and_check_vertices_edges(self):
        start = self.graph.vertex.create('start')
        end = self.graph.vertex.create('end')
        start.link('edge', end)

        # retrieve start and end
        start = self.graph.vertex.get(start.uid)
        end = self.graph.vertex.get(end.uid)

        self.assertTrue(start.outgoings())
        self.assertTrue(end.incomings())



# class BaseTestGremlin(object):

#     def test_graph_one(self):
#         self.graph.vertex(label='test', foo='bar')
#         self.graph.vertex(label='test', foo='baz')
#         self.graph.vertex(label='another', foo='foo')
#         obj = self.graph.one(label='test', foo='bar')
#         self.assertIsNotNone(obj)

#     def test_select(self):
#         seed = self.graph.vertex(label='seed')
#         seed.link(self.graph.vertex(label='one'), label='ok')
#         seed.link(self.graph.vertex(label='two'), label='ok')
#         seed.link(self.graph.vertex(label='one'), label='ko')
#         query = self.graph.query(outgoings, select(label='ok'))
#         count = len(list(query(seed)))
#         self.assertEqual(count, 2)

#     def test_empty_traversal(self):
#         count = len(list(self.graph.query()([])))
#         self.assertEqual(count, 0)

#     def test_skip(self):
#         seed = self.graph.vertex(label='seed')
#         seed.link(self.graph.vertex(label='one'))
#         seed.link(self.graph.vertex(label='two'))
#         seed.link(self.graph.vertex(label='one'))
#         query = self.graph.query(outgoings, skip(2))
#         count = len(list(query(seed)))
#         self.assertEqual(count, 1)

#     def test_limit(self):
#         seed = self.graph.vertex(label='seed')
#         seed.link(self.graph.vertex(label='one'))
#         seed.link(self.graph.vertex(label='two'))
#         seed.link(self.graph.vertex(label='one'))
#         query = self.graph.query(outgoings, limit(2))
#         count = len(list(query(seed)))
#         self.assertEqual(count, 2)

#     # def test_paginator(self):
#     #     seed = self.graph.vertex(label='seed')
#     #     list(map(lambda x: seed.link(self.graph.vertex()), range(20)))
#     #     self.assertEqual(seed.outgoings().paginator(5).count(), 5)
#     #     self.assertEqual(seed.outgoings().paginator(5).count(), 5)

#     def test_outgoings(self):
#         seed = self.graph.vertex()
#         other = self.graph.vertex()
#         seed.link(other)
#         other.link(self.graph.vertex())
#         other.link(self.graph.vertex())
#         query = self.graph.query(outgoings, end, outgoings, count)
#         self.assertEqual(query(seed), 2)

#     def test_incomings(self):
#         seed = self.graph.vertex()
#         other = self.graph.vertex()
#         link = seed.link(other)
#         query = self.graph.query(incomings, get)
#         self.assertEqual(query(other), [link])

#     def test_incomings_two(self):
#         seed = self.graph.vertex()
#         other = self.graph.vertex()
#         seed.link(other)
#         query = self.graph.query(incomings, start, get)
#         self.assertEqual(query(other), [seed])

#     def test_incomings_three(self):
#         seed = self.graph.vertex()
#         other = self.graph.vertex()
#         seed.link(other)
#         end = self.graph.vertex()
#         other.link(end)
#         query = self.graph.query(incomings, start, incomings, start, get)
#         self.assertEqual(query(end), [seed])

#     def test_order(self):
#         seed = self.graph.vertex()
#         seed.link(self.graph.vertex(value=5))
#         seed.link(self.graph.vertex(value=4))
#         seed.link(self.graph.vertex(value=1))

#         query = self.graph.query(outgoings, end, key('value'), value)
#         self.assertEqual(set(query(seed)), set([5, 4, 1]))

#         query = self.graph.query(outgoings, end, key('value'), sort(), value)
#         self.assertEqual(query(seed), [1, 4, 5])

#     def test_unique(self):
#         seed = self.graph.vertex()
#         seed.link(self.graph.vertex(value=1))
#         seed.link(self.graph.vertex(value=1))
#         seed.link(self.graph.vertex(value=1))
#         query = self.graph.query(outgoings, end, key('value'), unique, value)
#         self.assertEqual(query(seed), [1])
