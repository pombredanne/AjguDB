# AjuDB - leveldb powered graph database
# Copyright (C) 2015 Amirouche Boubekki <amirouche@hypermove.net>

# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.

# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301  USA
from collections import defaultdict
from wiredtiger import wiredtiger_open

from utils import pack
from utils import unpack



WT_NOT_FOUND = -31803


class CursorContextManager(object):

    def __init__(self, session, table, cursors):
        self._session = session
        self._table = table
        self._cursors = cursors
        self.cursor()

    def cursor(self):
        if self._cursors:
            cursor = self._cursors.pop()
        else:
            cursor = self._session.open_cursor(self._table)
        self._cursor = cursor

        # bind methods
        self.search = cursor.search
        self.search_near = cursor.search_near
        self.set_key = cursor.set_key
        self.set_value = cursor.set_value
        self.get_key = cursor.get_key
        self.get_value = cursor.get_value
        self.next = cursor.next
        self.insert = cursor.insert

    def reset(self):
        if self._cursor:
            self._cursor.reset()
            self._cursors.append(self._cursor)
            self._cursor = None
        # else already reset.

    def __enter__(self):
        return self._cursor

    def __exit__(self, exc, val, tb):
        self.reset()
        return False


class Table(object):

    def __init__(self, session, name, table_format, index_format):
        self._session = session
        self._table = 'table:' + name
        self._index = 'table:' + name + ' -index'
        self._cursors = list()
        self._indices = list()

        session.create(self._table, table_format)
        session.create(self._index, index_format)

    def cursor(self):
        return CursorContextManager(self._session, self._table, self._cursors)

    def index(self):
        return CursorContextManager(self._session, self._index, self._indices)


class Documents(object):
    """Stores documents with label as tuples with fine grained indices."""

    def __init__(self, session, name):
        self._session = session
        # labels
        self._labels = Table(
            session,
            'labels-' + name,
            'key_format=r,value_format=S',
            'key_format=SQ,value_format=S'
        )
        self._labels.append = session.open_cursor(
            'table:labels-' + name,
            None,
            'append'
        )
        # tuples
        self._tuples = Table(
            session,
            'tuples-' + name,
            'key_format=QS,value_format=u',
            'key_format=SuQ,value_format=S'
        )
        # FIXME: should also be stored in database
        self._indices = defaultdict(list)

    def index(self, label, key):
        self._indices[label].append(key)

    def label(self, uid):
        with self._labels.cursor() as cursor:
            cursor.set_key(uid)
            if cursor.search() != WT_NOT_FOUND:
                value = cursor.get_value()
                return value

    def identifiers(self, label):
        manager = self._labels.index()
        manager.set_key(label, 0)
        code = manager.search_near()
        if code == WT_NOT_FOUND:
            manager.reset()
            return list()

        elif code == -1:
            if manager.next() == WT_NOT_FOUND:
                return list()

        def iterator():
            while True:
                other = manager.get_key()
                ok = reduce(
                    lambda previous, x: (cmp(*x) == 0) and previous,
                    zip((label,), other),
                    True
                )
                if ok:
                    _, uid = other
                    yield uid
                    if manager.next() == WT_NOT_FOUND:
                        manager.reset()
                        break
                else:
                    manager.reset()
                    break
        return list(iterator())

    def add(self, label, properties=None):
        if not properties:
            properties = dict()
        # add label
        self._labels.append.set_value(label)
        self._labels.append.insert()
        uid = self._labels.append.get_key()
        # index label
        with self._labels.index() as cursor:
            cursor.set_key(label, uid)
            cursor.set_value('')
            cursor.insert()

        # add properties
        with self._tuples.cursor() as cursor:
            for key, value in properties.items():
                cursor.set_key(uid, key)
                cursor.set_value(pack(value))
                cursor.insert()
        # update index
        try:
            keys = self._indices[label]
        except KeyError:
            pass
        else:
            with self._tuples.index() as cursor:
                for key in keys:
                    try:
                        value = properties[key]
                    except KeyError:
                        pass
                    else:
                        cursor.set_key(key, pack(value), uid)
                        cursor.set_value('')
                        cursor.insert()
        return uid

    def tuple(self, uid, key):
        with self._tuples.cursor() as cursor:
            cursor.set_key(uid, key)
            if cursor.search() != WT_NOT_FOUND:
                value = cursor.get_value()
                value = unpack(value)
                return value

    def tuples(self, uid):
        def __get():
            with self._tuples.cursor() as cursor:
                cursor.set_key(uid, '')
                code = cursor.search_near()
                if code == WT_NOT_FOUND:
                    return
                if code == -1:
                    if cursor.next() == WT_NOT_FOUND:
                        return
                while True:
                    other, key = cursor.get_key()
                    value = cursor.get_value()
                    if other == uid:
                        value = unpack(value)
                        yield key, value
                        if cursor.next() == WT_NOT_FOUND:
                            break
                    else:
                        break
        tuples = dict(__get())
        return tuples

    def query(self, key, value=''):
        manager = self._tuples.index()
        match = (key, value) if value else (key,)
        manager.set_key(key, pack(value), 0)
        code = manager.search_near()

        if code == WT_NOT_FOUND:
            manager.reset()
            return list()

        if code == -1:
            if manager.next() == WT_NOT_FOUND:
                return list()

        def iterator():
            while True:
                key, value, uid = manager.get_key()
                value = unpack(value)
                other = (key, value)
                ok = reduce(
                    lambda previous, x: (cmp(*x) == 0) and previous,
                    zip(match, other),
                    True
                )
                if ok:
                    yield value, uid
                    if manager.next() == WT_NOT_FOUND:
                        manager.reset()
                        break
                else:
                    manager.reset()
                    break

        return list(iterator())


class EdgeLinks(object):
    """Store edges links in an efficient way"""

    def __init__(self, session):
        self._session = session
        # labels
        session.create(
            'table:edge-links',
            ''.join(
                'key_format=Q,value_format=QQ,'
                'columns=(id,start,end)',
            )
        )
        session.create(
            'index:edge-links:outgoings',
            'columns=(start,id)'
        )
        session.create(
            'index:edge-links:incomings',
            'columns=(end,id)'
        )

        self._outgoings = list()
        self._incomings = list()
        self._links = list()

    def add(self, uid, start, end):
        manager = CursorContextManager(
            self._session,
            'table:edge-links',
            self._links
        )
        manager.set_key(uid)
        manager.set_value(start, end)
        manager.insert()
        manager.reset()

    def get(self, uid):
        manager = CursorContextManager(
            self._session,
            'table:edge-links',
            self._links
        )
        manager.set_key(uid)
        if manager.search() == WT_NOT_FOUND:
            return None, None
        else:
            return manager.get_value()
        manager.insert()
        manager.reset()

    def all(self):
        manager = CursorContextManager(
            self._session,
            'table:edge-links:outgoings',
            self._links
        )
        manager._cursor.reset()
        code = manager.next()

        if code == WT_NOT_FOUND:
            manager.reset()
            return list()

        if code == -1:
            if manager.next() == WT_NOT_FOUND:
                return list()

        def iterator():
            while True:
                uid = manager.get_key()
                start, end = manager.get_value()
                yield uid, start, end
                if manager.next() == WT_NOT_FOUND:
                    manager.reset()
                    break

        return list(iterator())

    def outgoings(self, start):
        manager = CursorContextManager(
            self._session,
            'index:edge-links:outgoings',
            self._outgoings
        )
        manager.set_key(start, 0)
        code = manager.search_near()

        if code == WT_NOT_FOUND:
            manager.reset()
            return list()

        if code == -1:
            if manager.next() == WT_NOT_FOUND:
                return list()

        def iterator():
            while True:
                other, uid = manager.get_key()
                if other == start:
                    yield uid
                    if manager.next() == WT_NOT_FOUND:
                        manager.reset()
                        break
                else:
                    manager.reset()
                    break
        return list(iterator())

    def incomings(self, end):
        manager = CursorContextManager(
            self._session,
            'index:edge-links:incomings',
            self._incomings
        )
        manager.set_key(end, 0)
        code = manager.search_near()

        if code == WT_NOT_FOUND:
            manager.reset()
            return

        if code == -1:
            if manager.next() == WT_NOT_FOUND:
                return list()

        def iterator():
            while True:
                other, uid = manager.get_key()
                if other == end:
                    yield uid
                    if manager.next() == WT_NOT_FOUND:
                        manager.reset()
                        break
                else:
                    manager.reset()
                    break
        return list(iterator())


class Storage(object):
    """Generic database"""

    def __init__(self, path):
        self._wiredtiger = wiredtiger_open(path, 'create')
        self._session = self._wiredtiger.open_session()
        self.edges = Documents(self._session, 'edges')
        self.vertices = Documents(self._session, 'vertices')
        self.links = EdgeLinks(self._session)

    def close(self):
        self._wiredtiger.close()
