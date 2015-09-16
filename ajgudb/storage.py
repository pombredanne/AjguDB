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
from wiredtiger import wiredtiger_open

from utils import pack
from utils import unpack


WT_NOT_FOUND = -31803


class Vertices(object):

    def __init__(self, session):
        self._session = session

        # storage table
        session.create(
            'table:vertices',
            'key_format=r,value_format=SS,columns=(id,label,data)'
        )
        # cursor for identifier->vertex query
        self._cursor = session.open_cursor('table:vertices')
        # append cursor
        self._append = session.open_cursor(
            'table:vertices',
            None,
            'append'
        )
        # index for label->vertex query
        session.create(
            'index:vertices:labels',
            'columns=(label,id)'
        )
        self._index = session.open_cursor('index:vertices:labels')

    def identifiers(self, label):
        """Look vertices with the given `label`"""
        self._index.set_key(label, 0)

        # lookup the label
        code = self._index.search_near()
        if code == WT_NOT_FOUND:
            self._index.reset()
            return list()
        elif code == -1:
            if self._index.next() == WT_NOT_FOUND:
                return list()

        # iterate over the results and return it as a list
        def iterator():
            while True:
                other, uid = self._index.get_key()
                if other == label:
                    yield uid
                    if self._index.next() == WT_NOT_FOUND:
                        self._index.reset()
                        break
                else:
                    self._index.reset()
                    break

        return list(iterator())

    def add(self, label, properties=None):
        """Add vertex with the given `label` and `properties`

        `properties` are packed using msgpack"""
        properties = properties if properties else dict()

        # append vertex to the list of vertices
        # and retrieve its assigned identifier
        self._append.set_value(label, pack(properties))
        self._append.insert()
        uid = self._append.get_key()
        return uid

    def get(self, uid):
        """Look for a vertices with the given identifier `uid`"""
        # lookup the uid
        self._cursor.set_key(uid)
        if self._cursor.search() == WT_NOT_FOUND:
            return None, None
        else:
            # uid found, return label and properties
            label, data = self._cursor.get_value()
            return label, unpack(data)


class Edges(object):

    def __init__(self, session):
        self._session = session

        # storage table
        session.create(
            'table:edges',
            'key_format=r,value_format=QSQS,columns=(id,start,label,end,data)'
        )
        # cursor for identifier->edge query
        self._cursor = session.open_cursor('table:edges')
        # append cursor
        self._append = session.open_cursor(
            'table:edges',
            None,
            'append'
        )
        # cursor for label->edge query
        session.create(
            'index:edges:labels',
            'columns=(label,id)'
        )
        self._index = session.open_cursor('index:edges:labels')

        # cursor for `outgoing vertex`->edge
        session.create(
            'index:edges:outgoings',
            'columns=(start,id)'
        )
        self._outgoings = session.open_cursor('index:edges:outgoings')

        # cursor for `incomings vertex`->edge
        session.create(
            'index:edges:incomings',
            'columns=(end,id)'
        )
        self._incomings = session.open_cursor('index:edges:incomings')

    def identifiers(self, label):
        """Look for edges which have the given `label`"""
        # lookup label
        # XXX: same code as `Vertices`
        self._index.set_key(label)
        code = self._index.search_near()
        if code == WT_NOT_FOUND:
            self._index.reset()
            return list()
        elif code == -1:
            if self._index.next() == WT_NOT_FOUND:
                return list()

        # iterate over the results and return it as a list
        def iterator():
            while True:
                other, uid = self._index.get_key()
                if other == label:
                    yield uid
                    if self._index.next() == WT_NOT_FOUND:
                        self._index.reset()
                        break
                else:
                    self._index.reset()
                    break

        return list(iterator())

    def incomings(self, end):
        """Look for edges which end at the vertex with the given `uid`"""
        # lookup vertex
        # XXX: same code as `Vertices`
        self._incomings.set_key(end, 0)
        code = self._incomings.search_near()
        if code == WT_NOT_FOUND:
            self._incomings.reset()
            return list()
        elif code == -1:
            if self._incomings.next() == WT_NOT_FOUND:
                return list()

        # iterate over the results and return it as a list
        def iterator():
            while True:
                other, uid = self._incomings.get_key()
                if other == end:
                    yield uid
                    if self._incomings.next() == WT_NOT_FOUND:
                        self._incomings.reset()
                        break
                else:
                    self._incomings.reset()
                    break

        return list(iterator())

    def outgoings(self, start):
        """Look for edges which start at the vertex with the given `uid`"""
        # lookup vertex
        # XXX: same code as `Vertices`
        self._outgoings.set_key(start, 0)
        code = self._outgoings.search_near()
        if code == WT_NOT_FOUND:
            self._outgoings.reset()
            return list()
        elif code == -1:
            if self._outgoings.next() == WT_NOT_FOUND:
                return list()

        # iterate over the results and return it as a list
        def iterator():
            while True:
                other, uid = self._outgoings.get_key()
                if other == start:
                    yield uid
                    if self._outgoings.next() == WT_NOT_FOUND:
                        self._outgoings.reset()
                        break
                else:
                    self._outgoings.reset()
                    break
        return list(iterator())

        return list(iterator())

    def add(self, start, label, end, properties=None):
        """Add edge with and  return its unique identifier

        `start`, and `end` must be respectively vertices identifiers.
        `label` must be a string and `properties` a dictionary"""
        # append edge and return identifier
        properties = properties if properties else dict()
        self._append.set_value(start, label, end, pack(properties))
        self._append.insert()
        uid = self._append.get_key()
        return uid

    def get(self, uid):
        """Retrieve the edge with the given `uid`"""
        # lookup `uid`
        self._cursor.set_key(uid)
        if self._cursor.search() == WT_NOT_FOUND:
            return None, None, None, None
        else:
            # return edge
            start, label, end, data = self._cursor.get_value()
            return start, label, end, unpack(data)


class Storage(object):
    """Generic database"""

    def __init__(self, path):
        self._wiredtiger = wiredtiger_open(path, 'create,cache_size=10GB')
        self._session = self._wiredtiger.open_session()
        self.edges = Edges(self._session)
        self.vertices = Vertices(self._session)

    def close(self):
        self._wiredtiger.close()
