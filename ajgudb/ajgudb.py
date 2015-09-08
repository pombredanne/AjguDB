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
from .storage import Storage


class Base(dict):

    def delete(self):
        self._graphdb._tuples.delete(self.uid)

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.uid == other.uid
        return False

    def __repr__(self):
        return '<%s %s>' % (type(self).__name__, self.uid)

    def __nonzero__(self):
        return True

    def __hash__(self):
        return self.uid


class Vertex(Base):

    def __init__(self, graphdb, uid, label, properties):
        self._graphdb = graphdb
        self.uid = uid
        self.label = label
        super(Vertex, self).__init__(properties)

    def incomings(self):
        edges = self._graphdb._storage.links.incomings(self.uid)
        # this method must be with care, since it fully consume
        # the generator to avoid cursor leak
        edges = map(self._graphdb.edge.get, edges)
        return edges

    def outgoings(self):
        edges = self._graphdb._storage.links.outgoings(self.uid)
        # this method must be with care, since it fully consume
        # the generator to avoid cursor leak
        edges = map(self._graphdb.edge.get, edges)
        return edges

    def save(self):
        raise NotImplementedError

    def delete(self):
        raise NotImplementedError

    def link(self, label, end, **properties):
        uid = self._graphdb._storage.edges.add(label, properties)
        self._graphdb._storage.links.add(uid, self.uid, end.uid)
        edge = Edge(self._graphdb, uid, self.uid, label, end.uid, properties)
        return edge


class Edge(Base):

    def __init__(self, graphdb, uid, start, label, end, properties):
        self._graphdb = graphdb
        self.uid = uid
        self._start = start
        self.__start = None
        self._end = end
        self.__end = None
        super(Edge, self).__init__(properties)

    def start(self):
        if self.__start:
            return self.__start
        self.__start = self._graphdb.vertex.get(self._start)
        return self.__start

    def end(self):
        if self.__end:
            return self.__end
        self.__end = self._graphdb.vertex.get(self._end)
        return self.__end

    def save(self):
        raise NotImplementedError

    def delete(self):
        raise NotImplementedError


class VertexManager(object):

    def __init__(self, graphdb):
        self._graphdb = graphdb

    def create(self, label, **properties):
        uid = self._graphdb._storage.vertices.add(label, properties)
        return Vertex(self._graphdb, uid, label, properties)

    def get(self, uid):
        properties = self._graphdb._storage.vertices.tuples(uid)
        label = self._graphdb._storage.vertices.label(uid)
        return Vertex(self._graphdb, uid, label, properties)

    def one(self, label, **properties):
        import gremlin
        query = gremlin.query(
            gremlin.vertices(label),
            gremlin.select(**properties),
            gremlin.get
        )
        try:
            element = query(self._graphdb)[0]
        except IndexError:
            return None
        else:
            return element

    def get_or_create(self, label, **properties):
        element = self.one(label, **properties)
        if element:
            return element
        else:
            return self.vertex(label, **properties)

    def query(self, *steps):
        import gremlin
        return gremlin.query(gremlin.vertices(), *steps)(self.graphdb)


class EdgeManager(object):

    def __init__(self, graphdb):
        self._graphdb = graphdb

    def get(self, uid):
        properties = self._graphdb._storage.edges.tuples(uid)
        label = self._graphdb._storage.edges.label(uid)
        start, end = self._graphdb._storage.links.get(uid)
        return Edge(self._graphdb, uid, start, label, end, properties)

    def one(self, label, **properties):
        import gremlin
        query = gremlin.query(
            gremlin.edges(label),
            gremlin.select(**properties),
            gremlin.get
        )
        try:
            element = query(self._graphdb)[0]
        except IndexError:
            return None
        else:
            return element

    def get_or_create(self, label, **properties):
        element = self.one(label, **properties)
        if element:
            return element
        else:
            return self.vertex(label, **properties)

    def query(self, *steps):
        import gremlin
        return gremlin.query(gremlin.edges(), *steps)(self.graphdb)


class AjguDB(object):

    def __init__(self, path):
        self._storage = Storage(path)
        self.vertex = VertexManager(self)
        self.edge = EdgeManager(self)

    def close(self):
        self._storage.close()
