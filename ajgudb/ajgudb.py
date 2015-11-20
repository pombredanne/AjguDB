# AjuDB - wiredtiger powered graph database
# Copyright (C) 2015 Amirouche Boubekki <amirouche@hypermove.net>
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

    __slots__ = ('_graphdb', 'uid', 'label')

    def __init__(self, graphdb, uid, label, properties):
        self._graphdb = graphdb
        self.uid = uid
        self.label = label
        super(Vertex, self).__init__(properties)

    def incomings(self):
        edges = self._graphdb._storage.edges.incomings(self.uid)
        # XXX: this method must be with care, since it fully consume
        # the generator (to avoid cursor leak).
        return map(self._graphdb.edge.get, edges)

    def outgoings(self):
        edges = self._graphdb._storage.edges.outgoings(self.uid)
        # this method must be with care, since it fully consume
        # the generator (to avoid cursor leak).
        return map(self._graphdb.edge.get, edges)

    def link(self, label, end, **properties):
        uid = self._graphdb._storage.edges.add(
            self.uid,
            label,
            end.uid,
            properties
        )
        return Edge(self._graphdb, uid, self.uid, label, end.uid, properties)

    def save(self):
        self._graphdb._storage.vertices.update(self.uid, self)

    def delete(self):
        map(lambda x: x.delete(), self.incomings())
        map(lambda x: x.delete(), self.outgoings())
        self._graphdb._storage.vertices.delete(self.uid)


class Edge(Base):

    __slots__ = ('_graphdb', 'uid', '_start', 'label', '_end')

    def __init__(self, graphdb, uid, start, label, end, properties):
        self._graphdb = graphdb
        self.uid = uid
        self._start = start
        self._end = end
        super(Edge, self).__init__(properties)

    def start(self):
        return self._graphdb.vertex.get(self._start)

    def end(self):
        return self._graphdb.vertex.get(self._end)

    def save(self):
        self._graphdb._storage.edges.update(self.uid, self)

    def delete(self):
        self._graphdb._storage.edges.delete(self.uid)


class VertexManager(object):

    def __init__(self, graphdb):
        self._graphdb = graphdb

    def index(self, name):
        self._graphdb._storage.vertices._indices.append(name)

    def create(self, label, **properties):
        uid = self._graphdb._storage.vertices.add(label, properties)
        return Vertex(self._graphdb, uid, label, properties)

    def get(self, uid):
        label, properties = self._graphdb._storage.vertices.get(uid)
        return Vertex(self._graphdb, uid, label, properties)

    def one(self, label, **properties):
        import gremlin
        query = gremlin.query(
            gremlin.vertices(label),
            gremlin.where(**properties),
            gremlin.limit(1),
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
            return self._graphdb.vertex.create(label, **properties)

    def query(self, *steps):
        import gremlin
        return gremlin.query(gremlin.vertices(), *steps)(self.graphdb)


class EdgeManager(object):

    def __init__(self, graphdb):
        self._graphdb = graphdb

    def index(self, name):
        self._graphdb._storage.edges._indices.append(name)

    def get(self, uid):
        start, label, end, properties = self._graphdb._storage.edges.get(uid)
        return Edge(self._graphdb, uid, start, label, end, properties)

    def one(self, label, **properties):
        import gremlin
        query = gremlin.query(
            gremlin.edges(label),
            gremlin.where(**properties),
            gremlin.limit(1),
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

        self.get = self._storage.collection.get
        self.set = self._storage.collection.set
        self.remove = self._storage.collection.remove

    def close(self):
        self._storage.close()
