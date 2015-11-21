# AjuDB - wiredtiger powered graph database
# Copyright (C) 2015 Amirouche Boubekki <amirouche@hypermove.net>
from collections import Counter

from wiredtiger import wiredtiger_open

from utils import pack
from utils import unpack
from utils import AjguDBException


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

        # store indexed property keys
        self._indices = list()
        session.create(
            'table:vertices-keys',
            'key_format=SSQ,value_format=S'
        )
        self._keys = session.open_cursor('table:vertices-keys')

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

    def keys(self, key, value):
        # lookup the label
        self._keys.set_key(key, value, 0)
        code = self._keys.search_near()
        if code == WT_NOT_FOUND:
            self._keys.reset()
            return list()
        elif code == -1:
            if self._keys.next() == WT_NOT_FOUND:
                return list()

        # iterate over the results and return it as a list
        def iterator():
            while True:
                other_key, other_value, uid = self._keys.get_key()
                if other_key == key and other_value == value:
                    yield uid
                    if self._keys.next() == WT_NOT_FOUND:
                        self._keys.reset()
                        break
                else:
                    self._keys.reset()
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

        # index properties if any
        for key in properties.keys():
            if key in self._indices:
                self._keys.set_key(key, properties[key], uid)
                self._keys.set_value('')
                self._keys.insert()

        return uid

    def update(self, uid, properties):
        # lookup edge
        self._cursor.set_key(uid)
        if self._cursor.search() == WT_NOT_FOUND:
            raise KeyError('Vertex not found, identifier: %s' % uid)
        else:
            # update properties
            label, data = self._cursor.get_value()
            other = unpack(data)

            # remove old indices if any
            for key in other.keys():
                if key in self._indices:
                    self._keys.set_key(key, other[key], uid)
                    self._cursor.remove()

            # update
            self._cursor.set_value(label, pack(properties))

            # index properties
            for key in properties.keys():
                if key in self._indices:
                    self._keys.set_key(key, properties[key], uid)
                    self._keys.set_value('')
                    self._keys.insert()

    def delete(self, uid):
        """Remove the vertice with the given identifier `uid`"""
        # lookup the uid
        self._cursor.set_key(uid)
        if self._cursor.search() == WT_NOT_FOUND:
            raise KeyError('Vertex not found, identifier: %s' % uid)
        else:
            # remove primary row
            _, data = self._cursor.get_value()
            properties = unpack(data)
            self._cursor.remove()

            # remove indices if any
            for key in properties.keys():
                if key in self._indices:
                    self._keys.set_key(key, properties[key], uid)
                    self._cursor.remove()

    def get(self, uid):
        """Look for a vertice with the given identifier `uid`"""
        # lookup the uid
        self._cursor.set_key(uid)
        if self._cursor.search() == WT_NOT_FOUND:
            raise KeyError('Vertex not found, identifier: %s' % uid)
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

        # store indexed property keys
        self._indices = list()
        session.create(
            'table:edges-keys',
            'key_format=SSQ,value_format=S'
        )
        self._keys = session.open_cursor('table:edges-keys')

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

    def keys(self, key, value):
        # lookup the label
        self._keys.set_key(key, value, 0)
        code = self._keys.search_near()
        if code == WT_NOT_FOUND:
            self._keys.reset()
            return list()
        elif code == -1:
            if self._keys.next() == WT_NOT_FOUND:
                return list()

        # iterate over the results and return it as a list
        def iterator():
            while True:
                other_key, other_value, uid = self._keys.get_key()
                if other_key == key and other_value == value:
                    yield uid
                    if self._keys.next() == WT_NOT_FOUND:
                        self._keys.reset()
                        break
                else:
                    self._keys.reset()
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

    def add(self, start, label, end, properties=None):
        """Add edge with and  return its unique identifier

        `start`, and `end` must be respectively vertices identifiers.
        `label` must be a string and `properties` a dictionary"""
        # append edge and return identifier
        properties = properties if properties else dict()
        self._append.set_value(start, label, end, pack(properties))
        self._append.insert()
        uid = self._append.get_key()

        # index properties if any
        for key in properties.keys():
            if key in self._indices:
                self._keys.set_key(key, properties[key], uid)
                self._keys.set_value('')
                self._keys.insert()

        return uid

    def delete(self, uid):
        """Remove the edge with the given identifier `uid`"""
        # lookup the uid
        self._cursor.set_key(uid)
        if self._cursor.search() == WT_NOT_FOUND:
            raise KeyError('Edge not found, identifier: %s' % uid)
        else:
            # remove primary row
            _, _, _, data = self._cursor.get_value()
            properties = unpack(data)
            self._cursor.remove()

            # remove indices if any
            for key in properties.keys():
                if key in self._indices:
                    self._keys.set_key(key, properties[key], uid)
                    self._cursor.remove()

    def update(self, uid, properties):
        # lookup edge
        self._cursor.set_key(uid)
        if self._cursor.search() == WT_NOT_FOUND:
            raise KeyError('Edge not found, identifier: %s' % uid)
        else:
            # update properties
            start, label, end, data = self._cursor.get_value()
            other = unpack(data)

            # remove old indices if any
            for key in other.keys():
                if key in self._indices:
                    self._keys.set_key(key, other[key], uid)
                    self._cursor.remove()

            # update
            data = pack(properties)
            self._cursor.set_value(start, label, end, data)

            # index properties
            for key in properties.keys():
                if key in self._indices:
                    self._keys.set_key(key, properties[key], uid)
                    self._keys.set_value('')
                    self._keys.insert()

    def get(self, uid):
        """Retrieve the edge with the given `uid`"""
        # lookup `uid`
        self._cursor.set_key(uid)
        if self._cursor.search() == WT_NOT_FOUND:
            raise KeyError('Edge not found, identifier: %s' % uid)
        else:
            # return edge
            start, label, end, data = self._cursor.get_value()
            return start, label, end, unpack(data)


class Collection(object):

    def __init__(self, session):
        self._session = session

        # storage table
        session.create(
            'table:collection',
            'key_format=S,value_format=S'
        )
        self._cursor = session.open_cursor('table:collection')

    def set(self, key, value):
        self._cursor.set_key(key)
        self._cursor.set_value(pack(value))
        self._cursor.insert()

    def get(self, key):
        self._cursor.set_key(key)
        if self._cursor.search() == WT_NOT_FOUND:
            raise KeyError(key)
        return unpack(self._cursor.get_value())

    def remove(self, key):
        self._cursor.set_key(key)
        if self._cursor.search() == WT_NOT_FOUND:
            raise KeyError(key)
        self._cursor.remove()
        return True


class Trigrams(object):
    """Trigram character table

    Fuzzy search is implemented using this table definition.
    it provides three methods.

    .. warning:: Elements must be explicitly indexed and deleted.
    """

    def __init__(self, name, session):
        self._session = session
        # storage table
        session.create(
            'table:trigrams-' + name,
            'key_format=S,value_format=S'
        )
        self._cursor = session.open_cursor('table:collection')

    def trigrams(self, word):
        WORD_LENGTH = len(word)

        def iter():
            for i in range(0, WORD_LENGTH, 3):
                if WORD_LENGTH - i < 3:
                    break
                yield word[i:i+3]

        trigrams = iter()
        if WORD_LENGTH % 3 != 0:
            trigrams.append(word[-3:])

        return trigrams

    def index(self, word, uid):
        trigrams = self.trigrams(word)
        for trigram in trigrams:
            self._cursor.set_key(trigram)
            if self._cursor.search():
                value = self.cursor.get_value()
                value = unpack(value)
                value.append(uid)
                self._cursor.set_value(pack(value))
                self._cursor.update()
            else:
                self._cursor.set_value(pack([uid]))
                self._cursor.insert()

    def delete(self, word, uid):
        trigrams = self.trigrams(word)
        for trigram in trigrams:
            self._cursor.set_key(trigram)
            if self._cursor.search():
                value = self._cursor.get_value()
                value = unpack(value)
                value.remove(uid)
                self._cursor.set_value(pack(value))
                self._cursor.update()
            else:
                msg = 'uid %s not found for trigram character %s'
                msg = msg % (uid, trigram)
                raise AjguDBException(msg)

    def search(self, word, limit=10):
        trigrams = self.trigrams(word)
        out = list()
        for trigram in trigrams:
            self._cursor.set_key(trigram)
            if self._cursor.search():
                value = self._cursor.get_value()
                value = unpack(value)
                out.extend(value)
            else:
                continue
        return Counter(out).most_commom(limit)


class Storage(object):
    # FIXME: move this to AjguDB class

    def __init__(self, path):
        self._wiredtiger = wiredtiger_open(path, 'create,cache_size=10GB')
        self._session = self._wiredtiger.open_session()
        self.edges = Edges(self._session)
        self.vertices = Vertices(self._session)
        self.collection = Collection(self._session)
        # FIXME:
        # self.trigrams_vertices = Trigrams('vertices', self._session)
        # self.trigrams_edges = Trigrams('edges', self._session)

    def close(self):
        self._wiredtiger.close()
