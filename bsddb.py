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
import struct

from msgpack import dumps
from msgpack import loads

from bsddb3.db import DB
from bsddb3.db import DBEnv
from bsddb3.db import DB_BTREE
from bsddb3.db import DB_CREATE
from bsddb3.db import DB_INIT_MPOOL
from bsddb3.db import DB_LOG_AUTO_REMOVE


class AjguDBException(Exception):
    pass


def pack(*values):
    def __pack(value):
        if type(value) is int:
            return '1' + struct.pack('>q', value)
        elif type(value) is unicode:
            return '2' + value.encode('utf-8') + '\0'
        elif type(value) is str:
            return '3' + value + '\0'
        else:
            data = dumps(value, encoding='utf-8')
            return '4' + struct.pack('>q', len(data)) + data
    return ''.join(map(__pack, values))


def unpack(packed):
    kind = packed[0]
    if kind == '1':
        value = struct.unpack('>q', packed[1:9])[0]
        packed = packed[9:]
    elif kind == '2':
        index = packed.index('\0')
        value = packed[1:index].decode('utf-8')
        packed = packed[index+1:]
    elif kind == '3':
        index = packed.index('\0')
        value = packed[1:index]
        packed = packed[index+1:]
    else:
        size = struct.unpack('>q', packed[1:9])[0]
        value = loads(packed[9:9+size], encoding='utf-8')
        packed = packed[size+9:]
    if packed:
        values = unpack(packed)
        values.insert(0, value)
    else:
        values = [value]
    return values


class TupleSpace(object):
    """Generic database"""

    def __init__(self, path):
        self.env = DBEnv()
        self.env.set_cache_max(10, 0)
        self.env.set_cachesize(5, 0)
        flags = (
            DB_CREATE
            | DB_INIT_MPOOL
        )
        self.env.log_set_config(DB_LOG_AUTO_REMOVE, True)
        self.env.set_lg_max(1024**2)
        self.env.open(
            path,
            flags,
            0
        )

        # create vertices and edges k/v stores
        def new_store(name):
            flags = DB_CREATE
            elements = DB(self.env)
            elements.open(
                name,
                None,
                DB_BTREE,
                flags,
                0,
            )
            return elements

        self.index = new_store('index')
        self.tuples = new_store('tuples')

    def close(self):
        self.tuples.close()
        self.index.close()
        self.env.close()

    def get(self, uid):
        cursor = self.tuples.cursor()

        def __get():
            record = cursor.set_range(pack(uid, ''))
            if not record:
                return
            key, value = record
            while True:
                other, key = unpack(key)
                if other == uid:
                    value = unpack(value)[0]
                    yield key, value
                    record = cursor.next()
                    if record:
                        key, value = record
                        continue
                    else:
                        break
                else:
                    break

        tuples = dict(__get())
        cursor.close()
        return tuples

    def add(self, uid, **properties):
        for key, value in properties.items():
            self.tuples.put(pack(uid, key), pack(value))
            self.index.put(pack(key, value, uid), '')

    def delete(self, uid):
        # delete item from main table and index
        cursor = self.tuples.cursor()
        index = self.index.cursor()
        record = cursor.set_range(pack(uid, ''))
        if record:
            key, value = record
        else:
            cursor.close()
            raise AjguDBException('not found')
        while True:
            other, key = unpack(key)
            if other == uid:
                # remove tuple from main index
                cursor.delete()

                # remove it from index
                value = unpack(value)[0]
                index.set(pack(key, value, uid))
                index.delete()

                # continue
                record = cursor.next()
                if record:
                    key, value = record
                    continue
                else:
                    break
            else:
                break
        cursor.close()

    def update(self, uid, **properties):
        self.delete(uid)
        self.add(uid, **properties)

    def debug(self):
        for key, value in self.tuples.items():
            uid, key = unpack(key)
            value = unpack(value)[0]
            print(uid, key, value)

    def query(self, key, value=''):
        cursor = self.index.cursor()
        match = (key, value) if value else (key,)

        record = cursor.set_range(pack(key, value))
        if not record:
            cursor.close()
            return

        while True:
            key, _ = record
            other = unpack(key)
            ok = reduce(
                lambda previous, x: (cmp(*x) == 0) and previous,
                zip(match, other),
                True
            )
            if ok:
                yield other
                record = cursor.next()
                if not record:
                    break
            else:
                break
        cursor.close()


class Base(dict):

    def delete(self):
        self._graphdb._tuples.delete(self.uid)

    def __eq__(self, other):
        if isinstance(other, Base):
            return self.uid == other.uid
        return False

    def __repr__(self):
        return '<%s %s>' % (type(self).__name__, self.uid)

    def __nonzero__(self):
        return True


class Vertex(Base):

    def __init__(self, graphdb, uid, properties):
        self._graphdb = graphdb
        self.uid = uid
        super(Vertex, self).__init__(properties)

    def _iter_edges(self, _vertex, proc=None, **properties):
        def edges():
            key = '_meta_%s' % _vertex
            records = self._graphdb._tuples.query(key, self.uid)
            for key, name, uid in records:
                properties = self._graphdb._tuples.get(uid)
                properties.pop('_meta_type')
                edge = Edge(self._graphdb, uid, properties)
                yield edge

        if proc or properties:
            return GremlinIterator(edges()).filter(proc, **properties)
        else:
            return GremlinIterator(edges())

    def incomings(self, proc=None, **properties):
        return self._iter_edges('end', proc, **properties)

    def outgoings(self, proc=None, **properties):
        return self._iter_edges('start', proc, **properties)

    def save(self):
        self._graphdb._tuples.update(
            self.uid,
            _meta_type='vertex',
            **self
        )
        return self

    def link(self, end, **properties):
        properties['_meta_start'] = self.uid
        properties['_meta_end'] = end.uid
        uid = self._graphdb._uid()
        self._graphdb._tuples.add(uid, _meta_type='edge', **properties)
        return Edge(self._graphdb, uid, properties)


class Edge(Base):

    def __init__(self, graphdb, uid, properties):
        self._graphdb = graphdb
        self.uid = uid
        self._start = properties.pop('_meta_start')
        self._end = properties.pop('_meta_end')
        super(Edge, self).__init__(properties)

    def start(self):
        properties = self._graphdb._tuples.get(self._start)
        return Vertex(self._graphdb, self._start, properties)

    def end(self):
        properties = self._graphdb._tuples.get(self._end)
        return Vertex(self._graphdb, self._end, properties)

    def save(self):
        self._graphdb._tuples.update(
            self.uid,
            _meta_type='edge',
            _meta_start=self._start,
            _meta_end=self._end,
            **self
        )
        return self

    def delete(self):
        self._graphdb._tuples.delete(self.uid)


class GremlinIterator(object):

    sentinel = object()

    def __init__(self, iterator):
        self.iterator = iterator

    def __iter__(self):
        return self.iterator

    def all(self):
        return list(self)

    def one(self, default=sentinel):
        try:
            return next(self.iterator)
        except StopIteration:
            if default is self.sentinel:
                msg = 'not found.'
                raise AjguDBException(msg)
            else:
                return default

    def skip(self, count):
        def iterator():
            counter = 0
            for item in self:
                counter += 1
                if counter > count:
                    yield item
        return type(self)(iterator())

    def limit(self, count):
        def iterator():
            counter = 0
            for item in self:
                counter += 1
                yield item
                if counter == count:
                    break
        return type(self)(iterator())

    def paginator(self, count):
        def iterator():
            counter = 0
            page = list()
            for item in self:
                page.append(item)
                counter += 1
                if counter == count:
                    yield page
                    counter = 0
                    page = list()
            yield page
        return type(self)(iterator())

    def count(self):
        return reduce(lambda x, y: x+1, self, 0)

    def incomings(self, proc=None, **filters):
        def iterator():
            for item in self:
                for edge in item.incomings():
                    yield edge
        out = type(self)(iterator())
        if proc or filters:
            return out.filter(proc, **filters)
        else:
            return out

    def outgoings(self, proc=None, **filters):
        def iterator():
            for item in self:
                for edge in item.outgoings():
                    yield edge
        out = type(self)(iterator())
        if filters or proc:
            return out.filter(proc, **filters)
        else:
            return out

    def start(self):
        return self.map(lambda x: x.start())

    def end(self):
        return self.map(lambda x: x.end())

    def map(self, proc):
        def iterator():
            for item in self:
                yield proc(item)
        return type(self)(iterator())

    def dict(self, **kwargs):
        if kwargs:
            def get_dict(item):
                properties = dict()
                for key in kwargs.keys():
                    properties[key] = item.get(key)
                return properties
            return self(self).map(get_dict)
        else:
            return self(self).map(lambda x: dict(x))

    def uid(self):
        return self.map(lambda x: x.uid)

    def order(self, key=lambda x: x, reverse=False):
        out = sorted(
            self,
            key=key,
            reverse=reverse
        )
        return type(self)(iter(out))

    def property(self, name):
        return self.map(lambda x: x.get(name))

    def unique(self):
        # from ActiveState (MIT)
        #
        #   Lazy Ordered Unique elements from an iterator
        #
        def __unique(iterable, key=None):
            seen = set()

            if key is None:
                # Optimize the common case
                for item in iterable:
                    if item in seen:
                        continue
                    seen.add(item)
                    yield item

            else:
                for item in iterable:
                    keyitem = key(item)
                    if keyitem in seen:
                        continue
                    seen.add(keyitem)
                    yield item

        iterator = __unique(self)
        return type(self)(iterator)

    def filter(self, func=None, **properties):
        if func:
            def iterator():
                for item in self:
                    if func(item):
                        yield item
            if properties:
                return type(self)(iterator()).filter(**properties)
            else:
                return type(self)(iterator())
        else:
            def iterator():
                filters = set(properties.items())
                for item in self:
                    if filters.issubset(item.items()):
                        yield item
            return type(self)(iterator())


class AjguDB(object):

    def __init__(self, path):
        self._tuples = TupleSpace(path)

    def close(self):
        self._tuples.close()

    def _uid(self):
        try:
            counter = self._tuples.get(0)['counter']
        except KeyError:
            self._tuples.add(0, counter=1)
            return 1
        else:
            counter += 1
            self._tuples.update(0, counter=counter)
            return counter

    def get(self, uid):
        properties = self._tuples.get(uid)
        if properties:
            meta_type = properties.pop('_meta_type')
            if meta_type == 'vertex':
                return Vertex(self, uid, properties)
            else:
                return Edge(self, uid, properties)
        else:
            raise AjguDBException('not found')

    def vertex(self, **properties):
        uid = self._uid()
        self._tuples.add(uid, _meta_type='vertex', **properties)
        return Vertex(self, uid, properties)

    def get_or_create(self, **properties):
        vertex = self.filter(**properties).one(None)
        if vertex:
            return vertex
        else:
            return self.vertex(**properties)

    def filter(self, **properties):
        def iterator():
            key, value = properties.items()[0]
            for _, _, uid in self._tuples.query(key, value):
                yield self.get(uid)

        return GremlinIterator(iterator()).filter(**properties)

    def vertices(self):
        return self.filter(_meta_type='vertex')

    def edges(self):
        return self.filter(_meta_type='edge')
