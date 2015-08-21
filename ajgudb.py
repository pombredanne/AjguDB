import struct
from contextlib import contextmanager

from msgpack import dumps
from msgpack import loads

from bsddb3.db import DB
from bsddb3.db import DBEnv
from bsddb3.db import DB_BTREE
from bsddb3.db import DB_CREATE
from bsddb3.db import DB_INIT_LOG
from bsddb3.db import DB_INIT_TXN
from bsddb3.db import DB_INIT_LOCK
from bsddb3.db import DB_INIT_MPOOL
from bsddb3.db import DB_LOG_AUTO_REMOVE


class AjguDBException(Exception):
    pass


def pack(*values):
    def __pack(value):
        if type(value) is int:
            return '1' + struct.pack('>q', value)
        elif type(value) is str:
            return '2' + struct.pack('>q', len(value)) + value
        else:
            data = dumps(value, encoding='utf-8')
            return '3' + struct.pack('>q', len(data)) + data
    return ''.join(map(__pack, values))


def unpack(packed):
    kind = packed[0]
    if kind == '1':
        value = struct.unpack('>q', packed[1:9])[0]
        packed = packed[9:]
    elif kind == '2':
        size = struct.unpack('>q', packed[1:9])[0]
        value = packed[9:9+size]
        packed = packed[size+9:]
    else:
        size = struct.unpack('>q', packed[1:9])[0]
        value = loads(packed[9:9+size])
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
            | DB_INIT_LOG
            | DB_INIT_TXN
            | DB_INIT_MPOOL
            | DB_INIT_LOCK
        )
        self.env.log_set_config(DB_LOG_AUTO_REMOVE, True)
        self.env.set_lg_max(1024 ** 3)
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
                txn=txn
            )
            return elements
        txn = self.env.txn_begin()
        self.tuples = new_store('tuples')
        self.index = new_store('index')
        txn.commit()
        self.txn = None

    @contextmanager
    def transaction(self):
        self.txn = self.env.txn_begin()
        try:
            yield
        except Exception as exc:
            self.txn.abort()
            raise exc
        else:
            self.txn.commit()

    def get(self, uid):
        cursor = self.tuples.cursor(txn=self.txn)

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
            self.tuples.put(pack(uid, key), pack(value), txn=self.txn)
            self.index.put(pack(key, value, uid), '', txn=self.txn)

    def delete(self, uid):
        # delete item from main table and index
        cursor = self.tuples.cursor(txn=self.txn)
        index = self.index.cursor(txn=self.txn)
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

    def close(self):
        self.index.close()
        self.tuples.close()
        self.env.close()

    def debug(self):
        for key, value in self.tuples.items(self.txn):
            uid, key = unpack(key)
            value = unpack(value)[0]
            print(uid, key, value)

    def query(self, key, value=''):
        cursor = self.index.cursor(txn=self.txn)
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


class Vertex(object):

    def __init__(self, graphdb, uid, properties):
        self._graphdb = graphdb
        self.uid = uid
        self.properties = properties

    def __eq__(self, other):
        return self.uid == other.uid

    def _iter_edges(self, kind):
        records = self._graphdb._tuples.query('_meta_%s' % kind, self.uid)
        for key, name, uid in records:
            properties = self._graphdb._tuples.get(uid)
            yield Edge(self._graphdb, uid, properties)

    def incomings(self, label=None):
        return self._iter_edges('end')

    def outgoings(self, label=None):
        return self._iter_edges('start')

    def save(self):
        self._graphdb._tuples.update(
            self.uid,
            _meta_type='vertex',
            **self.properties
        )
        return self

    def delete(self):
        self._graphdb._tuples.delete(self.uid)


class Edge(object):

    def __init__(self, graphdb, uid, properties):
        self._graphdb = graphdb
        self.uid = uid
        self._start = properties.pop('_meta_start')
        self._end = properties.pop('_meta_end')
        self.properties = properties

    def __eq__(self, other):
        return self.uid == other.uid

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
            **self.properties
        )
        return self

    def delete(self):
        self._graphdb._tuples.delete(self.uid)


class AjguDB(object):

    def __init__(self, path):
        self._tuples = TupleSpace(path)

    def close(self):
        self._tuples.close()

    def _uid(self):
        try:
            counter = self._tuples.get(0)['counter']
        except:
            self._tuples.add(0, counter=1)
            counter = 1
        else:
            counter += 1
            self._tuples.update(0, counter=counter)
        finally:
            return counter

    def transaction(self):
        return self._tuples.transaction()

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

    def edge(self, start, end, **properties):
        uid = self._uid()
        properties['_meta_start'] = start.uid
        properties['_meta_end'] = end.uid
        self._tuples.add(uid, _meta_type='edge', **properties)
        return Edge(self, uid, properties)

    def filter(self, **filters):
        filters = set(filters.items())
        key, value = filters.pop()
        for _, _, uid in self._tuples.query(key, value):
            element = self.get(uid)
            if filters.issuperset(set(element.properties.items())):
                yield element
