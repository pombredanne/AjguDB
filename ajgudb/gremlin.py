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
from collections import namedtuple

from utils import AjguDBException


GremlinResult = namedtuple('GremlinResult', ('value', 'parent', 'step'))


class GremlinIterator(object):

    sentinel = object()

    def __init__(self, graphdb, iterator):
        self.iterator = iterator
        self.graphdb = graphdb

    def __iter__(self):
        return self.iterator

    def all(self):
        return list(map(lambda x: x.value, self.iterator))

    def get(self):
        return list(map(lambda x: self.graphdb.get(x.value), self.iterator))

    def one(self, default=sentinel):
        try:
            uid = next(self.iterator).value
        except StopIteration:
            if default is self.sentinel:
                raise AjguDBException('empty result')
            else:
                return default
        else:
            return self.graphdb.get(uid)

    def skip(self, count):
        def iterator():
            counter = 0
            for item in self:
                counter += 1
                if counter > count:
                    yield item
        return type(self)(self.graphdb, iterator())

    def limit(self, count):
        def iterator():
            counter = 0
            for item in self:
                counter += 1
                yield item
                if counter == count:
                    break
        return type(self)(self.graphdb, iterator())

    def __getitem__(self, slice):
        raise NotImplementedError()

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
        return type(self)(self.graphdb, iterator())

    def count(self):
        return reduce(lambda x, y: x+1, self, 0)

    def _edges(self, vertex):
        def iterator():
            key = '_meta_%s' % vertex
            for item in self.iterator:
                records = self.graphdb._tuples.query(key, item.value)
                for _, _, uid in records:
                    result = GremlinResult(uid, item, None)
                    yield result
        return type(self)(self.graphdb, iterator())

    def incomings(self):
        return self._edges('end')

    def outgoings(self):
        return self._edges('start')

    def start(self):
        def iterator():
            for item in self.iterator:
                uid = self.graphdb._tuples.ref(item.value, '_meta_start')
                result = GremlinResult(uid, item, None)
                yield result
        return type(self)(self.graphdb, iterator())

    def end(self):
        def iterator():
            for item in self.iterator:
                uid = self.graphdb._tuples.ref(item.value, '_meta_end')
                result = GremlinResult(uid, item, None)
                yield result
        return type(self)(self.graphdb, iterator())

    def map(self, proc):
        def iterator():
            for item in self.iterator:
                value = proc(self.graphdb, item)
                yield GremlinResult(value, item, None)
        return type(self)(self.graphdb, iterator())

    def dict(self):
        return type(self)(self.graphdb, self.map(lambda g, v: dict(g.get(v.value))))

    def sort(self, key=lambda g, x: x, reverse=False):
        out = sorted(
            self,
            key=lambda x: key(self.graphdb, x),
            reverse=reverse
        )
        return type(self)(self.graphdb, iter(out))

    def property(self, name):
        def iterator():
            for item in self.iterator:
                value = self.graphdb._tuples.ref(item.value, name)
                result = GremlinResult(value, item, None)
                yield result
        return type(self)(self.graphdb, iterator())

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

        iterator = __unique(self.iterator, lambda x: x.value)
        return type(self)(self.graphdb, iterator)

    def filter(self, predicate):
        def iterator():
            for item in self.iterator:
                if predicate(self.graphdb, item):
                    yield item
        return type(self)(self.graphdb, iterator())

    def select(self, **kwargs):
        properties = set(kwargs.items())

        def predicate(graphdb, item):
            element = graphdb.get(item.value)
            return properties.issubset(element.items())

        return self.filter(predicate)

    def step(self, name):
        def set_step(item):
            item.step = name
        return map(set_step, self.iterator)

    def back(self):
        def iterator():
            for item in self.iterator:
                yield item.parent
        return type(self)(self.graphdb, iterator())

    def average(self):
        values = self.all()
        return float(sum(values)) / float(len(values))
