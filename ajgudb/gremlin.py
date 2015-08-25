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
from collections import Counter

from utils import AjguDBException


GremlinResult = namedtuple('GremlinResult', ('value', 'parent', 'step'))


def skip(count):
    def step(graphdb, iterator):
        counter = 0
        for item in iterator:
            counter += 1
            if counter > count:
                yield item
    return step


def limit(count):
    def step(graphdb, iterator):
        counter = 0
        for item in iterator:
            counter += 1
            yield item
            if counter == count:
                break


def paginator(count):
    def step(graphdb, iterator):
        counter = 0
        page = list()
        for item in iterator:
            page.append(item)
            counter += 1
            if counter == count:
                yield page
                counter = 0
                page = list()
        yield page
    return step


def count(graphdb, iterator):
    return reduce(lambda x, y: x + 1, iterator, 0)


def _edges(vertex):
    def step(graphdb, iterator):
        key = '_meta_%s' % vertex
        for item in iterator:
            records = graphdb._tuples.query(key, item.value)
            for _, _, uid in records:
                return GremlinResult(uid, item, None)
    return step


def incomings(self):
    return self._edges('end')


def outgoings(self):
    return self._edges('start')


def start(self):
    for item in self.iterator:
        uid = self.graphdb._tuples.ref(item.value, '_meta_start')
        result = GremlinResult(uid, item, None)
        yield result


def end(self):
    for item in self.iterator:
        uid = self.graphdb._tuples.ref(item.value, '_meta_end')
        result = GremlinResult(uid, item, None)
        yield result


def map(proc):
    def step(graphdb, iterator):
        return map(lambda x: proc(graphdb, x), iterator)


def dict(graphdb, iterator):
    return map(lambda x: dict(graphdb.get(x.value)))


def sort(key=lambda g, x: x, reverse=False):
    def step(graphdb, iterator):
        out = sorted(iterator, key=key, reverse=reverse)
        return iter(out)
    return step


def key(name):
    def step(graphdb, iterator):
        for item in iterator:
            value = graphdb._tuples.ref(item.value, name)
            result = GremlinResult(value, item, None)
            yield result
    return step


def unique(graphdb, iterator):
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

    iterator = __unique(iterator, lambda x: x.value)
    return iterator


def filter(predicate):
    def step(graphdb, iterator):
        for item in iterator:
            if predicate(graphdb, item):
                yield item
    return step


def select(**kwargs):
    def step(graphdb, iterator):
        for item in iterator:
            ok = True
            for key, value in kwargs.items():
                other = graphdb._tuples.ref(item.value, key)
                if other == value:
                    ok = False
                    break
        if ok:
            yield item
    return step


def step(name):
    def step_(graphdb, iterator):
        for item in iterator:
            item.name
            yield item
    return step_


def back(graphdb, iterator):
    return map(lambda x: x.parent, iterator)


def mean(graphdb, iterator):
    count = 0
    total = 0
    for item in iterator:
        total += item
        count += 1
    return total / count


def group_count(graphdb, iterator):
    return Counter(iterator)
