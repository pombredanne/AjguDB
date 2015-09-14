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

from itertools import imap

from .ajgudb import Base


GremlinResult = namedtuple('GremlinResult', ('value', 'parent', 'step'))


def query(*steps):
    """Gremlin pipeline builder and executor"""
    def composed(graphdb, iterator=None):
        if isinstance(iterator, Base):
            iterator = [GremlinResult(iterator.uid, None, None)]
        elif isinstance(iterator, GremlinResult):
            iterator = [iterator]
        for step in steps:
            iterator = step(graphdb, iterator)
        return iterator
    return composed


def select(**kwargs):
    """Iterator that *select* elements based on key value"""
    def step(graphdb, iterator):
        if iterator:
            for item in iterator:
                ok = True
                for key, value in kwargs.items():
                    other = graphdb._tuples.ref(item.value, key)
                    if other != value:
                        ok = False
                        break
                if ok:
                    yield item
        else:
            items = kwargs.items()
            for _, _, uid in graphdb._tuples.query(*items[0]):
                ok = True
                for key, value in items[1:]:
                    other = graphdb._tuples.ref(uid, key)
                    if value != other:
                        ok = False
                        break
                if ok:
                    yield GremlinResult(uid, None, None)
    return step


def vertices(graphdb, iterator):
    """Iterator over all vertices"""
    for _, _, uid in graphdb._tuples.query('_meta_type', 'vertex'):
        yield GremlinResult(uid, None, None)


def edges(graphdb, iterator):
    """Iterator over all edges"""
    for _, _, uid in graphdb._tuples.query('_meta_type', 'edge'):
        yield GremlinResult(uid, None, None)


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
    return step


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


def _edges(vertex, graphdb, iterator):
    key = '_meta_%s' % vertex
    for item in iterator:
        records = graphdb._tuples.query(key, item.value)
        for _, _, uid in records:
            yield GremlinResult(uid, item, None)


def incomings(graphdb, iterator):
    return _edges('end', graphdb, iterator)


def outgoings(graphdb, iterator):
    return _edges('start', graphdb, iterator)


def start(graphdb, iterator):
    for item in iterator:
        uid = graphdb._tuples.ref(item.value, '_meta_start')
        result = GremlinResult(uid, item, None)
        yield result


def end(graphdb, iterator):
    for item in iterator:
        uid = graphdb._tuples.ref(item.value, '_meta_end')
        result = GremlinResult(uid, item, None)
        yield result


def each(proc):
    def step(graphdb, iterator):
        return imap(lambda x: GremlinResult(proc(graphdb, x), x, None), iterator)
    return step


def value(graphdb, iterator):
    return list(imap(lambda x: x.value, iterator))


def get(graphdb, iterator):
    return list(imap(lambda x: graphdb.get(x.value), iterator))


def sort(key=lambda g, x: x, reverse=False):
    def step(graphdb, iterator):
        out = sorted(iterator, key=lambda x: key(graphdb, x), reverse=reverse)
        return iter(out)
    return step


def key(name):
    def step(graphdb, iterator):
        for item in iterator:
            value = graphdb._tuples.ref(item.value, name)
            result = GremlinResult(value, item, None)
            yield result
    return step


def keys(*names):
    def step(graphdb, iterator):
        for item in iterator:
            values = list()
            for name in names:
                value = graphdb._tuples.ref(item.value, name)
                values.append(value)
            result = GremlinResult(values, item, None)
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


def step(name):
    def step_(graphdb, iterator):
        for item in iterator:
            item.name
            yield item
    return step_


def back(graphdb, iterator):
    return imap(lambda x: x.parent, iterator)


def path(steps):

    def path_reducer(previous, _):
        previous.append(previous[-1].parent)
        return previous

    def step(graphdb, iterator):
        for item in iterator:
            yield reduce(path_reducer, range(steps), [item])

    return step


def mean(graphdb, iterator):
    count = 0
    total = 0
    for item in iterator:
        total += item
        count += 1
    return total / count


def group_count(graphdb, iterator):
    yield Counter(imap(lambda x: x.value, iterator))


def scatter(graphdb, iterator):
    for item in iterator:
        for other in item.value:
            yield GremlinResult(other, item, None)


MockBase = namedtuple('MockBase', ('uid', ))


def link(**kwargs):
    def step(graphdb, iterator):
        start = graphdb.get_or_create(**kwargs)
        for item in iterator:
            node = MockBase(item.value)
            start.link(node)
        yield start
    return step
