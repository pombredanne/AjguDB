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


steps = list()


def register(step):
    steps.append(step)
    return step


# class GremlinResult(object):

#     # __slots__ = ('value', 'parent', 'step')

#     def __init__(self, value, parent, step, graphdb, iterator):
#         self._graphdb = graphdb
#         self._iterator = iterator
#         self._value = value
#         self._parent = parent
#         self._step = step
#         self._steps = dict()
#         for step in steps:
#             self._steps[step.__name__] = step

#     def ___getattr__(self, name):
#         return self._steps[name]

#     def query(self, iterator):
#         from .ajgudb import Base
#         if isinstance(iterator, Base):
#             iterator = [GremlinResult(iterator.uid, None, None)]
#         elif isinstance(iterator, GremlinResult):
#             iterator = [iterator]
#         iterator = self.iterator
#         for step in steps:
#             iterator = step(self.graphdb, iterator)
#         return iterator


GremlinResult = namedtuple('GremlinResult', ('value', 'parent', 'step'))


@register
def skip(count):
    def step(graphdb, iterator):
        counter = 0
        for item in iterator:
            counter += 1
            if counter > count:
                yield item
    return step


@register
def limit(count):
    def step(graphdb, iterator):
        counter = 0
        for item in iterator:
            counter += 1
            yield item
            if counter == count:
                break
    return step


@register
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


@register
def count(graphdb, iterator):
    return reduce(lambda x, y: x + 1, iterator, 0)


def _edges(vertex, graphdb, iterator):
    key = '_meta_%s' % vertex
    for item in iterator:
        records = graphdb._tuples.query(key, item.value)
        for _, _, uid in records:
            yield GremlinResult(uid, item, None)


@register
def incomings(graphdb, iterator):
    return _edges('end', graphdb, iterator)


@register
def outgoings(graphdb, iterator):
    return _edges('start', graphdb, iterator)


@register
def start(graphdb, iterator):
    for item in iterator:
        uid = graphdb._tuples.ref(item.value, '_meta_start')
        result = GremlinResult(uid, item, None)
        yield result


@register
def end(graphdb, iterator):
    for item in iterator:
        uid = graphdb._tuples.ref(item.value, '_meta_end')
        result = GremlinResult(uid, item, None)
        yield result


@register
def each(proc):
    def step(graphdb, iterator):
        return map(lambda x: proc(graphdb, x), iterator)


@register
def value(graphdb, iterator):
    return map(lambda x: x.value, iterator)


@register
def get(graphdb, iterator):
    return list(map(lambda x: graphdb.get(x.value), iterator))


@register
def sort(key=lambda g, x: x, reverse=False):
    def step(graphdb, iterator):
        out = sorted(iterator, key=lambda x: key(graphdb, x), reverse=reverse)
        return iter(out)
    return step


@register
def key(name):
    def step(graphdb, iterator):
        for item in iterator:
            value = graphdb._tuples.ref(item.value, name)
            result = GremlinResult(value, item, None)
            yield result
    return step


@register
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


@register
def filter(predicate):
    def step(graphdb, iterator):
        for item in iterator:
            if predicate(graphdb, item):
                yield item
    return step


@register
def select(**kwargs):
    def step(graphdb, iterator):
        for item in iterator:
            ok = True
            for key, value in kwargs.items():
                other = graphdb._tuples.ref(item.value, key)
                if other != value:
                    ok = False
                    break
            if ok:
                yield item

    return step


@register
def step(name):
    def step_(graphdb, iterator):
        for item in iterator:
            item.name
            yield item
    return step_


@register
def back(graphdb, iterator):
    return map(lambda x: x.parent, iterator)


@register
def mean(graphdb, iterator):
    count = 0
    total = 0
    for item in iterator:
        total += item
        count += 1
    return total / count


@register
def group_count(graphdb, iterator):
    return Counter(iterator)
