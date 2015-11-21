# AjuDB - wiredtiger powered graph database
# Copyright (C) 2015 Amirouche Boubekki <amirouche@hypermove.net>
from itertools import imap

from collections import namedtuple
from collections import Counter

from .ajgudb import Vertex
from .ajgudb import Edge


VERTEX, EDGE = range(2)

GremlinResult = namedtuple('GremlinResult', ('value', 'parent', 'kind'))


def query(*steps):
    """Gremlin pipeline builder and executor"""

    def composed(graphdb, iterator=None):
        if isinstance(iterator, Vertex):
            iterator = [GremlinResult(iterator.uid, None, VERTEX)]
        elif isinstance(iterator, Edge):
            iterator = [GremlinResult(iterator.uid, None, EDGE)]
        elif isinstance(iterator, GremlinResult):
            iterator = [iterator]
        for step in steps:
            iterator = step(graphdb, iterator)
        return iterator

    return composed


def vertices(label=''):
    def step(graphdb, iterator):
        """Iterator over all vertices"""
        for uid in graphdb._storage.vertices.identifiers(label):
            yield GremlinResult(uid, None, VERTEX)
    return step


def edges(label=None):
    def step(graphdb, iterator):
        """Iterator over all vertices"""
        if label:
            for uid in graphdb._storage.edges.identifiers(label):
                yield GremlinResult(uid, None, VERTEX)
        else:
            for uid in graphdb._storage.edges.all():
                yield GremlinResult(uid, None, VERTEX)
    return step


def select_vertices(**kwargs):
    if len(kwargs.items()) > 1:
        raise Exception('Only one key/value pair is supported')
    key, value = kwargs.items()[0]

    def step(graphdb, _):
        for uid in graphdb._storage.vertices.keys(key, value):
            yield GremlinResult(uid, None, VERTEX)

    return step


def where(**kwargs):
    """Iterator that selects elements that match the `kwargs` specification"""
    def step(graphdb, iterator):
        for item in iterator:
            for key, value in kwargs.items():
                if item.kind == VERTEX:
                    _, properties = graphdb._storage.vertices.get(item.value)
                else:
                    _, _, _, properties = graphdb._storage.edges.get(item.value)
                if properties[key] != value:
                    break
            else:
                yield item
    return step


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


# edges navigation

def _edges(direction, graphdb, iterator):
    query = getattr(graphdb._storage.edges, direction)
    for item in iterator:
        for uid in query(item.value):
            yield GremlinResult(uid, item, EDGE)


def incomings(graphdb, iterator):
    return _edges('incomings', graphdb, iterator)


def outgoings(graphdb, iterator):
    return _edges('outgoings', graphdb, iterator)


#

def start(graphdb, iterator):
    for item in iterator:
        start, _, _, _ = graphdb._storage.edges.get(item.value)
        yield GremlinResult(start, item, VERTEX)


def end(graphdb, iterator):
    for item in iterator:
        _, _, end, _ = graphdb._storage.edges.get(item.value)
        yield GremlinResult(end, item, VERTEX)


def each(proc):
    def step(graphdb, iterator):
        return imap(lambda x: GremlinResult(proc(graphdb, x), x, None), iterator)  # noqa
    return step


def value(graphdb, iterator):
    return imap(lambda x: x.value, iterator)


def get(graphdb, iterator):
    def __iterator():
        for item in iterator:
            if item.kind == VERTEX:
                yield graphdb.vertex.get(item.value)
            else:
                yield graphdb.edge.get(item.value)
    return list(__iterator())


def sort(key=lambda g, x: x, reverse=False):
    def step(graphdb, iterator):
        out = sorted(iterator, key=lambda x: key(graphdb, x), reverse=reverse)
        return iter(out)
    return step


def key(name):
    def step(graphdb, iterator):
        for item in iterator:
            if item.kind == VERTEX:
                _, properties = graphdb._storage.vertices.get(item.value)
            else:
                _, _, _, properties = graphdb._storage.edges.get(item.value)
            try:
                yield GremlinResult(properties[name], item, None)
            except KeyError:
                pass
    return step


def keys(*names):
    def step(graphdb, iterator):
        for item in iterator:
            values = list()
            for name in names:
                if item.kind == VERTEX:
                    value = graphdb._storage.vertices.tuple(item.value, name)
                else:
                    value = graphdb._storage.edges.tuple(item.value, name)
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


def back(graphdb, iterator):
    return imap(lambda x: x.parent, iterator)


def path(steps):

    def path_(previous, _):
        previous.append(previous[-1].parent)
        return previous

    def step(graphdb, iterator):
        for item in iterator:
            yield reduce(path_, range(steps), [item])

    return step


def mean(graphdb, iterator):
    count = 0.
    total = 0.
    for item in iterator:
        total += item
        count += 1
    return total / count


def group_count(graphdb, iterator):
    yield Counter(map(lambda x: x.value, iterator))


def scatter(graphdb, iterator):
    for item in iterator:
        for other in item.value:
            yield GremlinResult(other, item, None)


MockBase = namedtuple('MockBase', ('uid', ))
