# AjuDB - wiredtiger powered graph database
# Copyright (C) 2015 Amirouche Boubekki <amirouche@hypermove.net>
from .gremlin import vertices
from .gremlin import edges
from .gremlin import get


def to_nx(db):
    """Convert db to networkx representation"""
    import networkx as nx

    graph = nx.DiGraph()

    graph.add_nodes_from(db.vertices())

    for item in db.edges():
        start = db._tuples.ref(item.value, '_meta_start')
        end = db._tuples.ref(item.value, '_meta_end')
        graph.add_edge(start, end)

    return graph


def to_gt(db):
    """Convert db to graph-tool representation"""
    from graph_tool.all import Graph

    graph = Graph(directed=True)

    mapping = dict()

    for native in db.query(vertices, get)():
        vertex = graph.add_vertex()
        mapping[native.uid] = graph.vertex_index[vertex]

    for native in db.query(edges, get)():
        start = native.start().uid
        start = mapping[start]
        end = native.end().uid
        end = mapping[end]
        graph.add_edge(start, end)

    return graph


