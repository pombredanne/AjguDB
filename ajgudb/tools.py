import networkx as nx


def to_nx(db):
    graph = nx.DiGraph()

    graph.add_nodes_from(db.vertices())

    for item in db.edges():
        start = db._tuples.ref(item.value, '_meta_start')
        end = db._tuples.ref(item.value, '_meta_end')
        graph.add_edge(start, end)

    return graph
