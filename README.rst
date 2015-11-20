========
 AjguDB
========

**This program is alpha becarful**

- graphdb
- schemaless
- single thread
- transaction-less
- GPLv2 or GPLv3

AjguDB wants to be a fast enough python graph database for exploring connected data.

ChangeLog
=========

0.7
---


- ajgudb: you can threat ``AjguDB`` as simple key/value store via its methods
  ``AjguDB.get(key)``, ``AjguDB.set(key, value)`` and ``AjguDB.remove(key)``
- gremlin: ``select`` is renamed ``where`` because it match the SQL terminology.
  SQL's ``FROM`` is ``vertices`` and ``edges`` steps.
- **storage: only wiredtiger 2.6.1 backend storage is supported**
- storage: rework the backend to use less write and similar read operations
  count.
- storage: Now edges and vertices are stored in different tables this might
  also lead to performance improvement during querying.
- storage: elements identifiers are now computed by the backend storage, wiredtiger.


0.5
---

- ajgudb

  - add bsddb backend
  - add wiredtiger backend
  - leveldb: increase block size to 1GB

- gremlin:

  - add ``keys`` to retrieve several keys at the same time
  - use lazy ``itertools.imap`` instead of the gready python2's ``map``


0.4.2
-----

- ajgudb:

  - add a shortcut method ``AjguDB.one(**kwargs)`` to query for one element.

- gremlin:

  - fix ``group_count``, now it's a step and not a *final step*
  - fix ``each`` to return ``GremlinResult`` so that history is not lost
    and ``back`` can be used
  - add ``scatter``, it's only useful after ``group_count`` so far.

- tools:

  - add a converstion function ``ajgudb.tools.to_gt`` to convert the database to
    `graph-tool <https://graph-tool.skewed.de/>`_ graph.
  - there is also a function ``to_nx`` to convert the database to
    `networkx <http://networkx.github.io/>`_


API Reference
=============

``from ajgudb import AjguDB``


``AjguDB(path)``
----------------
Create or open a database at ``path``

``AjguDB.close()``
~~~~~~~~~~~~~~~~~~
Close the database.

``AjguDB.get(key, value)``
~~~~~~~~~~~~~~~~~~~~~~~~~~
Associate ``key`` with ``value`` against the graph.

Graph key/value are stored on their own storage backend.

``AjguDB.get(key)``
~~~~~~~~~~~~~~~~~~~
Retrieve the value associated with ``key``. Raise ``KeyError`` if the key is not
found. 

``AjguDB.remove(key)``
~~~~~~~~~~~~~~~~~~~~~~
Remove the key/value pair associated with ``key``. Raise ``KeyError`` if the
``key`` is not found.


``Vertex``
----------
``Vertex`` class inherit ``dict``, so that you can use ``dict`` method to access
its *graph properties* as dictionary key.

``Vertex.uid``
~~~~~~~~~~~~~~
Return the ``Vertex`` unique identifier.

``Vertex.incomings()``
~~~~~~~~~~~~~~~~~~~~~~
Retrieve incoming edges.

``Vertex.outgoings()``
~~~~~~~~~~~~~~~~~~~~~~
Retrieve outgoing edges.

``Vertex.save()``
~~~~~~~~~~~~~~~~~
If the ``Vertex`` is mutated after creation you must save it.

``Vertex.delete()``
~~~~~~~~~~~~~~~~~~~
Delete the ``Vertex`` object.

``Vertex.link(other, **properties)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Create an ``Edge`` from the current ``Vertex`` to ``other`` with ``properties``.


``Edge``
--------

``Edge`` inherit the dictionary, so you can use ``dict`` method to access
its properties as dictionary keys.

``Edge.start()'``
~~~~~~~~~~~~~~~~~
Return the ``Edge`` starting ``Vertex``.

``Edge.end()``
~~~~~~~~~~~~~~
Return the ``Edge`` ending ``Vertex``.

``Edge.save()``
~~~~~~~~~~~~~~~
If the ``Edge`` is mutated after creation you must save it.

``Edge.delete()``
~~~~~~~~~~~~~~~~~
Delete the ``Edge`` object.


``gremlin``
-----------

You can query the graph by composing steps. It is similar to tinkerpop's
`Gremlin language <http://gremlindocs.spmallette.documentup.com>`_.

This are the functions that you have to use to query the graph using
`AjguDB.query`.

Here are the provided steps:

- ``count``: count the number of items in the iterator.
- ``incomings``: get incomings edges.
- ``outgoings``: get outgoings edges.
- ``both``: get both incomings and outgoings edges.
- ``start``: get start vertex.
- ``end``: get end vertex.
- ``value``: get the ``dict`` of the value.
- ``order(key=lambda x: x, reverse=False)``: order the iterator.
- ``key(name)`` Get the value of ``name`` key.
- ``key(*names)`` Get the values of keys in ``names``.
- ``unique`` return an iterator with unique values.
- ``select(**kwargs)`` return values matching ``kwargs``.
- ``filter(predicate)`` return values satisfying ``predicate``.
  ``predicate`` takes ``AjguDB`` and ``GremlinResult`` as arugments
- ``each(proc)``: apply proc to very value in the iterator.
  ``proc`` takes the ``AjguDB`` and ``GremlinResult`` as arugments.
- ``mean`` compute the mean value.
- ``group_count`` Return a counter made of the values from the previous step
- ``scatter`` unroll the content of the iterator

They are a few steps missing compared to gremlin reference implementation.

Missing steps with comments:

- both, bothE, bothV => use incomings, outgoings, start and end)
- gather, groupBy => ???
- group_count with side effect => ???
- memoize => ???
- cap => ???
- select => ???
- and, or => use python
- except, retain => use filter instead
- hasNot => use filter instead
- interval => use filter instead
- random, shuffle => ???
- optional => can't implement that without troubles
- sideEffect => ???
- store => ???
- table => ???
- tree => ???
- branch steps => use python


Author
======

`Say hi! <amirouche@hypermove.net>`_
