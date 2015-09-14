========
 AjguDB
========

**This program is alpha becarful**

- graphdb
- schemaless
- single thread
- transaction-less
- LGPLv2.1 or later

AjguDB wants to be a easy to use graph database for python to help during
graph exploration of data that does not fit in RAM and requires a graph API.

It support three backends LevelDB, WiredTiger and Oracle Berkeley Database.

AjguDB index all fields for the better and the worst. The better
being is that it's easy to use API. Make sure to only import the data you need
and use something else to store fields you don't need to be indexed.

You might hit issues regarding encoding, there is I think no way to solve them
once on for all without moving to Python 3. 

Roadmap
=======

The  0.5.x will remain the stable release for the time being. Work on the
`develop branch <https://github.com/amirouche/AjguDB/tree/develop>`_ will
become 0.7 when its time comes.


0.7
---

- Python 3: missing wiredtiger bindings that works with python 3. my
  `wiredtiger-ffi <https://github.com/amirouche/python-wiredtiger-ffi>`_ is buggy

- Improve performance. ajgudb doesn't compete at all against sqlite 
  while loading stackexchange's superuser dump (5G), neither does it handle
  well querying the data, probably because of "index-all-the-thing" feature.
  The ``TupleSpace`` design is a nice but it's not the only tool required to build
  a graph database that includes many kinds of data.

- wiredtiger backend

  Prelimanry benchmarks show that leveldb and bsddb does not perform as good on
  batch insert 5G (superuser) and 50G (wikidata) and official benchmarks
  says that it performs better on random read/write. So the plan is to move to
  to wiredtiger only.


Other stuff
-----------
  
- Add support for wiredtiger transactions. Transactions can improve performance.
- Add full-text search indices.
- Add geographic indices.
- Add Cassandra backend.
    

ChangeLog
=========

0.5.1
-----

- ajgudb: when a vertex is deleted its edges must also be deleted
- wiredtiger: when the table is empty avoid to crash
- gremlin: add ``path(number_of_steps)`` step wich returns the current node
  and its ancestors.


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
close the database.

``AjguDB.get(uid)``
~~~~~~~~~~~~~~~~~~~
Retrieve ``Vertex`` or ``Edge`` with ``uid`` as identifier.

``AjguDB.vertex(**properties)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Create a new vertes with ``properties`` as initial properties.

``AjguDB.get_or_create(**properties)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Get or create ``Vertex`` with the provided ``properties``.

``AjguDB.one(**properties)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Get a vertex or edge that match the given ``properties`` or return `None`.

``AjguDB.query(*steps)``
~~~~~~~~~~~~~~~~~~~~~~~~
Create a query against this graph using gremlin `steps`. This returns a function
that can take an iterator, an edge, a vertex or nothing as arguments. It depends
of the query.

Here is an exemple query against movielens that takes a vertex as first argument:

.. code::

   query = db.query(incomings, filter(isgood), count)

If you want to know the number of good rating that a `movie` has received use
call `query` as follow:

.. code::

   good_rating_count = query(movie)


``Vertex``
----------

``Vertex`` inherit the dictionary, so you can use ``dict`` method to access
its properties as dictionary key.

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

This where the magic happens. You can query the graph by composing steps. It is
similar to tinkerpop's `Gremlin language <http://gremlindocs.spmallette.documentup.com>`_.

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
- ``back`` retrieve the parent element
- ``path(number_of_steps)`` return ``number_of_steps`` of previous elements
  starting with the current element. The returned object is a list of size
  ``number_of_steps + 1`` formed of the elements of the path that leads to the
  current element included. It allows to do ``join`` operations.

They are a few steps missing compared to gremlin reference implementation.
That said, you can easily implement them yourself:

Missing steps with comments:

- both, bothE, bothV => use incomings, outgoings, start and end)
- gather, groupBy => ???
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

`Say héllo! <amirouche@hypermove.net>`_
