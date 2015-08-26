========
 AjguDB
========

**This program is alpha becarful**

- graphdb
- schemaless
- single thread
- transaction-less
- LGPLv2.1 or later

AjguDB wants to be a fast graph database for python to help your during your
exploration.

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
similar to tinkerpop's `Gremlin <http://gremlindocs.spmallette.documentup.com>`_.

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
- ``unique`` return an iterator with unique values.
- ``select(**kwargs)`` return values matching ``kwargs``.
- ``filter(predicate)`` return values satisfying ``predicate``.
  ``predicate`` takes ``AjguDB`` and ``GremlinResult`` as arugments
- ``each(proc)``: apply proc to very value in the iterator.
  ``proc`` takes the ``AjguDB`` and ``GremlinResult`` as arugments.
- ``mean`` compute the mean value.
- ``group_count`` Return a counter made of the values from the previous step

They are a few steps missing compared to gremlin reference implementation.
That said, you can easily implement them yourself:

Missing steps with comments:

- both, bothE, bothV => use incomings, outgoings, start and end)
- gather, scatter, groupBy => ???
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
