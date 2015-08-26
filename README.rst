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
--------------

Create a database at ``path``

``AjguDB.close()``
~~~~~~~~~~~~~~~~

close the database

``AjguDB.get(uid)``
~~~~~~~~~~~~~~~~~

Retrieve ``Vertex`` or ``Edge`` with ``uid`` as identifier.

``AjguDB.vertex(**properties)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a new vertes with ``properties`` as initial properties.

``AjguDB.get_or_create(**properties)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Get or create ``Vertex`` with the provided ``properties``.

``AjguDB.select(**properties)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Retrieve an generator a ``GremlinIterator`` over the ``Edge`` and/or ``Vertex`` with
the ``properties`` as properties.

``Vertex``
--------

``Vertex`` inherit the dictionary, so you can use ``dict`` method to access
a ``Vertex`` properties.

``Vertex.uid``
~~~~~~~~~~~~
Return the ``Vertex`` unique identifier.

``Vertex.incomings()``
~~~~~~~~~~~~~~~~~~~~
Retrieve incoming edges filtered with proc and/or properties.

``Vertex.outgoings()``
~~~~~~~~~~~~~~~~~~~~
Retrieve outgoing edges filtered with proc and/or properties.

``Vertex.save()``
~~~~~~~~~~~~~~~
If the ``Vertex`` is mutated after creation you must save it.

``Vertex.delete()``
~~~~~~~~~~~~~~~~~
Delete the ``Vertex`` object.

``Vertex.link(other, **properties)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Create an ``Edge`` from the current ``Vertex`` to ``other`` with ``properties``.


``Edge``
------

``Edge`` inherit the dictionary, so you can use ``dict`` method to access
an ``Edge`` properties.

``Edge.start()'``
~~~~~~~~~~~~~~~
Return the ``Edge`` starting ``Vertex``.

``Edge.end()``
~~~~~~~~~~~~
Return the ``Edge`` ending ``Vertex``.

``Edge.save()``
~~~~~~~~~~~~~
If the ``Edge`` is mutated after creation you must save it.

``Edge.delete()``
~~~~~~~~~~~~~~~
Delete the ``Edge`` object.


``GremlinIterator``
-----------------

This where the magic happens. You can query the graph by composing steps.

This is similar to tinkerpop's `Gremlin <http://gremlindocs.spmallette.documentup.com>`_
except the implementation is incomplete.

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

For instance you can do:

.. code::

   query = graphdb.query(select(label='movie'), incomings, filter(label='rating'), key('value'), sort(lambda x.value), limit(10), back, end, value('title'))
   for movie in query(graphdb.vertices()):
       print movie

This will select the 10 poorest film on movielens.

Missing steps with comments:

- both, bothE, bothV => use incomings, outgoings, start and end)
- gather, scatter, groupBy => why?
- group_count with side effect => why?
- memoize => why?
- cap => why?
- select => why?
- and, or => use python
- except, retain => use filter instead
- hasNot => use filter instead
- interval => use filter instead
- random, shuffle => why?
- optional => can't implement that without troubles
- sideEffect => why?
- store => why?
- table => why?
- tree => why?
- branch steps => use python
  

Author
======

`Say hi! <amirouche@hypermove.net>`_
