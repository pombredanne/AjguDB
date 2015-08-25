========
 AjguDB
========

- graphdb
- schemaless
- single thread
- transaction-less
- LGPLv2.1 or later

AjguDB wants to be a fast graph database for python to help your during your
exploration.

API Reference
=============

`from ajgudb import AjguDB`

`AjguDB(path)`
--------------

Create a database at `path`

`AjguDB.close()`
~~~~~~~~~~~~~~~~

close the database

`AjguDB.get(uid)`
~~~~~~~~~~~~~~~~~

Retrieve `Vertex` or `Edge` with `uid` as identifier.

`AjguDB.vertex(**properties)`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a new vertes with `properties` as initial properties.

`AjguDB.get_or_create(**properties)`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Get or create `Vertex` with the provided `properties`.

`AjguDB.select(**properties)`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Retrieve an generator a `GremlinIterator` over the `Edge` and/or `Vertex` with
the `properties` as properties.

`Vertex`
--------

`Vertex` inherit the dictionary, so you can use `dict` method to access
a `Vertex` properties.

`Vertex.uid`
~~~~~~~~~~~~
Return the `Vertex` unique identifier.

`Vertex.incomings()`
~~~~~~~~~~~~~~~~~~~~
Retrieve incoming edges filtered with proc and/or properties.

`Vertex.outgoings()`
~~~~~~~~~~~~~~~~~~~~
Retrieve outgoing edges filtered with proc and/or properties.

`Vertex.save()`
~~~~~~~~~~~~~~~
If the `Vertex` is mutated after creation you must save it.

`Vertex.delete()`
~~~~~~~~~~~~~~~~~
Delete the `Vertex` object.

`Vertex.link(other, **properties)`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Create an `Edge` from the current `Vertex` to `other` with `properties`.


`Edge`
------

`Edge` inherit the dictionary, so you can use `dict` method to access
an `Edge` properties.

`Edge.start()'`
~~~~~~~~~~~~~~~
Return the `Edge` starting `Vertex`.

`Edge.end()`
~~~~~~~~~~~~
Return the `Edge` ending `Vertex`.

`Edge.save()`
~~~~~~~~~~~~~
If the `Edge` is mutated after creation you must save it.

`Edge.delete()`
~~~~~~~~~~~~~~~
Delete the `Edge` object.


`GremlinIterator`
-----------------

This where the magic happens. You can chain methods on the iterator to
realise the query you need. 

This is similar to tinkerpop's `Gremlin <http://gremlindocs.spmallette.documentup.com>`_
except the implementation is incomplete.

Here are the provided operators:

- `GremlinIterator.all()`: retrieve all the results. Most likely returns uids.
- `GremlinIterator.one()`: retrieve the first result and fetch it. Returns a vertex or an edge.
- `GremlinIterator.get()`: retrieve all result and fetch them. Returns vertex and edge.
- `GremlinIterator.count()`: count the number of items in the iterator.
- `GremlinIterator.incomings()`: get incomings edges 
- `GremlinIterator.outgoings()`: get outgoings edges
- `GremlinIterator.both()`: get both incomings and outgoings edges
- `GremlinIterator.start()`: get start vertex
- `GremlinIterator.end()`: get end vertex
- `GremlinIterator.map(proc)`: apply proc to very value in the iterator.
  `proc` takes the `AjguDB` and `GremlinResult` as arugments
- `GremlinIterator.dict()`: get the `dict` of the value
- `GremlinIterator.order(key=lambda x: x, reverse=False)`: order the iterator
- `GremlinIterator.property()` Get the value of property `name` 
- `GremlinIterator.unique()` return an iterator with unique values
- `GremlinIterator.select(**kwargs)` return values matching `kwargs`
- `GremlinIterator.filter(predicate)` return values satisfying `predicate`.
  `predicate` takes `AjguDB` and `GremlinResult` as arugments


Author
======

`Say hi! <amirouche@hypermove.net>`_



