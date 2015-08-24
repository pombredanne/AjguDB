# Building a python graphdb in one night

- graphdb
- schemaless
- single thread
- transaction-less
- LGPLv2.1 or later


## Introduction

*This is a new version of [ajgu](https://pypi.python.org/pypi/ajgu)*.

You maybe already know that I am crazy about graph databases. I am experimenting
around building a graph databases using Python. While I did little work on the
query language side or [GraphQL](https://github.com/graphql/). I did experiment
with a new storage schema.

It is inspired from [datomic](https://docs.datomic.com/) Entity-Attribute-Value
pattern. I call it the `TupleSpace`.

## Spoiler

One has to know that there is much much less code in this graphdb
implementation *but* that edges and vertices share the same table and the
same index. 
  
The main advantage of the new schema is that it's simple to implement,
understand and has a low sloc count. As an extra simplification I use
[plyvel python leveldb bindings](https://plyvel.readthedocs.org/en/latest/)
which provides a very nice API.

## What is the Tuple Space

The idea behind tuple space is to store a set of tuples inside a single
table that look like the following:


```
(1, "title", "Building a python graphdb in one night")
(1, "body", "You maybe already know that I am...")
(1, "publishedat", "2015-08-23")

(2, "name", "database")

(3, "start", 1)
(3, "end", 2)

...

(4, "title", "key/value store key composition")

...

(42, "title", "building a graphdb with HappyBase")
```

The tuple can be described as `(identifier, name, value)`.

If you study the above example you will discover that both edges (aka.
`ManyToMany` relations) are represented using the same tuple schema.

Representing `ForeignKey` is possible but is left as an exercice to
the reader ;)

## Implementation of a `TupleSpace`

To implement the above schema inside a *ordered key/value* store we have
to find a relevant key. That key I think, is the composition of the `identifier`
and `name`. This leads to definition of the scheme

```
Key(identifier, name) -> Value(value)
```

Every key is unique and is associated with a `value`. Given the fact that the
store is ordered one can easily retrieve every `(name, value)` tuple associated
with a given `identifier` by going through the the ordered key space.

The above tuple space will look like in the key/value database using a high
level view (ie. not bytes view) like the following:

```
         |           Key            |                 Value
---------+------------+-------------+------------------------------------------
Colunms  | identifier |  name       |                 value
---------+------------+-------------+------------------------------------------
         |     1      |    title    | "Building a python graphdb in one night"
         |     1      |    body     | "You maybe already know that I am..."
         |     1      | publishedat |              "2015-08-23"

         |     2      |   name      |                database

         |     3      |   start     |                   1
         |     3      |    end      |                   2

              ...          ...                         ...

         |     4      |   title     |     "key/value store key composition"

              ...          ...                         ...

         |     42     |   title     |    "building a graphdb with HappyBase"

              ...          ...                         ...

```

## Indexing

Every tuple is indexed, whatever the tuple. This can be improved, but it's
the simplest.

To index a tuple, we use a permutation of the tuple to make easy using
plyvel range queries ie. `DB.iterator()` to retrieve a given tuple knowing
its `name` or `name` and `value`. The key schema of the index is the following:


```
Key(name, value, identifier) ->
```

The value is not used. It could be used to store all the document associated
with `identifier`.


## Key composition

To keep the database ordered you need to pack correctly the components of
the key. You can not simply convert string to bytes, how will you distinguish
the string from the other components of the key? You can't use the string
representation of number ie. `"42"` for `"42"`. Remember `"2"` is bigger
than `"10"`. "aac"

In a complete `TupleSpace` implementation, since values are mixed inside the
same table. The solution to support all numbers is to always use the same
packing schema whatever the signature and whether they are float or not.

The simple case of positive integers is solved by `struct.pack('>Q', number)`.

Here is a naive packing function that support every Python objects, keeps
the ordering of strings and positive integers where integers comes before
strings which come before other kind of Python values.

```python
def pack(*values):
    def __pack(value):
        if type(value) is int:
            return '1' + struct.pack('>q', value)
        elif type(value) is str:
            return '2' + value + '\0'
        else:
            data = dumps(value, encoding='utf-8')
            return '3' + struct.pack('>q', len(data)) + data
    return ''.join(map(__pack, values))

```

## `TupleSpace` API


The tuples schema provide a `Document` objects represented as simple dict.
So eventually `TupleSpace` API looks like:

```python
def add(uid, **document):
    pass

def update(uid, **document):
    pass

def delete(uid):
    pass
```
    
Given a document
    
```python
post = dict(
    title="Building a python graphdb in one night",
    body="You maybe already know that I am...",
    publishedat="2015-08-23",
)
```

And an identifier you can add that document to the tuple space.
The identifier can come from the outside or like the graphdb does
can be a counter that is stores as document `0` inside the `TupleSpace`.


## GraphDB

At this point,  the `TupleSpace` provides documents and a bit relational
paradigm as you can work with references. `AjguDB` provides a layer on top
of `TupleSpace` to easily work with graph database.

### Data model

The first aspect is building the graph data model:

- `Vertex` are simple `TupleSpace` documents which their identifier comes from
  document `0` counter which is incremented everytime a new vertex or edge
  is created. Moreover is stores in as `_meta_type` name (document attribute)
  that the document represent a `Vertex`

- `Edge` are also simple `TupleSpace` documents with their identifier like for
  `Vertex` comes from the *same* document `0`. Same as `Vertex`, `Edge` document
  store as `_meta_type` the fact that they are edge. Moreover `start` and `end`
  attributes are also stores in the `TupleSpace` document.

Given the fact that every tuples are indexed, it's easy to retrieve
all *incomings* and *outgoings* edges of a given `Vertex` so it's not required
to cache them in the `Vertex` document (as it is done in
[ajgu](https://pypi.python.org/pypi/ajgu)).

### Better schema

Even if this requires benchmarking, an idea to improve performance one
`TupleSpace` object for:

- edge documents
- edge links
- vertex documents

Or better use a specific schema *edge links*.

### API

I changed some what the API compared to `ajgu`. The in particular

- `label` is not required but recommended
- queries happens on both `Vertex` and `Edge` space so make sure
  to namespace them somehow. Or use the `_meta_type` as filter.

#### `AjguDB`

`from ajgudb import AjguDB`

##### `AjguDB(path)`

Create a database at `path`

##### `AjguDB.close()`

close the database

##### `AjguDB.get(uid)`

Retrieve `Vertex` or `Edge` with `uid` as identifier.

##### `AjguDB.vertex(**properties)`

Create a new vertes with `properties` as initial properties.

##### `AjguDB.get_or_create(**properties)`

Get or create `Vertex` with the provided `properties`.

##### `AjguDB.filter(**properties)`

Retrieve an generator a `GremlinIterator` over the `Edge` and/or `Vertex` with
the `properties` as properties.

#### `Vertex`

`Vertex` inherit the dictionary, so you can use `dict` method to access
a `Vertex` properties.

##### `Vertex.uid`

Return the `Vertex` unique identifier.

##### `Vertex.incomings(proc=None, **properties)`

Retrieve incoming edges filtered with proc and/or properties.

##### `Vertex.outgoings(proc=None, **properties)`

Retrieve outgoing edges filtered with proc and/or properties.

##### `Vertex.save()`

If the `Vertex` is mutated after creation you must save it.

##### `Vertex.delete()`

Delete the `Vertex` object.

##### `Vertex.link(other, **properties)`

Create an `Edge` from the current `Vertex` to `other` with `properties`.

#### `Edge`

`Edge` inherit the dictionary, so you can use `dict` method to access
an `Edge` properties.

##### `Edge.start()'`

Return the `Edge` starting `Vertex`.

##### `Edge.end()`

Return the `Edge` ending `Vertex`.

##### `Edge.save()`

If the `Edge` is mutated after creation you must save it.

##### `Edge.delete()`

Delete the `Edge` object.


#### `GremlinIterator`

This where the magic happens. You can chain methods on the iterator to
realise the query you need. 

This is similar to tinkerpop's [Gremlin](http://gremlindocs.spmallette.documentup.com)
except the implementation is incomplete and can be faster.

Here are the provided operators:

- all
- one
- count
- incomings
- outgoings
- both
- start
- end
- gather
- map
- dict
- uid
- order
- property
- unique
- filter

