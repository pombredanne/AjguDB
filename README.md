# Building a python graphdb in one night

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
same index. It can be improved by using:

- one table for edge documents
- one table for edges links
- one table for vertex documents

Moving to this schema will be easier, but  I don't need it yet.
  
The main advantage of the new schema is that it's simple to implement,
understand and has a low sloc count.

## What is the Tuple Space

The idea behind tuple space is to store a set of tuples inside a single table
that look like the following:


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

## Implementation of a Tuple Space

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

### Key composition

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

```
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
