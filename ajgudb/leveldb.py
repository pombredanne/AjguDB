# AjuDB - leveldb powered graph database
# Copyright (C) 2015 Amirouche Boubekki <amirouche@hypermove.net>

# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.

# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301  USA
from plyvel import DB

from packing import pack
from packing import unpack


class LevelDBStorage(object):
    """Generic database"""

    def __init__(self, path):
        self.db = DB(
            path,
            create_if_missing=True,
            lru_cache_size=10*10,
            bloom_filter_bits=64,
            block_size=10**9,
            compression=None,
        )
        self.tuples = self.db.prefixed_db(b'tuples')
        self.index = self.db.prefixed_db(b'index')

    def close(self):
        self.db.close()

    def ref(self, uid, key):
        match = [uid, key]
        for key, value in self.tuples.iterator(start=pack(uid, key)):
            other = unpack(key)
            if other == match:
                value = unpack(value)[0]
                return value
            else:
                return None

    def get(self, uid):
        def __get():
            for key, value in self.tuples.iterator(start=pack(uid)):
                other, key = unpack(key)
                if other == uid:
                    value = unpack(value)[0]
                    yield key, value
                else:
                    break

        tuples = dict(__get())
        return tuples

    def add(self, uid, **properties):
        tuples = self.tuples.write_batch(transaction=True)
        index = self.index.write_batch(transaction=True)
        for key, value in properties.items():
            tuples.put(pack(uid, key), pack(value))
            index.put(pack(key, value, uid), '')
        tuples.write()
        index.write()

    def delete(self, uid):
        tuples = self.tuples.write_batch(transaction=True)
        index = self.index.write_batch(transaction=True)
        for key, value in self.tuples.iterator(start=pack(uid)):
            other, name = unpack(key)
            if uid == other:
                tuples.delete(key)
                value = unpack(value)[0]
                index.delete(pack(name, value, uid))
            else:
                break
        tuples.write()
        index.write()

    def update(self, uid, **properties):
        self.delete(uid)
        self.add(uid, **properties)

    def debug(self):
        for key, value in self.tuples.iterator():
            uid, key = unpack(key)
            value = unpack(value)[0]
            print(uid, key, value)

    def query(self, key, value=''):
        match = (key, value) if value else (key,)

        iterator = self.index.iterator(start=pack(key, value))
        for key, value in iterator:
            other = unpack(key)
            ok = reduce(
                lambda previous, x: (cmp(*x) == 0) and previous,
                zip(match, other),
                True
            )
            if ok:
                yield other
            else:
                break
