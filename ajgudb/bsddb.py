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
from bsddb3.db import DB
from bsddb3.db import DBEnv
from bsddb3.db import DB_BTREE
from bsddb3.db import DB_CREATE
from bsddb3.db import DB_INIT_MPOOL
from bsddb3.db import DB_LOG_AUTO_REMOVE

from packing import pack
from packing import unpack


class BSDDBStorage(object):
    """Generic database"""

    def __init__(self, path):
        self.env = DBEnv()
        self.env.set_cache_max(10, 0)
        self.env.set_cachesize(5, 0)
        flags = (
            DB_CREATE
            | DB_INIT_MPOOL
        )
        self.env.log_set_config(DB_LOG_AUTO_REMOVE, True)
        self.env.set_lg_max(1024**2)
        self.env.open(
            path,
            flags,
            0
        )

        # create vertices and edges k/v stores
        def new_store(name):
            flags = DB_CREATE
            elements = DB(self.env)
            elements.open(
                name,
                None,
                DB_BTREE,
                flags,
                0,
            )
            return elements

        self.index = new_store('index')
        self.tuples = new_store('tuples')

    def close(self):
        self.tuples.close()
        self.index.close()
        self.env.close()

    def ref(self, uid, key):
        cursor = self.tuples.cursor()
        query = pack(uid, key)
        record = cursor.set_range(query)
        if not record:
            cursor.close()
            return
        key, value = record
        if key == query:
            value = unpack(value)[0]
            cursor.close()
            return value
        else:
            cursor.close()
            return None

    def get(self, uid):
        cursor = self.tuples.cursor()

        def __get():
            record = cursor.set_range(pack(uid, ''))
            if not record:
                return
            key, value = record
            while True:
                other, key = unpack(key)
                if other == uid:
                    value = unpack(value)[0]
                    yield key, value
                    record = cursor.next()
                    if record:
                        key, value = record
                        continue
                    else:
                        break
                else:
                    break

        tuples = dict(__get())
        cursor.close()
        return tuples

    def add(self, uid, **properties):
        for key, value in properties.items():
            self.tuples.put(pack(uid, key), pack(value))
            self.index.put(pack(key, value, uid), '')

    def delete(self, uid):
        # delete item from main table and index
        cursor = self.tuples.cursor()
        index = self.index.cursor()
        record = cursor.set_range(pack(uid, ''))
        if record:
            key, value = record
        else:
            cursor.close()
            raise AjguDBException('not found')
        while True:
            other, key = unpack(key)
            if other == uid:
                # remove tuple from main index
                cursor.delete()

                # remove it from index
                value = unpack(value)[0]
                index.set(pack(key, value, uid))
                index.delete()

                # continue
                record = cursor.next()
                if record:
                    key, value = record
                    continue
                else:
                    break
            else:
                break
        cursor.close()

    def update(self, uid, **properties):
        self.delete(uid)
        self.add(uid, **properties)

    def debug(self):
        for key, value in self.tuples.items():
            uid, key = unpack(key)
            value = unpack(value)[0]
            print(uid, key, value)

    def query(self, key, value=''):
        cursor = self.index.cursor()
        match = (key, value) if value else (key,)

        record = cursor.set_range(pack(key, value))
        if not record:
            cursor.close()
            return

        while True:
            key, _ = record
            other = unpack(key)
            ok = reduce(
                lambda previous, x: (cmp(*x) == 0) and previous,
                zip(match, other),
                True
            )
            if ok:
                yield other
                record = cursor.next()
                if not record:
                    break
            else:
                break
        cursor.close()
