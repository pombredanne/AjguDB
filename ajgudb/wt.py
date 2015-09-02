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
from contextlib import contextmanager

from wiredtiger import wiredtiger_open

from packing import pack
from packing import unpack


WT_NOT_FOUND = -31803


class WiredTigerStorage(object):
    """Generic database"""

    def __init__(self, path):
        self.wiredtiger = wiredtiger_open(path, 'create')
        self.session = self.wiredtiger.open_session()
        self.session.create(
            'table:tuples',
            'key_format=QS,value_format=u,columns=(i,k,v)'
        )
        self.session.create('index:tuples:index', 'columns=(k,v,i)')
        self._index_cursors = list()
        self._tuples_cursors = list()

    @contextmanager
    def tuples(self):
        if self._tuples_cursors:
            cursor = self._tuples_cursors.pop()
        else:
            cursor = self.session.open_cursor('table:tuples')
        try:
            yield cursor
        finally:
            cursor.reset()
            self._tuples_cursors.append(cursor)

    @contextmanager
    def index(self):
        if self._index_cursors:
            cursor = self._index_cursors.pop()
        else:
            cursor = self.session.open_cursor('index:tuples:index')
        try:
            yield cursor
        finally:
            cursor.reset()
            self._index_cursors.append(cursor)

    def close(self):
        self.wiredtiger.close()

    def ref(self, uid, key):
        with self.tuples() as cursor:
            cursor.set_key(uid, key)
            if cursor.search() != WT_NOT_FOUND:
                value = cursor.get_value()
                value = unpack(value)[0]
                return value

    def get(self, uid):
        def __get():
            with self.tuples() as cursor:
                cursor.set_key(uid, '')
                code = cursor.search_near()
                if code == WT_NOT_FOUND:
                    return
                if code == -1:
                    cursor.next()
                while True:
                    other, key = cursor.get_key()
                    value = cursor.get_value()
                    if other == uid:
                        value = unpack(value)[0]
                        yield key, value
                        if cursor.next() == WT_NOT_FOUND:
                            break
                    else:
                        break
        tuples = dict(__get())
        return tuples

    def add(self, uid, **properties):
        with self.tuples() as cursor:
            for key, value in properties.items():
                cursor.set_key(uid, key)
                cursor.set_value(pack(value))
                cursor.insert()

    def delete(self, uid):
        with self.tuples() as cursor:
            cursor.set_key(uid, '')
            if cursor.search_near() == WT_NOT_FOUND:
                return
            while True:
                other, key = cursor.get_key()
                if other == uid:
                    cursor.remove()
                    if cursor.next() == WT_NOT_FOUND:
                        break
                else:
                    break

    def update(self, uid, **properties):
        self.delete(uid)
        self.add(uid, **properties)

    def query(self, key, value=''):
        with self.index() as cursor:
            match = (key, value) if value else (key,)

            cursor.set_key(key, pack(value), 0)
            code = cursor.search_near()
            if code == WT_NOT_FOUND:
                return
            if code == -1:
                cursor.next()

            while True:
                key, value, uid = cursor.get_key()
                value = unpack(value)[0]
                other = (key, value)
                ok = reduce(
                    lambda previous, x: (cmp(*x) == 0) and previous,
                    zip(match, other),
                    True
                )
                if ok:
                    yield [key, value, uid]
                    if cursor.next() == WT_NOT_FOUND:
                        break
                else:
                    break
