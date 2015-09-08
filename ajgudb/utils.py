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
from msgpack import loads
from msgpack import dumps


def pack(value):
    return dumps(value, encoding='utf-8')


def unpack(value):
    return loads(value, encoding='utf-8')


class AjguDBException(Exception):
    pass


class CloseableIterator(object):

    def __init__(self, iterator, close):
        self.iterator = iterator
        self.close = close

    def __iter__(self):
        return self.iterator

    def next(self):
        return next(self.iterator)
