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
import struct

from msgpack import dumps
from msgpack import loads


def pack(*values):
    def __pack(value):
        if type(value) is int:
            return '1' + struct.pack('>q', value)
        elif type(value) is unicode:
            return '2' + value.encode('utf-8') + '\0'
        elif type(value) is str:
            return '3' + value + '\0'
        else:
            data = dumps(value, encoding='utf-8')
            return '4' + struct.pack('>q', len(data)) + data
    return ''.join(map(__pack, values))


def unpack(packed):
    kind = packed[0]
    if kind == '1':
        value = struct.unpack('>q', packed[1:9])[0]
        packed = packed[9:]
    elif kind == '2':
        index = packed.index('\0')
        value = packed[1:index].decode('utf-8')
        packed = packed[index+1:]
    elif kind == '3':
        index = packed.index('\0')
        value = packed[1:index]
        packed = packed[index+1:]
    else:
        size = struct.unpack('>q', packed[1:9])[0]
        value = loads(packed[9:9+size], encoding='utf-8')
        packed = packed[size+9:]
    if packed:
        values = unpack(packed)
        values.insert(0, value)
    else:
        values = [value]
    return values
