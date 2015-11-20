# AjuDB - wiredtiger powered graph database
# Copyright (C) 2015 Amirouche Boubekki <amirouche@hypermove.net>
from msgpack import loads
from msgpack import dumps


def pack(value):
    return dumps(value, encoding='utf-8')


def unpack(value):
    return loads(value, encoding='utf-8')


class AjguDBException(Exception):
    pass
