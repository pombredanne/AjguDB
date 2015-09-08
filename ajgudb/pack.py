from msgpack import loads
from msgpack import dumps


def pack(value):
    return dumps(value, encoding='utf-8')


def unpack(value):
    return loads(value, encoding='utf-8')
