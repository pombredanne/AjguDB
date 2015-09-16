from msgpack import Unpacker

from ajgudb import gremlin


example = {
    u'id': u'/e/ff75ac33f095b9b2ee53ea6630e1a80be7e0a1b0',
    u'start': u'/c/af/afghanistan',
    u'rel': u'/r/Antonym',
    u'surfaceText': None,
    u'end': u'/c/af/pakistan',
    u'context': u'/ctx/all',

    u'source_uri': u'/and/[/s/rule/synonym_section/,/s/web/de.wiktionary.org/wiki/Afghanistan/]',
    u'uri': u'/a/[/r/Antonym/,/c/af/afghanistan/,/c/af/pakistan/]',
    u'dataset': u'/d/wiktionary/de/af',
    u'license': u'/l/CC/By-SA',

    u'weight': 1.0,
    u'sources': [
        u'/s/rule/synonym_section',
        u'/s/web/de.wiktionary.org/wiki/Afghanistan'
    ],
    u'features': [
        u'/c/af/afghanistan /r/Antonym -',
        u'/c/af/afghanistan - /c/af/pakistan',
        u'- /r/Antonym /c/af/pakistan'
    ]
}


def get_or_create(graphdb, label, key, value):
    try:
        return gremlin.query(
            gremlin.select_vertices(**dict(key=value)),
            gremlin.limit(1),
            gremlin.get
        )(graphdb)[0]
    except IndexError:
        return graphdb.vertex.create(label, **dict(key=value))


def load():
    from ajgudb import AjguDB
    print('build database at db')
    db = AjguDB('./db/')

    # add index on name
    db.vertex.index('name')

    for index in range(8):
        name = 'data/assertions/part_0{}.msgpack'.format(index)
        with open(name, 'rb') as stream:
            print(name)
            unpacker = Unpacker(stream, encoding='utf-8')
            for value in unpacker:
                start = value.pop('start').encode('utf-8')
                end = value.pop('end').encode('utf-8')
                # only import english concepts
                if not (start.startswith('/c/en/')
                        and end.startswith('/c/en/')):
                    continue
                start = get_or_create(db, 'concept/item', 'name', start)
                end = get_or_create(db, 'concept/item', 'name', end)

                # do not include the following keys
                value.pop(u'sources')
                value.pop(u'features')
                value.pop(u'source_uri')
                value.pop(u'dataset')
                value.pop(u'license')
                value.pop(u'uri')
                value.pop(u'id')

                # convert unicode keys into byte/str keys
                value = {key.encode('utf-8'): value[key] for key in value.keys()}  # noqa
                start.link('concept/relation', end,  **value)
    db.close()


if __name__ == '__main__':
    load()
