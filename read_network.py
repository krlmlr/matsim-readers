import gzip
import xml.sax
import numpy as np
import pathlib
import sys, sqlite3
import time

class NetworkReader(xml.sax.ContentHandler):
    def __init__(self, cursor):
        self.cursor = cursor

        self.display = time.time()
        self.nodecount = 0
        self.linkcount = 0

    def startElement(self, name, attributes):
        if name == 'node':
            id, x, y = attributes['id'], attributes['x'], attributes['y']
            self.cursor.execute('insert into _nodes (id, x, y) values (?,?,?)', (id, x, y))
            self.nodecount += 1

        elif name == 'link':
            id, from_, to = attributes['id'], attributes['from'], attributes['to']
            self.cursor.execute('insert into _links (id, from_id, to_id) values (?,?,?)', (id, from_, to))
            self.linkcount += 1

        if self.display + 1.0 < time.time():
            print('   Read %d nodes and %d links  ...' % (self.nodecount, self.linkcount))
            self.display = time.time()

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('read_network.py source_xml database')
        exit()

    source = pathlib.Path(sys.argv[1]).resolve()
    destination = sys.argv[2]

    print('Converting network from:')
    print('    %s' % source)
    print('')

    print('Will write to:')
    print('    %s' % destination)
    print('')

    connection = sqlite3.connect(str(destination))
    cursor = connection.cursor()

    cursor.execute("""
        create table _nodes (
            id text,
            x real,
            y real)""")

    cursor.execute("""
        create table _links (
            id text,
            from_id text,
            to_id text)""")

    print('Reading network ...\n')

    reader = NetworkReader(cursor)
    with gzip.open(str(source)) as f:
        xml.sax.parse(f, reader)

    connection.commit()

    cursor.execute('alter table _nodes rename to nodes')
    cursor.execute('alter table _links rename to links')

    connection.commit()

    print('\nFinished reading network (%d nodes, %d links)!\n' % (reader.nodecount, reader.linkcount))
    connection.close()
