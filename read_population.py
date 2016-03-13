import gzip
import xml.sax
import numpy as np
import pathlib
import sys, sqlite3
import time

class PopulationReader(xml.sax.ContentHandler):
    def __init__(self, cursor):
        self.reset()

        self.display = time.time()
        self.count = 0

        self.cursor = cursor

    def reset(self):
        self.person = None
        self.selected = False
        self.first_leg = None

    def startElement(self, name, attributes):
        if name == 'person':
            self.person = attributes['id']
        elif name == 'plan' and attributes['selected'] == 'yes':
            self.selected = True
        elif name == 'leg' and self.selected:
            self.first_leg = attributes['mode']

    def endElement(self, name):
        if name == 'person':
            if self.first_leg is not None:
                self.cursor.execute('insert into _population (id, first_leg) values (?,?)', (self.person, self.first_leg))

            self.count += 1

            if self.display + 1.0 < time.time():
                print('   Read %d persons ...' % self.count)
                self.display = time.time()

            self.reset()

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('read_population.py source_xml database suffix')
        exit()

    source = pathlib.Path(sys.argv[1]).resolve()
    destination = pathlib.Path(sys.argv[2])
    suffix = sys.argv[3]

    print('Converting population from:')
    print('    %s' % source)
    print('')

    table = 'population_%s' % suffix

    print('Will write to table %s in:' % table)
    print('    %s ' % destination)
    print('')

    connection = sqlite3.connect(str(destination))
    cursor = connection.cursor()

    cursor.execute('create table _population (id text, first_leg text)')

    print('Reading population ...\n')

    reader = PopulationReader(cursor)

    if str(source)[-2:] == 'gz':
        with gzip.open(str(source)) as f:
            xml.sax.parse(f, reader)
    else:
        with open(str(source)) as f:
            xml.sax.parse(f, reader)

    connection.commit()
    print('\nFinished reading %d persons!\n' % reader.count)

    cursor.execute('alter table _population rename to %s' % table)
    connection.commit()

    connection.close()
