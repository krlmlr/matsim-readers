import gzip
import xml.sax
import numpy as np
import pathlib
import sys, sqlite3
import time

class DistancesReader(xml.sax.ContentHandler):
    def __init__(self, cursor):
        self.reset()

        self.display = time.time()
        self.count = 0

        self.cursor = cursor

    def reset(self):
        self.person = None
        self.selected = False
        self.leg = None
        self.route = None

    def startElement(self, name, attributes):
        if name == 'person':
            self.person = attributes['id']
        elif name == 'plan' and attributes['selected'] == 'yes':
            self.selected = True
        elif name == 'leg' and self.selected:
            self.leg = (
                    attributes['mode'],
                    float(np.dot(np.array([int(d) for d in attributes['dep_time'].split(':')]), np.array([3600, 60, 1]))),
                    float(np.dot(np.array([int(d) for d in attributes['arr_time'].split(':')]), np.array([3600, 60, 1])))
                )
        elif name == 'route' and self.leg is not None:
            self.route = attributes['distance']

    def endElement(self, name):
        if name == 'leg' and self.leg is None:
            print('Leg without a route... are you using the experienced plans file?')
            self.leg = None
        elif name == 'leg':
            mode, departure_time, arrival_time = self.leg
            distance = self.route

            self.cursor.execute('insert into _distances (person, mode, departure_time, arrival_time, distance) values (?,?,?,?,?)', (self.person, mode, departure_time, arrival_time, distance))

            self.count += 1
            if self.display + 1.0 < time.time():
                print('   Read %d distances ...' % self.count)
                self.display = time.time()

            self.leg = None
            self.route = None
        elif name == 'person':
            self.reset()

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('read_distances.py source_xml suffix')
        exit()

    source = pathlib.Path(sys.argv[1]).resolve()
    destination = pathlib.Path(sys.argv[2])
    suffix = sys.argv[3]

    print('Converting distnaces from:')
    print('    %s' % source)
    print('')

    table = 'distances_%s' % suffix

    print('Will write to table %s in:' % table)
    print('    %s ' % destination)
    print('')

    connection = sqlite3.connect(str(destination))
    cursor = connection.cursor()

    cursor.execute('create table _distances (person text, mode text, departure_time real, arrival_time real, distance real)')

    print('Reading distnaces ...\n')

    reader = DistancesReader(cursor)

    if str(source)[-2:] == 'gz':
        with gzip.open(str(source)) as f:
            xml.sax.parse(f, reader)
    else:
        with open(str(source)) as f:
            xml.sax.parse(f, reader)

    connection.commit()
    print('\nFinished reading %d distances!\n' % reader.count)

    cursor.execute('alter table _distances rename to %s' % table)
    connection.commit()

    connection.close()
