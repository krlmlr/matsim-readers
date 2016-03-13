import gzip
import xml.sax
import numpy as np
import pathlib
import sys, sqlite3
import time

class LinkEventsReader(xml.sax.ContentHandler):
    def __init__(self, cursor):
        self.cursor = cursor
        self.display = time.time()

        self.count = 0
        self.inconsistent = [0, 0, 0]

        self.vehicles = {}

    def startElement(self, name, attributes):
        if not 'type' in attributes: return
        type = attributes['type']
        if not type in ['entered link', 'left link']: return

        type = attributes['type']
        timestamp = attributes['time']
        vehicle = attributes['vehicle']
        link = attributes['link']

        if self.display + 1.0 < time.time():
            print('   Read %d link times (%d inconsistent) ...' % (self.count, sum(self.inconsistent)))
            self.display = time.time()

        if type == 'entered link':
            if vehicle in self.vehicles:
                self.inconsistent[0] += 1
                return

            self.vehicles[vehicle] = (link, timestamp)

        elif type == 'left link':
            if not vehicle in self.vehicles:
                # This is not really an inconsistency, it just means the vehicle was parked
                # self.inconsistent[1] += 1
                return

            start_link, start_time = self.vehicles[vehicle]
            if start_link != link:
                self.inconsistent[2] += 1
                del self.vehicles[vehicle]
                return

            cursor.execute('insert into _linktimes (link, enter_time, leave_time) values (?,?,?)', (start_link, start_time, timestamp))
            self.count += 1
            del self.vehicles[vehicle]

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('read_events.py source_xml database suffix')

    source = pathlib.Path(sys.argv[1]).resolve()
    destination = sys.argv[2]
    suffix = sys.argv[3]

    print('Converting link times from:')
    print('    %s' % source)
    print('')

    print('Will write to:')
    print('    %s' % destination)
    print('')

    connection = sqlite3.connect(str(destination))
    cursor = connection.cursor()

    cursor.execute("""
        create table _linktimes (
            link text,
            enter_time real,
            leave_time real)""")

    print('Reading link times ...\n')

    reader = LinkEventsReader(cursor)
    with gzip.open(str(source)) as f:
        xml.sax.parse(f, reader)

    cursor.execute('alter table _linktimes rename to link_times_%s' % suffix)
    connection.commit()

    print('Inconsistencies: ', reader.inconsistent)
    print('\nFinished reading %d link times!\n' % reader.count)
    connection.close()
