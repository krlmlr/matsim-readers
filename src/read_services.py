import gzip
import xml.sax
import numpy as np
import pathlib
import sys, sqlite3
import time

class ServiceReader(xml.sax.ContentHandler):
	REQUEST_ATTRIBUTES = (None, 'dropoffLinkId', 'passengerId', 'pickupLinkId', 'pickupTime', 'submissionTime')
	REQUEST_FIELDS = ('request_id', 'dropoff_link', 'passenger', 'pickup_link', 'pickup_time', 'submission_time')
	REQUEST_TYPES = ('integer primary key', 'text', 'text', 'text', 'real', 'real')

	SERVICE_ATTRIBUTES = (
		None, None, 'startTime', 'departureTime', 'pickupArrivalTime', 'passengerArrivalTime', 'pickupTime', 'pickupDepartureTime', 'dropoffArrivalTime', 'dropoffTime', 'endTime',
		'pickupDriveDistance', 'dropoffDriveDistance', 'startLinkId', 'driverAgent', 'passengerAgent')
	SERVICE_FIELDS = (
		'service_id', 'request_id', 'start_time', 'departure_time', 'pickup_arrival_time', 'passenger_arrival_time', 'pickup_time', 'pickup_departure_time', 'dropoff_arrival_time', 'dropoff_time', 'end_time',
		'pickup_distance', 'dropoff_distance', 'start_link', 'driver', 'passenger')
	SERVICE_TYPES = (
		'integer primary key', 'integer', 'real', 'real', 'real', 'real', 'real', 'real', 'real', 'real', 'real',
		'real', 'real', 'text', 'text', 'text')

	def __init__(self, cursor):
		self.cursor = cursor
		self.service = None
		self.request = None

		self.display = time.time()
		self.count = 0

	def startElement(self, name, attributes):
		if name == 'service':
			self.service = attributes
			self.request = None
		elif name == 'request':
			self.request = attributes

	def endElement(self, name):
		if name == 'service':
			fields = [field for field, attr in zip(ServiceReader.REQUEST_FIELDS, ServiceReader.REQUEST_ATTRIBUTES) if attr is not None]
			values = ['?'] * len(fields)

			fields, values = ','.join(fields), ','.join(values)
			data = [self.request[attr] for attr in ServiceReader.REQUEST_ATTRIBUTES if attr is not None]

			query = 'insert into _requests (%s) values (%s)' % (fields, values)
			self.cursor.execute(query, data)

			fields = [field for field, attr in zip(ServiceReader.SERVICE_FIELDS, ServiceReader.SERVICE_ATTRIBUTES) if attr is not None]
			values = ['?'] * (len(fields) + 1)
			fields.append('request_id')

			fields, values = ','.join(fields), ','.join(values)
			data = [self.service[attr] for attr in ServiceReader.SERVICE_ATTRIBUTES if attr is not None]
			data.append(self.cursor.lastrowid)

			query = 'insert into _services (%s) values (%s)' % (fields, values)
			self.cursor.execute(query, data)
			self.count += 1

			if self.display + 1.0 < time.time():
				print('   Read %d services ...' % self.count)
				self.display = time.time()

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('read_services.py source_xml database suffix')
        exit()

    source = pathlib.Path(sys.argv[1]).resolve()
    destination = sys.argv[2]
    suffix = sys.argv[3]

    print('Converting services from:')
    print('    %s' % source)
    print('')

    print('Will write to:')
    print('    %s' % destination)
    print('')

    connection = sqlite3.connect(str(destination))
    cursor = connection.cursor()

    fields = ','.join(['%s %s' % (field, ftype) for field, ftype in zip(ServiceReader.SERVICE_FIELDS, ServiceReader.SERVICE_TYPES)])
    cursor.execute('create table _services (%s)' % fields)

    fields = ','.join(['%s %s' % (field, ftype) for field, ftype in zip(ServiceReader.REQUEST_FIELDS, ServiceReader.REQUEST_TYPES)])
    cursor.execute('create table _requests (%s)' % fields)

    print('Reading services ...\n')

    reader = ServiceReader(cursor)
    with gzip.open(str(source)) as f:
        xml.sax.parse(f, reader)

    connection.commit()
    print('\nFinished reading %d services!\n' % reader.count)
    connection.commit()

    requests_table = 'requests_%s' % suffix
    services_table = 'services_%s' % suffix

    cursor.execute('alter table _requests rename to %s' % requests_table)
    cursor.execute('alter table _services rename to %s' % services_table)

    connection.commit()
    connection.close()
