import gzip
import xml.sax
import sys, sqlite3
import time
import functools

class TransparentDecompressionStream:
	@staticmethod
	def make(file_name):
		if file_name.endswith('.gz'):
			stream = gzip.open(file_name)
		else:
			stream = open(file_name)

		return stream

class EventsReader(xml.sax.ContentHandler):
	ATTRIBUTES = ('event_id', 'time', 'type', 'link', 'vehicle', 'legMode')
	TYPES = ('integer primary key', 'real', 'text', 'text', 'text', 'text')

	def __init__(self, cursor):
		self.cursor = cursor
		self.display = time.time()
		self.count = 0

	def get_values(self, attributes):
		values = []
		for attr in EventsReader.ATTRIBUTES:
			values.append(attributes[attr] if attr in attributes else None)
		return values

	@staticmethod
	def make_type_fields():
		return ', '.join([
			'%s %s' % (name, type_)
			for type_, name in zip(EventsReader.TYPES, EventsReader.ATTRIBUTES)])

	@staticmethod
	def make_fields():
		return ', '.join(EventsReader.ATTRIBUTES)

	@staticmethod
	def make_values():
		return ', '.join(['?'] * len(EventsReader.ATTRIBUTES))

	@staticmethod
	@functools.lru_cache(maxsize=1)
	def make_query():
		fields = EventsReader.make_fields()
		values = EventsReader.make_values()

		query = 'insert into _events (%s) values (%s)' % (fields, values)
		return query

	def startElement(self, name, attributes):
		if not name == 'event': return
		if not attributes['type'] in ['entered link']: return

		self.cursor.execute(EventsReader.make_query(), self.get_values(attributes))
		self.count += 1

		if (self.count % 1000) == 0:
			if self.display + 1.0 < time.time():
				print('   Read %d events ...' % self.count)
				self.display = time.time()

if __name__ == '__main__':
	if len(sys.argv) < 4:
		print('read_events.py source_xml database suffix')

	source = sys.argv[1]
	destination = sys.argv[2]
	suffix = sys.argv[3]

	print('Converting events from:')
	print('	%s' % source)
	print('')

	print('Will write to:')
	print('	%s' % destination)
	print('')

	connection = sqlite3.connect(str(destination))  # @UndefinedVariable
	cursor = connection.cursor()

	cursor.execute('create table _events (%s)' % EventsReader.make_type_fields())

	print('Reading events ...\n')

	reader = EventsReader(cursor)
	with TransparentDecompressionStream.make(str(source)) as f:
		xml.sax.parse(f, reader)

	connection.commit()

	print('\nFinished reading %d events!\n' % reader.count)

	cursor.execute('create index time on _events (time)')
	cursor.execute('create index link on _events (link)')

	print('\nFinished creating indexes!\n')

	cursor.execute('select min(time), max(time) from _events')
	[sim_start_time, sim_end_time] = cursor.fetchone()

	print('Simulation start time: %f' % sim_start_time)
	print('Simulation end time: %f\n' % sim_end_time)

	tables = ['events']

	for table in tables:
		cursor.execute('alter table _%s rename to %s_%s' % (table, table, suffix))

	connection.commit()
	connection.close()
