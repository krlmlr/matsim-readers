import gzip
import xml.sax
import numpy as np
import pathlib
import sys, sqlite3
import time

class EventsReader(xml.sax.ContentHandler):
	ATTRIBUTES = ('event_id', 'time', 'type', 'person', 'link', 'actType', 'vehicle', 'legMode')
	TYPES = ('integer primary key', 'real', 'text', 'text', 'text', 'text', 'text', 'text')

	def __init__(self, cursor):
		self.cursor = cursor
		self.display = time.time()
		self.count = 0

	def get_values(self, attributes):
		values = []
		for attr in EventsReader.ATTRIBUTES:
			values.append(attributes[attr] if attr in attributes else None)
		return values

	def make_type_fields():
		return ', '.join([
			'%s %s' % (name, type)
			for type, name in zip(EventsReader.TYPES, EventsReader.ATTRIBUTES)])

	def make_fields():
		return ', '.join(EventsReader.ATTRIBUTES)

	def make_values():
		return ', '.join(['?'] * len(EventsReader.ATTRIBUTES))

	def startElement(self, name, attributes):
		if not name == 'event': return
		if not attributes['type'] in ['actstart', 'actend', 'arrival', 'departure', 'AVDispatchModeChange']: return

		fields = EventsReader.make_fields()
		values = EventsReader.make_values()

		query = 'insert into _events (%s) values (%s)' % (fields, values)
		self.cursor.execute(query, self.get_values(attributes))
		self.count += 1

		if self.display + 1.0 < time.time():
			print('   Read %d events ...' % self.count)
			self.display = time.time()

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('read_events.py source_xml database suffix')

    source = pathlib.Path(sys.argv[1]).resolve()
    destination = sys.argv[2]
    suffix = sys.argv[3]

    print('Converting events from:')
    print('    %s' % source)
    print('')

    print('Will write to:')
    print('    %s' % destination)
    print('')

    connection = sqlite3.connect(str(destination))
    cursor = connection.cursor()

    cursor.execute('create table _events (%s)' % EventsReader.make_type_fields())
    cursor.execute("""
    	create table _activities (
    		activity_id integer primary key,
            person text,
    		start_id integer,
    		end_id integer,
    		start_time real,
    		end_time real,
    		link text,
    		act_type text)""")

    print('Reading events ...\n')

    reader = EventsReader(cursor)
    with gzip.open(str(source)) as f:
    	xml.sax.parse(f, reader)

    connection.commit()

    print('\nFinished reading %d events!\n' % reader.count)

    cursor.execute('select min(time) from _events')
    sim_start_time = cursor.fetchone()[0]

    cursor.execute('select max(time) from _events')
    sim_end_time = cursor.fetchone()[0]

    print('Simulation start time: %f' % sim_start_time)
    print('Simulation end time: %f\n' % sim_end_time)

    print('Indexing activities ...')

    currentActivity = {}
    writecursor = connection.cursor()

    fixstart = 0
    fixend = 0

    display = time.time()
    count = 0

    for row in cursor.execute('select * from _events where type in ("actstart","actend") order by time asc'):
    	event_id, etime, type, person, link, act_type = row[:6]

    	if type == 'actstart':
    		currentActivity[person] = (event_id, etime, link, act_type)
    	elif type == 'actend':
    		start_id, start_time = None, sim_start_time

    		if person in currentActivity:
    			start_id, start_time, link, act_type = currentActivity[person]
    			del currentActivity[person]
    		else:
    			fixstart += 1

    		writecursor.execute('insert into _activities (person, start_id, end_id, start_time, end_time, link, act_type) values (?,?,?,?,?,?,?)',
    			(person, start_id, event_id, start_time, etime, link, act_type))

    	count += 1
    	if display + 1.0 < time.time():
    		print('    Processed %d acivities ...' % count)
    		display = time.time()

    for person, activity in currentActivity.items():
    	start_id, start_time, link, act_type = activity
    	fixend += 1
    	writecursor.execute('insert into _activities (person, start_id, end_id, start_time, end_time, link, act_type) values (?,?,?,?,?,?,?)',
    		(person, start_id, None, start_time, sim_end_time, link, act_type))

    connection.commit()

    print('\nFixed %d activity start times' % fixstart)
    print('Fixed %d activity end times\n\n' % fixend)

    print('Reading legs from the database...')

    result = cursor.execute("""
        create table _legs as
        select
            de.person,
            de.time as departure_time,
            min(ae.time) as arrival_time,
            de.legMode as mode,
            de.link as departure_link,
            ae.link as arrival_link
        from _events as de
        left join _events as ae on de.person = ae.person
        where
            de.type = "departure" and ae.type = "arrival" and
            ae.legMode = de.legMode and
            ae.time >= de.time and
            de.person not glob '*[a-zA-Z]*'
        group by de.event_id
    """)

    print('Done!')
    connection.commit()

    tables = ['events', 'activities', 'legs']

    for table in tables:
        cursor.execute('alter table _%s rename to %s_%s' % (table, table, suffix))

    connection.commit()
    connection.close()
