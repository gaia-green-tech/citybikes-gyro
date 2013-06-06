from __future__ import absolute_import
from redis import Redis, ConnectionPool
from rq import Queue
from rq_scheduler import Scheduler
from datetime import datetime
import pybikes
from gyro.configuration import db_credentials as credentials
from gyro.configuration import redis_server
from pymongo import Connection
from gyro.models import StationDocument, SystemDocument, Stat, StatDocument

connection = Connection(credentials['host'], credentials['port'])
db = getattr(connection, credentials['database'])
pool = ConnectionPool(host=redis_server['host'],port=redis_server['port'],db=0)
redis_connection = Redis(connection_pool = pool)

q_medium = Queue('medium', connection = redis_connection)
q_high = Queue('high', connection = redis_connection)

scheduler = Scheduler('medium_sched', connection = redis_connection)
scheduler_high = Scheduler('high_sched', connection = redis_connection)

def syncSystem(scheme, system):
    sys = pybikes.getBikeShareSystem(scheme, system)
    sysDoc = SystemDocument(db, connection, scheme, sys)
    sysDoc.save()
    syncStations(sys, True)

def syncStation(station_chunk, tag, resync = False):
    print "Processing chunk"
    for station in station_chunk:
        station.update()
        sDoc = StationDocument(db, connection, station, tag)
        if resync or sDoc.find({'_id': station.get_hash()}).count() == 0:
            # Save this unsynched station
            print "Saving STATION %s.%s" % (tag, station.get_hash())
            sDoc.save()

        # Update the stat...
        print "Adding STAT for %s.%s" % (tag, station.get_hash())
        stat = Stat(station)
        statDoc = StatDocument(db, connection, stat)
        statDoc.save()

def syncStations(system, resync = False):
    system.update()
    #Async stations in parallel...
    print "Generating chunks..."
    chunks = [system.stations[i:i+10] for i in range(0, len(system.stations), 10)]
    print "%d chunks!" % len(chunks)
    for station_chunk in chunks:
        if system.sync:
            syncStation(station_chunk, system.tag, resync)
        else:
            scheduler_high.schedule(
                scheduled_time = datetime.now(),
                func = syncStation,
                args = (station_chunk, system.tag, resync,),
                interval = 300,
                repeat = None
            )

def updateSystem(scheme, system):
    instance = pybikes.getBikeShareSystem(scheme, system)
    if instance.sync:
        scheduler.schedule(
                scheduled_time = datetime.now(),
                func = syncStations,
                args = (instance, False,),
                interval = 60,
                repeat = None
        )
    else:
        q_medium.enqueue_call(func = syncStations,
                        args = (instance, False,),
                        timeout = 240)
