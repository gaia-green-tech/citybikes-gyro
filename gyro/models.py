# -*- coding: utf-8 -*-
# Copyright (C) 2010-2012, eskerda <eskerda@gmail.com>
# Distributed under the AGPL license, see LICENSE.txt

class Document(object):
    
    __collection__ = None
    data = {}

    def __init__(self, db, connection, *args, **kwargs):
        self.collection = getattr(db, self.__collection__)
        self.connection = connection
        self.db = db
        if len(args) > 0:
            self.__load__(*args, **kwargs)

    def __getattr__(self, attr):
        if attr in self.data:
            return self.data[attr]
        else:
            err = '\'%s\' object has no attribute \'%s\'' % (self.__class__.__name__, attr)
            raise AttributeError(err)

    def __load__(self, *args, **kwargs):
        pass

    def save(self, safe=True, *args, **kwargs):
        """
        Known issue:
        OperationFailure: Can't extract geo keys: { _id: "d54b248ebd3f4644b88b353749a0b053", network_id: "citycycle", extra: { status: "OPEN", uid: 122, bonus: false, last_update: 1568721438000, banking: true, address: "Lower River Tce / Ellis St", slots: 16 }, location: { type: "Point", coordinates: [ -27.482279, 153.028723 ] }, name: "122 - LOWER RIVER TCE / ELLIS ST" }  longitude/latitude is out of bounds, lng: -27.4823 lat: 153.029
        """
        return self.collection.save(self.data, safe, *args, **kwargs)

    def read(self, id):
        self.data = self.collection.find_one({'_id': id})

    def find(self, *args, **kwargs):
        return self.collection.find(*args, **kwargs)

class Stat(object):
    def __init__(self, station):
        self.station_id = station.get_hash()
        self.bikes = station.bikes
        self.free = station.free
        self.timestamp = station.timestamp
        self.extra = station.extra

class StatDocument(Document):
    __collection__ = 'station_stats'

    def __load__(self, stat):

        self.data = {
            'station_id': stat.station_id,
            'bikes': stat.bikes,
            'free': stat.free,
            'timestamp': stat.timestamp,
            'extra': stat.extra
        }
    def save(self, safe = True, *args, **kwargs):
        # Get last stat from this station document
        stationDoc = StationDocument(self.db, self.connection)
        stationDoc.read(self.station_id)
        if 'last_stat' in stationDoc.data:
            last_stat = stationDoc.data['last_stat']
        else:
            last_stat = None
        # Create a new stat entry if it differs from the last one
        if last_stat is None or \
	       last_stat['bikes'] != self.data['bikes'] or \
           last_stat['free'] != self.data['free']:
                super( StatDocument, self).save(safe, *args, **kwargs)
        # Update last_stat on station relation.
        stationDoc.data['last_stat'] = self.data
        stationDoc.save()



class StationDocument(Document):
    __collection__ = 'stations'

    def __load__(self, station = None, network_id = None, _id = None):
        if (station is not None and network_id is not None):
            self.data = {
                '_id': station.get_hash(),
                'location': {
                    'type': 'Point',
                    'coordinates': [
                        station.latitude,
                        station.longitude
                    ]
                },
                'name': station.name,
                'network_id': network_id,
                'extra': station.extra
            }
        if _id is not None:
            self.read(_id)

class SystemDocument(Document):
    __collection__ = 'systems'

    def __load__(self, schema, system):
        self.data = {
            '_id': system.tag,
            'schema': schema 
        }
        self.data = dict(self.data, ** system.meta)

