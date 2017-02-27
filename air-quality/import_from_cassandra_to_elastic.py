from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch
import dateparser


cluster = Cluster()
session = cluster.connect('air_data')
es = Elasticsearch()

rows = session.execute('SELECT * FROM measures')
for row in rows:
    measure = {
    'timestamp': row.time,
    'station': row.station,
    'no': row.no,
    'no2': row.no2,
    'tempature': row.temp,
    'humidity': row.humi,
    'co2': row.co2,
    'pm10': row.pm10
    }
    res = es.index(index='air-' + row.station, doc_type='measure', body=measure)
