#!/usr/bin/python

import sys
import dateparser
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch


if(len(sys.argv) < 5):
	print('A Cassandra host, Cassandra\'s port, Elasticsearch\s host and ElasticSearch\'s port need to be provided as launch parameters : python your_path/import_cassandra.py cassandra_host cassandra_port elasticsearch_host elasticsearch_port')
else:
	cluster = Cluster([sys.argv[1]], port=sys.argv[2])
	session = cluster.connect('air_data')
	es = Elasticsearch(hosts=[{'host':sys.argv[3], 'port':sys.argv[4]}])

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
