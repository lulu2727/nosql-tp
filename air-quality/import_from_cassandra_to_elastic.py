#!/usr/bin/python

import sys
import dateparser
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch


if(len(sys.argv) < 5):
	print('Cassandra\'s hosts, Cassandra\'s port, Elasticsearch\'s hosts and ElasticSearch\'s port need to be provided as launch parameters : python your_path/import_cassandra.py cassandra_hosts cassandra_port elasticsearch_hosts elasticsearch_port')
else:
	cassandraHosts = sys.argv[1].split(',')
	cassandraPort = sys.argv[2]
	elasticsearchHosts = sys.argv[3].split(',')
	elasticsearchPort = sys.argv[4]
	
	cluster = Cluster(], port=sys.argv[2])
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
