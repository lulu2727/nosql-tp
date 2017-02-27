

from datetime import datetime
from cassandra.cluster import Cluster
import csv
import dateparser

stations = [{'stationName' : 'auber', 'filePath' : 'data/qualite-de-lair-mesuree-dans-la-station-auber.csv'},
         {'stationName' : 'chatelet', 'filePath' : 'data/qualite-de-lair-mesuree-dans-la-station-chatelet.csv'},
         {'stationName' : 'franklin-d-roosevelt', 'filePath' : 'data/qualite-de-lair-mesuree-dans-la-station-franklin-d-roosevelt.csv'}]

def importStationData(station): #read data from a csv station file and export to a cassandra cluster.
    with open(station['filePath'], 'rb') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')

        for row in reader:
            try:
                datetime = dateparser.parse(row['DATE'] + ' ' + row['HEURE'])
                session.execute("""
                INSERT INTO measures (time, station, no, no2, pm10, co2, temp, humi)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (datetime.isoformat(), station['stationName'], convertToInt(row['NO']), convertToInt(row['NO2']), convertToInt(row['PM10']), convertToInt(row['CO2']), convertToInt(row['TEMP']), convertToInt(row['HUMI'])))
            except:
                pass

def convertToInt(value):
    try:
        return int(value)
    except ValueError:
        return None

#By default the cluster is only composed of localhost node.
cluster = Cluster()
#Connect to the cluster and use the keyspace air_data
session = cluster.connect('air_data')

# Import station data to Cassandra
for station in stations:
    importStationData(station)
