"""
	inct_create.py

	Uses url to download entire incidents table from Open Data Philly, and put it into a spatial PostGIS table.
	This script takes a mandatory postgresql connection string input, and an optional schema name input

	example cmd line:

	$usr: python odp_inct_create.py "dbname=<database_name> host=<server> user=<user> password=<password> port=<port>"

		OR

	$usr: python odp_inct_create.py "dbname=<database_name> host=<server> user=<user> password=<password> port=<port>"
		  "schema"

	GNU License
	Code by Connor Hornibrook, 2017
"""
import os
import psycopg2
import csv
import sys
from subprocess import call, STDOUT

URL = "https://phl.carto.com/api/v2/sql?q=SELECT+*,+ST_Y(the_geom)+AS+lat,+ST_X(the_geom)+AS+lng+FROM+incidents_" \
      + "part1_part2&filename=incidents_part1_part2&format=csv&skipfields=cartodb_id"

FIELDS = {
	"objectid": "BIGINT",
	"dc_dist": "TEXT",
	"psa": "TEXT",
	"dispatch_date_time": "TIMESTAMP",
	"dc_key": "BIGINT PRIMARY KEY",
	"location_block": "TEXT",
	"ucr_general": "INT",
	"text_general_code": "TEXT",
	"lng": "DOUBLE PRECISION",
	"lat": "DOUBLE PRECISION"
}

# cleaner fields, used in the database table
NEW_FIELDS = {
	"objectid": "OBJECTID",
	"dc_dist": "DISTRICT",
	"psa": "PSA",
	"dispatch_date_time": "DATE_TIME_OCCUR",
	"dc_key": "DC_NUMBER",
	"location_block": "LOCATION",
	"ucr_general": "UCR",
	"text_general_code": "CRIME_TYPE",
	"lng": "X",
	"lat": "Y"
}

BLANK_VALS = [None, "", " "]

if __name__ == "__main__":

	# validate and retrieve script arguments
	numArgs = len(sys.argv)
	if numArgs < 2:
		print "No arguments supplied!"
		sys.exit()
	elif numArgs > 3:
		print "Too many arguments supplied!"
		sys.exit()

	cnxnString = sys.argv[1]
	schema = "public"
	if numArgs > 2:
		schema = sys.argv[2]

	# parse needed strings
	tableName = "{}.{}".format(schema, "ODP_CRIME_INCIDENTS")
	outCSV = os.path.join(os.getcwd(), "incidents.csv")

	# set up db connection
	cnxn = None
	try:
		cnxn = psycopg2.connect(cnxnString)
	except psycopg2.OperationalError:
		print "Invalid connection string!"
		exit()
	cursor = cnxn.cursor()

	# parse the sql for creating the table
	sql = (
		"DROP TABLE IF EXISTS {0};\nCREATE TABLE {0}\n\t(".format(tableName)
	)
	for field, fieldType in FIELDS.iteritems():
		sql += "\n\t{fName} {fType} NOT NULL,".format(fName=NEW_FIELDS[field], fType=fieldType)
	sql = sql[:-1] + "\n);"

	try:
		cursor.execute(sql)
	except psycopg2.ProgrammingError:
		print "Error with following query:\n\n{}".format(sql)
		exit()

	with open(os.devnull, 'w') as devnull:
		call(["curl", "-o", outCSV, URL], stdout=devnull, stderr=STDOUT)

	with open(outCSV, "rb") as csvfile:
		dr = csv.DictReader(csvfile)
		for row in dr:
			insertSQL = "INSERT INTO {0} (".format(tableName)
			valueSQL = "VALUES ("
			for field, fieldType in FIELDS.iteritems():
				insertSQL += "{}, ".format(NEW_FIELDS[field])
				value = row[field]

				if fieldType in ["BIGINT", "INT"] and value in BLANK_VALS:
					value = 0
				elif fieldType in ["DOUBLE PRECISION"] and value in BLANK_VALS:
					value = 0.0
				elif fieldType in ["TEXT", "TIMESTAMP"] and value is None:
					value = ""

				# quote-ify and escape out apostrophes if needed
				value = "'{}'".format(value.replace("'", "''")) if fieldType in ["TEXT", "TIMESTAMP"] else value
				valueSQL += "{}, ".format(value)

			insertSQL, valueSQL = insertSQL[:-2] + ")", valueSQL[:-2] + ")"
			sql = "{} {};".format(insertSQL, valueSQL)

			try:
				cursor.execute(sql)
			except psycopg2.ProgrammingError:
				print "Error with following query:\n\n{}".format(sql)
				exit()

	os.remove(outCSV)

	# create the geometry field and calculate it
	sql = "ALTER TABLE {0} ADD COLUMN geom_3857 geometry;".format(tableName)
	sql += "\nUPDATE {0} SET geom_3857 = ST_Transform(ST_SetSRID(ST_Point(x, y), 4326), 3857);".format(tableName)
	sql += "\nALTER TABLE {0} DROP COLUMN X, DROP COLUMN Y;".format(tableName)

	try:
		cursor.execute(sql)
	except psycopg2.ProgrammingError:
		print "Error with following query:\n\n{}".format(sql)
		exit()

	cnxn.commit()
	cnxn.close()
	del cursor, cnxn
