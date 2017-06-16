"""
	inct2pg.py

	Uses url to download entire incidents table from Open Data Philly, and put it into a spatial PostGIS table.
	This script takes a mandatory postgresql connection string input, and an optional schema name input

	example cmd line:

	$usr: python odp_inct_create.py "dbname=<database_name> host=<server> user=<user> password=<password> port=<port>"
		  "schema.table_name"

	GNU License
	Code by Connor Hornibrook, 2017
"""

INIT_URL = "https://phl.carto.com/api/v2/sql?q=SELECT+objectid+AS+OBJECTID,+dc_dist+AS+DISTRICT,+psa+AS+PSA," \
           "+dispatch_date_time+AS+DATE_TIME_OCCUR,+dc_key+AS+DC_NUMBER,+location_block+AS+LOCATION," \
           "+ucr_general+AS+UCR,+text_general_code+AS+CRIME_TYPE,+ST_Y(the_geom)+AS+Y,+ST_X(the_geom)+" \
           "AS+X+FROM+incidents_part1_part2&filename=incidents_part1_part2&format=csv&skipfields=cartodb_id"

UPDATE_URL = "https://phl.carto.com/api/v2/sql?q=SELECT+objectid+AS+OBJECTID,+dc_dist+AS+DISTRICT,+psa+AS+PSA," \
             "+dispatch_date_time+AS+DATE_TIME_OCCUR,+dc_key+AS+DC_NUMBER,+location_block+AS+LOCATION," \
             "+ucr_general+AS+UCR,+text_general_code+AS+CRIME_TYPE,+ST_Y(the_geom)+AS+Y,+ST_X(the_geom)+" \
             "AS+X+FROM+incidents_part1_part2+WHERE+dispatch_date_time+%3E+{0}&filename=incidents_part1_part2" \
             "&format=csv&skipfields=cartodb_id"

URL_ENCODING = {
	" ": "%20",
	"-": "%2D",
	":": "%3A",
	"'": "%27"
}

FIELDS = {
	"objectid": "BIGINT",
	"district": "TEXT",
	"psa": "TEXT",
	"date_time_occur": "TIMESTAMP",
	"dc_number": "BIGINT PRIMARY KEY",
	"location": "TEXT",
	"ucr": "INT",
	"crime_type": "TEXT",
	"x": "DOUBLE PRECISION",
	"y": "DOUBLE PRECISION"
}

FIELDS_IN_ORDER = ["objectid", "dc_number", "district", "psa", "location", "date_time_occur", "crime_type", "ucr",
                   "x", "y"]

# this regex validates postgresql schema.table names
DB_TABLE_REGEX = r"[_a-zA-Z]+([_a-zA-Z]*\d*)*[.][_a-zA-Z]+([_a-zA-Z]*\d*)*"

BLANK_VALS = [None, "", " "]


def dateEncoding(date, decode=False):
	""" Encodes a date using HTML URL encoding. """
	newDate = date if type(date) is str else str(date)

	for old, encoded in URL_ENCODING.iteritems():
		if decode:
			newDate = newDate.replace(encoded, old)
		else:
			newDate = newDate.replace(old, encoded)
	return newDate


def inct2db(argList):
	""" Called in main, made separate in order to make the function importable. """

	# do imports inside function to avoid namespace errors
	import os
	import psycopg2
	import csv
	import sys
	import re
	from subprocess import call, STDOUT

	# Looks strange to do it this way, but it was a quick way to move this out of the main method. I
	# am going more fitting validation soon...
	# validate and retrieve script arguments
	numArgs = len(argList)
	if numArgs < 3:
		print "Not enough arguments supplied!"
		sys.exit()
	elif numArgs > 3:
		print "Too many arguments supplied!"
		sys.exit()

	cnxnString = argList[1]
	tableName = argList[2]

	if re.match(DB_TABLE_REGEX, tableName) is None:
		print "Invalid PostgreSQL schema.table name format!"
		sys.exit()

	# parse needed strings
	outCSV = os.path.join(os.getcwd(), "incidents.csv")

	# set up db connection
	cnxn = None
	try:
		cnxn = psycopg2.connect(cnxnString)
	except psycopg2.OperationalError:
		print "Invalid connection string!"
		sys.exit()
	cursor = cnxn.cursor()
	print "Connected to database."

	downloadURL = INIT_URL
	isUpdate, maxDate = False, ""
	sql = "SELECT * FROM {0} LIMIT 1;".format(tableName)
	try:
		cursor.execute(sql)  # will raise ProgrammingError if this is an initial import

		# if not, craft an update URL
		print "Existing table detected, running update."
		sql = "SELECT MAX(DATE_TIME_OCCUR) FROM {0};".format(tableName)
		cursor.execute(sql)
		maxDate = dateEncoding("'" + str(cursor.fetchone()[0]) + "'")
		downloadURL = UPDATE_URL.format(maxDate)
		isUpdate = True

	except psycopg2.ProgrammingError:

		print "No existing table detected, running initial incidents import for '{}'.".format(tableName)
		# parse the sql for creating the table
		sql = (
			"CREATE TABLE {0}\n\t(".format(tableName)
		)
		for field in FIELDS_IN_ORDER:
			fieldType = FIELDS[field]
			nullStatus = " NOT NULL" if field == "dc_number" else ""
			sql += "\n\t{fieldName} {fieldType}{null},".format(fieldName=field, fieldType=fieldType, null=nullStatus)
		sql = sql[:-1] + "\n);"

		try:
			cursor.execute(sql)
		except psycopg2.ProgrammingError:
			print "Error with following query:\n\n{}".format(sql)
			sys.exit()
		except psycopg2.InternalError as ie:
			cursor.execute("rollback;")
			cursor.execute(sql)

		sql = "ALTER TABLE {0} ADD COLUMN geom_3857 geometry;".format(tableName)
		try:
			cursor.execute(sql)
		except psycopg2.InternalError:
			print "Error with following query:\n\n{}".format(sql)
			sys.exit()

	print "Downloading newest data..."
	with open(os.devnull, 'w') as devnull:
		call(["curl", "-o", outCSV, downloadURL], stdout=devnull, stderr=STDOUT)
	print "\tFinished"

	print "Inserting values into '{}'...".format(tableName)
	with open(outCSV, "rb") as csvfile:
		dr = csv.DictReader(csvfile)
		for row in dr:
			insertSQL = "INSERT INTO {0} (".format(tableName)
			valueSQL = "VALUES ("
			lng, lat = 0.0, 0.0
			for field in FIELDS_IN_ORDER:
				fieldType = FIELDS[field]
				insertSQL += "{}, ".format(field)
				value = row[field]

				if field == "x" and value not in BLANK_VALS:
					lng = value
				elif field == "y" and value not in BLANK_VALS:
					lat = value

				if field == "date_time_occur" and "+00" in value:
					value = value.replace("+00", "")  # quick fix, better solution coming later

				if fieldType in ["BIGINT", "INT"] and value in BLANK_VALS:
					value = 0
				elif fieldType in ["DOUBLE PRECISION"] and value in BLANK_VALS:
					value = 0.0
				elif fieldType in ["TEXT", "TIMESTAMP"] and value is None:
					value = ""

				# quote-ify and escape out apostrophes if needed
				value = "'{}'".format(value.replace("'", "''")) if fieldType in ["TEXT", "TIMESTAMP"] else value
				valueSQL += "{}, ".format(value)

			# add geometry
			insertSQL, valueSQL = insertSQL[:-2] + ")", valueSQL[:-2] + ")"
			sql = "{} {};".format(insertSQL, valueSQL)
			try:
				cursor.execute(sql)
			except psycopg2.ProgrammingError as p:
				print "Error with following query:\n\n{}".format(sql)
				sys.exit()
	print "Finished."

	condition = " WHERE date_time_occur > {}".format(
		dateEncoding(maxDate, decode=True)) if isUpdate else ""

	sql = "UPDATE {0} SET geom_3857 = ST_Transform(ST_SetSRID(ST_Point(x, y), 4326), 3857){1};".format(tableName,
	                                                                                                   condition)
	try:
		print "Creating geom..."
		cursor.execute(sql)
		print "Finished."
	except psycopg2.ProgrammingError as p:
		print "Error with following query:\n\n{}".format(sql)
		sys.exit()

	print "Removing csv..."
	os.remove(outCSV)
	print "Finished."

	cnxn.commit()
	cnxn.close()
	del cursor, cnxn


if __name__ == "__main__":
	import sys
	inct2db(sys.argv)
