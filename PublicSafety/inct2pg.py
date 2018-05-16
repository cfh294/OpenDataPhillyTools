"""
	inct2pg.py
	ETL script that loads latest Philadelphia crime incident data into a spatial PostgreSQL table

	command line usage:

	$usr: python inct2pg.py  connection_string  schema  table_name

	GNU License
	Code by Connor Hornibrook, 2017

	-- update 15 May 2018
	     Refactored entire program to utilize safer parameterized queries, as well as the argparse
	     library to simplify the code. The program now uses a helper class called PgHandler to
	     abstract out some database functionality that may be common among other future programs in
	     this repository (see OpenDataPhillyTools/classes/handlers.py).
	     The whole repository is now updated to be in Python 3!

"""
import psycopg2
import csv
import requests
import argparse
import sys
from classes import PgHandler

# constants
DDL_FILE = "sql/crime_incident.ddl"
ODP_TABLE = "incidents_part1_part2"
GEOM = "the_geom"
BASE_URL = "https://phl.carto.com/api/v2/sql?q={query}&filename=incidents_part1_part2&format=csv&skipfields=cartodb_id"
WGS_84 = 4326

# the keys are the ODP field names, the values are my preferred field names
PRETTY_NAMES = {
	"objectid":           "OBJECTID",
	"dc_dist":            "DISTRICT",
	"psa":                "PSA",
	"dispatch_date_time": "DATE_TIME_OCCUR",
	"dc_key":             "DC_NUMBER",
	"location_block":     "LOCATION",
	"ucr_general":        "UCR",
	"text_general_code":  "CRIME_TYPE"
}

ORDERED_FIELDS = ["dc_key", "dc_dist", "psa", "dispatch_date_time", "objectid",
                  "location_block", "ucr_general", "text_general_code"]
KEY_FIELD = "dc_number"


def get_arg_parser():
	"""
	:return: An ArgumentParser object for this program
	"""
	parser = argparse.ArgumentParser()
	parser.add_argument("connection", help="A PostgreSQL connection string")
	parser.add_argument("schema", help="The schema in which the incident table is located")
	parser.add_argument("table", help="The table name")
	return parser


def cleanup_connection(connection_object):
	"""
	Clears a connection object from memory
	:param connection_object: A psycopg2 connection object
	:return:
	"""
	connection_object.rollback()
	connection_object.close()
	del connection_object

if __name__ == "__main__":

	# grab the input arguments
	arg_parser = get_arg_parser()
	args = arg_parser.parse_args()

	# create the PostgreSQL connection object
	try:
		connection = psycopg2.connect(args.connection)
	except psycopg2.ProgrammingError:
		print("Invalid connection string, aborting.\nDone.")
		sys.exit(1)
	except psycopg2.OperationalError:
		print("Invalid credentials for given connection string, aborting.\nDone")
		sys.exit(1)

	# grab the table and schema names
	schema = args.schema
	table = args.table
	full_name = "{s}.{t}".format(s=schema, t=table)

	with PgHandler(connection) as pgis:

		# validate the schema
		if not pgis.schema_exists(schema):
			print("Schema does not exist, aborting.\nDone.")
			cleanup_connection(connection)
			sys.exit(1)

		# parse the URL query
		q = ", ".join([old + " as " + PRETTY_NAMES[old] for old in ORDERED_FIELDS])
		q += ", ST_Y({geometry}) as y, ST_X({geometry}) as x".format(geometry=GEOM)
		q += ", ST_AsText({geometry}) as geom".format(geometry=GEOM)
		q = "SELECT " + q + " FROM " + ODP_TABLE

		# check to see if the table already exists. if so, we only need the crimes past the
		# max date in the table
		if pgis.table_exists(table, schema):
			sql = "select max(date_time_occur) from {0}".format(full_name)
			max_date = pgis.execute_sql(sql, get_data=True, single_value=True)
			q += " where dispatch_date_time > '{0}'".format(max_date)
			formatted_date = max_date.strftime("%m/%d/%Y")
			print("Existing table found, records after {0} to be downloaded.".format(formatted_date))

		# create the table, if needed
		else:
			print("No table found, creating '{0}' in '{1}' schema.".format(table, schema))
			try:
				with open(DDL_FILE, "r") as ddl:
					sql = "\n".join([line.rstrip() for line in ddl.readlines()]).format(full_name)
					pgis.execute_sql(sql)
			except IOError:
				print("DDL file not found ('{0}'), aborting.\nDone.".format(DDL_FILE))

		# download the actual data using an HTTP request, convert it to a python list object
		data = requests.get(BASE_URL.format(query=q)).content.decode("utf-8")
		rows = list(csv.reader(data.splitlines(), delimiter=","))
		all_fields = rows.pop(0)

		num_fields = len(all_fields)
		geom_index = num_fields - 1
		wildcards = ["%s" for i in range(num_fields - 1)]

		# parse together parameterized sql for inserting values into the incidents table
		# this statement will always be the same, the parameters will just change
		geom_sql = ", ST_GeomFromText(%s, %s)"
		insert_sql = "insert into " + full_name + "(" + ", ".join(all_fields) + ")"
		values_sql = " values (" + ", ".join(wildcards) + geom_sql + ")"
		insert_sql += values_sql
		num_new = len(rows)

		if num_new > 0:
			print("Inserting {0} new records...".format(num_new))

			# loop through the downloaded data, treat the rows as parameter tuples for
			# parameterized sql statements with psycopg2
			for row in rows:

				# change empty values to nulls
				row = list(map(lambda value: None if value == "" else value, row))

				# append the srid to the row so it can be used as a parameter
				row.append(WGS_84)
				dc_number = row[0]

				# if we find a key conflict, simply update the record with the newer data
				try:
					pgis.execute_sql(insert_sql, params=row)
				except psycopg2.IntegrityError:
					connection.rollback()
					del_sql = "delete from " + full_name + " where " + KEY_FIELD + " = %(bad_key)s"
					pgis.execute_sql(del_sql, {"bad_key": dc_number})
					pgis.execute_sql(insert_sql, params=row)
		else:
			print("No new records found.")
		print("Done")

		# commit our changes
		connection.commit()

	cleanup_connection(connection)
