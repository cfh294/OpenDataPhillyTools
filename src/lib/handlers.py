class PgHandler(object):

	def __init__(self, connection):
		"""
		Constructor
		:param connection: a psycopg2 connection
		"""
		self.__connection = connection
		self.__cursor = connection.cursor()

	def schema_exists(self, schema):
		"""
		:param schema: The schema name being tested
		:return:       Whether or not the specified name exists within the current connection (boolean)
		"""
		sql = "select exists (" \
		      "select 1 from information_schema.tables " \
		      "where upper(table_schema) = %(schema_name)s" \
		      ")"
		params = {"schema_name": schema.upper()}
		return self.execute_sql(sql, params=params, get_data=True, single_value=True)

	def table_exists(self, table_name, schema_name="public"):
		"""
		:param table_name:  The table name being tested
		:param schema_name: The schema where the table can be found (defaults to "public")
		:return:            Whether or not the table exists within the current connection (boolean)
		"""
		sql = "select exists (" \
		      "select 1 from information_schema.tables " \
		      "where upper(table_name) = %(table_name)s " \
		      "and upper(table_schema) = %(schema_name)s" \
		      ")"
		params = {"table_name": table_name.upper(), "schema_name": schema_name.upper()}
		return self.execute_sql(sql, params=params, get_data=True, single_value=True)

	def execute_sql(self, sql, params=None, get_data=False, single_value=False):
		"""
		Wrapper method for executing sql statements, with or without parameters
		:param sql:          A sql statement
		:param params:       A parameter tuple or dict (optional, defaults to None)
		:param get_data:     Whether or not the user wishes to receive output/fetched data (boolean)
		:param single_value: Whether or not the user expects a single value returned from the query (boolean)
		:return:             The fetched result(s) of the execution, if wanted (list)
		"""

		try:
			if params is None:
				self.__cursor.execute(sql)
			else:
				self.__cursor.execute(sql, params)

			if get_data:
				if single_value:
					return self.__cursor.fetchone()[0]
				else:
					return self.__cursor.fetchall()

		except Exception as db_error:
			raise db_error

	def __enter__(self):
		"""
		Context management for entering "with" blocks
		:return: this instance of PgHandler
		"""
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		"""
		Context management for exiting "with" blocks
		"""
		self.__cursor.close()
		self.__cursor = None
