# OpenDataPhillyTools
A repository containing useful scripts that deal with data from [Open Data Philly](https://www.opendataphilly.org/).

## [Public Safety](https://github.com/cfh294/OpenDataPhillyTools/tree/master/Public%20%20Safety)
Tools here pertain to public safety data

* [inct_create.py](https://github.com/cfh294/OpenDataPhillyTools/blob/master/Public%20%20Safety/inct_create.py)
    - Takes a [PostgreSQL connection string](https://wiki.postgresql.org/wiki/Using_psycopg2_with_PostgreSQL#Connect_to_Postgres) and an optional schema name as inputs.
    - Uses curl to grab entire csv of the [crime incidents data](https://www.opendataphilly.org/dataset/crime-incidents) and puts it into a spatial table.
    - Requires [cURL](https://curl.haxx.se/), [PostgreSQL](https://www.postgresql.org/) (and its [PostGIS](http://www.postgis.net/) extension), and [psycopg2](http://initd.org/psycopg/) to run.
    - $user: python inct_create.py "connection string" "schema (optional)"
    - NOTE: This is a very large table; there are millions of records. The script will take a few minutes to run. Though it would work for both tasks, this script is really only meant for INITIALLY downloading this data into a database, not updating it.



