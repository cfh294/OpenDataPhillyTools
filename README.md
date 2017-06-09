# OpenDataPhillyTools
A repository containing useful scripts that deal with data from [Open Data Philly](https://www.opendataphilly.org/).

## Public Safety
Tools here pertain to public safety data

* inct_create.py
    - Takes a PostgreSQL connection string (and an optional schema name) as input.
    - Uses curl to grab entire csv of the [crime incidents data](https://www.opendataphilly.org/dataset/crime-incidents) and puts it into a spatial table.
    - Requires curl, [PostgreSQL](https://www.postgresql.org/) (and its [PostGIS](http://www.postgis.net/) extension), and [psycopg2](http://initd.org/psycopg/) to run.
    - $user: python inct_create.py "connection string" "schema (optional)"



