# OpenDataPhillyTools
A repository containing useful scripts that deal with data from [Open Data Philly](https://www.opendataphilly.org/).

## [PublicSafety](https://github.com/cfh294/OpenDataPhillyTools/tree/master/PublicSafety)
Tools here pertain to public safety data. This can be imported as a module in other python applications or scripts.

* [inct2pg](https://github.com/cfh294/OpenDataPhillyTools/blob/master/PublicSafety/inct2pg.py)
    - Written in Python 3.6.5
    - Takes a [PostgreSQL connection string](https://wiki.postgresql.org/wiki/Using_psycopg2_with_PostgreSQL#Connect_to_Postgres), a schema name, and a table name as inputs.
    - Uses HTTP request to grab csv of the [crime incidents data](https://www.opendataphilly.org/dataset/crime-incidents) and puts it into a spatial table.
    - Requires [PostgreSQL](https://www.postgresql.org/) (and its [PostGIS](http://www.postgis.net/) extension), and [psycopg2](http://initd.org/psycopg/) to run.
    - If input table already exists, the script will update it with the newest incidents.
    - $user: python inct2pg.py "connection string" "schema" "table"

