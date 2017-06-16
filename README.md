# OpenDataPhillyTools
A repository containing useful scripts that deal with data from [Open Data Philly](https://www.opendataphilly.org/).

## [PublicSafety](https://github.com/cfh294/OpenDataPhillyTools/tree/master/PublicSafety)
Tools here pertain to public safety data. This can be imported as a module in other python applications or scripts.

* [inct2pg](https://github.com/cfh294/OpenDataPhillyTools/blob/master/PublicSafety/inct2pg.py)
    - Takes a [PostgreSQL connection string](https://wiki.postgresql.org/wiki/Using_psycopg2_with_PostgreSQL#Connect_to_Postgres) and an optional schema name as inputs.
    - Uses curl to grab csv of the [crime incidents data](https://www.opendataphilly.org/dataset/crime-incidents) and puts it into a spatial table.
    - Requires [cURL](https://curl.haxx.se/), [PostgreSQL](https://www.postgresql.org/) (and its [PostGIS](http://www.postgis.net/) extension), and [psycopg2](http://initd.org/psycopg/) to run.
    - If input table already exists, the script will update it with the newest incidents.
    - Can now be imported into other scripts.
    - $user: python inct2pg.py "connection string" "schema.table"
* [crimeGeometry](https://github.com/cfh294/OpenDataPhillyTools/blob/master/PublicSafety/crimeGeometry.py)
    - Contains classes that pertain to police administration areas.
    - Basic [data abstraction](https://en.wikipedia.org/wiki/Abstraction_(software_engineering)).
    - Still in early development.
