create table {0} (
	objectid        bigint,
	district        text,
	psa             text,
	date_time_occur timestamp,
	dc_number       bigint primary key,
	location        text,
	ucr             int,
	crime_type      text,
	x               double precision,
	y               double precision,
	geom            geometry
)