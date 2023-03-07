#!/usr/bin/env python3

import os
import re
import sys

import h3
import pandas as pd
import psycopg

SPECIES_RE = re.compile('.*res_(\d+)_7.csv')

try:
	filename = sys.argv[1]
except IndexError:
	print(f'usage: {sys.argv[0]} [CSV file]')
	sys.exit(-1)

ext = os.path.splitext(filename)[1]
if ext == '.parquet':
	df = pd.read_parquet(filename)
elif ext == '.csv':
	df = pd.read_csv(filename, index_col=False)
elif ext == '.hdf5':
	df = pd.read_hdf(filename)
else:
	print(f'unrecognised data type {ext}')
	sys.exit(-1)

species = SPECIES_RE.match(filename).groups()[0]
experiment = os.path.dirname(os.path.abspath(filename)).split(os.path.sep)[-1]

with psycopg.connect(host="localhost", password="mysecretpassword", user="postgres", dbname="postgres") as conn:

	# Open a cursor to perform database operations
	with conn.cursor() as cur:

		# Execute a command: this creates a new table
		cur.execute("""
			CREATE TABLE IF NOT EXISTS geotest (
				tile CHAR(16),
				species integer,
				centre geometry(POINT,4326),
				area real,
				experiment VARCHAR(32),
				UNIQUE(tile, species, experiment)
			);
		""")
		cur.execute("""
			CREATE INDEX IF NOT EXISTS tile_idx on geotest(tile);
		""")
		cur.execute("""
			CREATE INDEX IF NOT EXISTS species_idx on geotest(species);
		""")
		cur.execute("""
			CREATE INDEX IF NOT EXISTS experiment_idx on geotest(experiment);
		""")
		cur.execute("""
			CREATE INDEX IF NOT EXISTS centre_idx on geotest USING GIST(centre);
		""")

		with cur.copy("COPY geotest (tile, species, centre, area, experiment) FROM STDIN") as copy:
			for _, tileid, area in df.itertuples():
				if area == 0.0:
					continue
				lat, lng = h3.cell_to_latlng(tileid)
				copy.write_row((tileid, species, f'POINT({lng} {lat})', area, experiment))

		# Make the changes to the database persistent
		conn.commit()
