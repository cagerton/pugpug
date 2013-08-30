#!/usr/bin/env python3.3
"""
Pugpug - Database schema migration helper for Postgres.

Pugpug is uses sha1s of the schema of each table to keep track of versions.
Pugpug keeps state in the ./pugpug/ directory along with all migration sql
and snapshots at each step.

Usage:
	pugpug.py <db> init [--force]
	pugpug.py <db> check
	pugpug.py <db> add <FILE> <comment>...
	pugpug.py <db> migrate [<slug>]
	pugpug.py <db> history
	pugpug.py <db> show [<slug>]
"""

# TODO: support for parallel workflow migrations. Also, deleting tables and merging migrations.

import subprocess
import hashlib
import re
import os, sys
import yaml
from datetime import datetime
from docopt import docopt
from slugify import slugify


# Ansi color codes. TODO: extract or use a real library 
color_names = ('black', 'red', 'green','yellow','blue','magenta','cyan','white')
attr_names = ('reset','bright','dim','underline','blink','reverse','hidden')
fg_codes = dict(zip(color_names, range(30,38)))
bg_codes = dict(zip(color_names, range(40,48)))
attr_codes = dict(zip(attr_names, range(0,9)))
def color_seq(attr='reset',fg=None,bg=None):
	if bg is None and fg is None:
		return "\x1B[%dm" % attr_codes[attr]
	elif bg is None:
		return "\x1B[%d;%dm" % (attr_codes[attr], fg_codes[fg])
	return "\x1B[%d;%d;%dm" % (attr_codes[attr], fg_codes[fg], bg_codes[bg])


string_sha = lambda s:hashlib.sha1(s.encode('utf8')).hexdigest()

PG_DUMP_PATH = '/usr/pgsql-9.2/bin/pg_dump'
PSQL_PATH = 'psql'
EMPTYSHA = string_sha("")

SequenceFile  = 'pugpug/sequence.yml'
MigrateFiles  = 'pugpug/sql/%(slug)s.sql'
TableFiles    = 'pugpug/table_data/%(table)s.yml'


class PugPugPG(object):
	"""
	Helps dump the structure of postgres databases.
	Assumes that the current user is already authorized to read the db.
	"""
	def __init__(self, db):
		self.db = db

	def list_tables(self):
		raw = subprocess.check_output((PG_DUMP_PATH, '-xs',self.db,)).decode('utf8')
		create_matches = [ re.match("^CREATE TABLE (\w+)",line) for line in raw.split("\n")]
		tables = [ m.group(1) for m in create_matches if m ]
		return tables

	def show_create(self, table):
		raw = subprocess.check_output((PG_DUMP_PATH, '-xs',self.db,'-t',table)).decode('utf8')
		lines = [ line for line in raw.split("\n") if not re.match("^(\s*--.*|\s*)$",line) ]
		cmds = [ line for line in lines if not re.match("^SET .*;", line)]
		return "\n".join(cmds)

	def show_full_create(self):
		raw = subprocess.check_output((PG_DUMP_PATH, '-xs',self.db)).decode('utf8')
		lines = [ line for line in raw.split("\n") if not re.match("^(\s*--.*|\s*)$",line) ]
		cmds = [ line for line in lines if not re.match("^SET .*;", line)]
		return "\n".join(cmds)

	def db_summary(self):
		return dict([(t, self.table_summary(t),) for t in self.list_tables()])

	def table_summary(self, table):
		sql = self.show_create(table)
		return dict(sql=sql, sha=string_sha(sql))

	def run_sql(self, filename):
		subprocess.check_call((PSQL_PATH, self.db, '-f', filename))


class PugPugState(object):
	"""Manages information about tables and migrations using YAML & SQL files."""

	def init_from_zero(self, force=False):
		if not os.path.exists('pugpug/sql'):
			os.makedirs('pugpug/sql')
			os.makedirs('pugpug/table_data')
		if os.path.exists(SequenceFile) and not force:
			raise Exception("Cowardly refusing to blow away data without --force.")
		self.table_transforms = {}
		self.simple_starts = {}
		self.tables = {}
		self.seen = set()
		self.seq = {}

	def load_all(self):
		with open(SequenceFile, 'r') as f:
			self.seq = yaml.safe_load(f)

		self.seen = set()
		self.table_transforms = {}
		self.simple_starts = {}

		# get table names and start-sha -> migration mappings
		for slug, mig in self.seq.items():
			self.index_migration(slug, mig)

		# and load the table snapshots.
		self.tables = {}
		for table in self.table_transforms:
			with open(TableFiles % dict(table=table), 'r') as f:
				self.tables[table] = yaml.safe_load(f)

	def save_all(self):
		with open(SequenceFile, 'w') as f:
			self.dump_sorted_yaml(self.seq ,f)
		for table in self.tables:
			with open(TableFiles % dict(table=table), 'w') as f:
				self.dump_sorted_yaml(self.tables[table] ,f)

	def index_migration(self, slug, mig):
		"""Keep migration hints around."""
		start_shas, end_shas = mig['start_shas'], mig['end_shas']
		self.simple_starts[self.snap_sha(start_shas)] = slug
		for table in set(start_shas.keys())|set(end_shas.keys()):
			if table not in self.table_transforms:
				self.table_transforms[table] = {}
			start_sha, end_sha = start_shas.get(table, EMPTYSHA), end_shas.get(table, EMPTYSHA)
			self.seen.add(start_sha)
			self.seen.add(end_sha)
			if(start_sha != end_sha):
				if start_sha in self.table_transforms[table]:
					print("Warning: multiple transforms for %s:%s" % (table,start_sha))
					print(" TODO: add better support for this.")
				self.table_transforms[table][start_sha] = slug

	def snap_to_shas(self, snap):
		"""Remove full sql from dictionaries for table snapshots."""
		return dict([(t,info['sha']) for t,info in snap.items()])

	def add_migration(self, slug, comment, sql, start_snap, end_snap):
		mig = {
			'comment':comment,
			'start_shas': self.snap_to_shas(start_snap),
			'end_shas': self.snap_to_shas(end_snap),
			'sql_sha':string_sha(sql),
		}
		self.seq[slug] = mig
		self.index_migration(slug, mig)
		self.update_table_snaps(end_snap)

	def update_table_snaps(self, db_snapshot):
		for name in db_snapshot:
			if name not in self.tables:
				self.tables[name] = {}
			table = self.tables[name]
			table_snap = db_snapshot[name]
			sha = table_snap['sha']
			if sha not in table:
				table[sha] = table_snap['sql']

	def dump_sorted_yaml(self, dictionary, stream):
		for k in sorted(dictionary.keys()):
			yaml.safe_dump({k: dictionary[k]}, stream, default_flow_style=False)

	def check_validity(self, slug, db_snap, postrun=False):
		"""Return true iff table to be changed has the same start state."""
		db_shas = self.snap_to_shas(db_snap)
		mig = self.seq[slug]
		names = set(mig['start_shas'].keys())|set(mig['end_shas'].keys())
		changes = []
		bad_tables = []
		for name in names:
			start = mig['start_shas'].get(name,EMPTYSHA)
			end   = mig['end_shas'].get(name,EMPTYSHA)
			if(start != end):
				# dealing with changed only now.
				if start != db_shas.get(name,EMPTYSHA) and not postrun:
					bad_tables.append(name)
				elif postrun and end != db_shas.get(name,EMPTYSHA):
					bad_tables.append(name)
		if len(bad_tables):
			return bad_tables
		return None

	def get_file(self, slug):
		return MigrateFiles % dict(slug=slug)

	def snap_sha(self, db_snap):
		table_defs = ["%s=%s" % (k, db_snap[k]) for k in sorted(db_snap.keys())]
		return string_sha("&".join(table_defs))

	def is_up_to_date(self, db_snap):
		query_sha = self.snap_sha(self.snap_to_shas(db_snap))
		mig_key = max(self.seq.keys())
		return query_sha == self.snap_sha(self.seq[mig_key]['end_shas'])

	def find_next_migration_simple(self, db_snap):
		"""Finds most recent migration start state identical to db_snap."""
		query_sha = self.snap_sha(self.snap_to_shas(db_snap))
		return self.simple_starts.get(query_sha,None)

	def get_slugs(self):
		return sorted(self.seq.keys())

	def find_next_migration_advanced(self, db_snap):
		db_shas = self.snap_to_shas(db_snap)
		tables = set(db_snap.keys())|set(self.tables.keys())
		table_shas = dict([(t,db_shas.get(t,EMPTYSHA)) for t in tables])

		error = []
		untracked = []
		up_to_date = []
		next_migs = {}

		for table in tables:
			table_sha = db_shas.get(table, EMPTYSHA)
			next_sha = self.table_transforms.get(table,{}).get(table_sha, None)

			if next_sha is None:
				if table_sha in self.seen:
					up_to_date.append(table)
				else:
					if(table in self.tables):
						args = (color_seq(fg='red'),table,color_seq(), table_sha)
						print("ERROR unknown state: %s%s%s: %s" % args)
					else:
						args = (color_seq(fg='red'),table,color_seq())
						print("ERROR unknown table: %s%s%s" % args)
					error.append(table)
			else:
				if next_sha not in next_migs:
					next_migs[next_sha] = []
				next_migs[next_sha].append(table)

		for m, tables in next_migs.items():
			ts = ", ".join(tables)
			args = dict(m=m, blue=color_seq(fg='blue'),reset=color_seq(), ts=ts, y=color_seq(fg='yellow'))
			bad_tables = self.check_validity(m, db_snap)
			if not bad_tables:
				print("Migration: %(blue)s%(m)s%(reset)s for tables: %(blue)s%(ts)s%(reset)s" % args)
			else:
				args['bad']=", ".join(bad_tables)
				print("Can't run migration: %(y)s%(m)s%(reset)s for tables: %(y)s%(ts)s%(reset)s" % args)
				print("		        tables in violation: %(y)s%(bad)s%(reset)s" % args)
			
		if len(next_migs):
			return next_migs


class PugPug(object):
	def __init__(self, db):
		self.db = db
		self.pg = PugPugPG(db)
		self.state = PugPugState()

	def mk_slug(self, comment):
		dt = datetime.now().strftime("%Y-%M-%d_%H-%M-%S-")
		return slugify(dt+comment[0:40])

	def init(self, force=False):
		comment = "Initial Leap."
		slug = self.mk_slug(comment)
		sql = self.pg.show_full_create()

		db_empty = dict()
		db_snap = self.pg.db_summary()

		self.state.init_from_zero(force)
		self.state.add_migration(slug, comment, sql, db_empty, db_snap) 
		self.state.save_all()

	def add(self, filename, comment):
		slug = self.mk_slug(comment)
		new_file = self.state.get_file(slug)

		# Copy it over to a safe spot.
		with open(filename,'r') as src_file:
			with open(new_file, 'w') as dst_file:
				sql = src_file.read()
				dst_file.write(sql)

		self.state.load_all()
		db_start = self.pg.db_summary()
		try:
			self.pg.run_sql(new_file)
		except:
			print("Failure. Cleaning up.")
			os.remove(new_file)
			return False

		db_end = self.pg.db_summary()
		self.state.add_migration(slug, comment, sql, db_start, db_end) 
		self.state.save_all()


	def migrate(self, slug=None):
		self.state.load_all()
		start_snap = self.pg.db_summary()
		if not slug:
			slug = self.state.find_next_migration_simple(start_snap)
		if not slug:
			print("Next migration isn't clean/obvious. Run check then specify slug.")
			return

		bad_tables = self.state.check_validity(slug, start_snap)
		if(bad_tables):
			print("Can't run this! due to :",bad_tables)
			return
		filename = self.state.get_file(slug)
		self.pg.run_sql(filename)
		end_snap = self.pg.db_summary()

		bad_tables = self.state.check_validity(slug, end_snap, postrun=True)
		if(bad_tables):
			print("Run Failed! Might have fucked up:", bad_tables)

	def check(self):
		db_snap = self.pg.db_summary()
		self.state.load_all()

		if self.state.is_up_to_date(db_snap):
			print(color_seq(fg='green')+"Up to date."+color_seq())
			return None
		else:
			next = self.state.find_next_migration_simple(db_snap)
			if(next):
				args = dict(n=next, blue=color_seq(fg='blue'),reset=color_seq(),)
				print("Next migration (simple): %(blue)s%(n)s%(reset)s" % args)
				return next
			next = self.state.find_next_migration_advanced(db_snap)
			if(next):
				print("Non-linearizable migrations.")
				return next
			print("No matching migrations found.")
			return False

	def history(self):
		self.state.load_all()
		slugs = self.state.get_slugs()
		print("Showing known migrations:")
		for slug in self.state.get_slugs():
			print(" * %s%s%s" % (color_seq(fg='blue',attr='bright'),slug,color_seq(),))

	def show(self, slug=None):
		if not slug:
			self.state.load_all()
			slug = self.state.get_slugs()[-1]
		print("Showing %s%s%s" % (color_seq(fg='blue',attr='bright'),slug,color_seq(),))
		#print("-- File: ",self.state.get_file(slug))
		subprocess.check_call(('cat', self.state.get_file(slug)))
		print("\n\n")

if __name__ == "__main__":
	args = docopt(__doc__, version='Project.py concept.')
	m = PugPug(args['<db>'])
	
	if args['init']:
		m.init(args['--force'])
	elif args['check']:
		if(not m.check()):
			sys.exit(1)
	elif args['add']:
		comment = " ".join(args['<comment>'])
		m.add(args['<FILE>'], comment=comment)
	elif args['migrate']:
		m.migrate(slug=args['<slug>'])
	elif args['history']:
		m.history()
	elif args['show']:
		m.show(slug=args['<slug>'])
	else:
		print("whatever.")

