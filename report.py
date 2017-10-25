#!/usr/bin/python

import pprint
import sqlite3
import argparse
import datetime

pp = pprint.PrettyPrinter(indent=4)

args_parse = argparse.ArgumentParser(description="Get those ARPs!")
args_parse.add_argument('-d', '--dbfile', dest='dbfile', help='The database file to read the collected data from.')
args_parse.add_argument('-r', '--report', dest='report_type', help='The type of report to run.  Available options are: agent-summary.')
args = args_parse.parse_args()

def execute_atomic_int_query(sql):
	conn = sqlite3.connect(args.dbfile)
	with conn:
		cur = conn.cursor()
		cur.execute(sql)
		conn.commit()
		result = cur.fetchone()
		my_int = 0
		if 'tuple' in str(type(result)):
			my_int = result[0]
		elif 'int' in str(type(rsult)):
			my_int = result
		elif 'NoneType' in str(type(result)):
			my_int = False
		else:
			raise TypeError("Unexpected SQL query result type: " + str(type(result)) + ".")
		return my_int

def execute_single_row_query(sql):
	conn = sqlite3.connect(args.dbfile)
	with conn:
		cur = conn.cursor()
		cur.execute(sql)
		conn.commit()
		result = cur.fetchone()
