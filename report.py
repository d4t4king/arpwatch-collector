#!/usr/bin/python

import time
import pprint
import sqlite3
import argparse
import datetime

pp = pprint.PrettyPrinter(indent=4)

args_parse = argparse.ArgumentParser(description="Get those ARPs!")
args_parse.add_argument('-d', '--dbfile', dest='dbfile', default='/var/lib/arpwatch/collection.db', help='The database file to read the collected data from.')
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
		elif 'int' in str(type(result)):
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
		result = cur.fetchone()
	return result

def execute_multi_row_query(sql):
	conn = sqlite3.connect(args.dbfile)
	with conn:
		cur = conn.cursor()
		cur.execute(sql)
		results = cur.fetchall()
	return results

def get_oldest(what):
    if what == 'agent':
        agents = execute_multi_row_query('SELECT DISTINCT id FROM agents')
        oldest_d = {}
        oldest_i = 0
        oldest_date = 0
        for agnt in agents:
            #pp.pprint(agnt)            # returns a list of tuples (1,)
            dateint = execute_atomic_int_query("SELECT first_pull_date FROM agents WHERE id='{0}'".format(agnt[0]))
            if oldest_date == 0 or oldest_date > dateint:
                oldest_date = dateint
                oldest_i = agnt[0]
    else:
        raise Exception("Unrecognized entity ({0})".format(what))
    old_obj = execute_single_row_query("SELECT id,ipaddr,fqdn FROM agents WHERE id='{0}'".format(oldest_i))
    oldest_d[oldest_date] = old_obj
    return oldest_d

def get_newest(what):
    if what == 'agent':
        agents = execute_multi_row_query('SELECT DISTINCT id FROM agents')
        newest_d = {}
        newest_i = 0
        newest_date = 0
        for agnt in agents:
            dateint = execute_atomic_int_query("SELECT first_pull_date FROM agents WHERE id='{0}'".format(agnt[0]))
            if newest_date == 0 or newest_date < dateint:
                newest_date = dateint
                newest_i = agnt[0]
    else:
        raise Exception("Unrecognized entity ({0})".format(what))
    new_obj = execute_single_row_query("SELECT id,ipaddr,fqdn FROM agents WHERE id='{0}'".format(newest_i))
    newest_d[newest_i] = new_obj
    return newest_d

def main():
    agents_mac_count = {}
    if args.report_type == 'agent-summary':
        agent_ids = execute_multi_row_query('SELECT id FROM agents')
        for id in agent_ids:
            count = execute_atomic_int_query("SELECT COUNT(DISTINCT mac_id) FROM agents_macs WHERE agent_id='{0}'".format(id[0]))
            agnt = execute_single_row_query("SELECT ipaddr,fqdn FROM AGENTS WHERE id='{0}'".format(id[0]))
            #pp.pprint(agnt)
            if agnt[1] and not agnt[1] == 'None':
                agents_mac_count[agnt[1]] = count
            else:
                agents_mac_count[agnt[0]] = count
        #pp.pprint(agents_mac_count)
        
        print "========================================================================"
        print "| %5d agents in database. %43s|" % (len(agent_ids), " ")
        print "========================================================================"
        print "| %27s | %14s %23s |" % ("Agent IP/FQDN", "MAC Addr Count", " ")
        print "========================================================================"
        for key, val in sorted(agents_mac_count.iteritems(), key=lambda (k,v): (v,k), reverse=True):
            print "| %27s | %5d %33s|" % (key, val, " ")
        print "========================================================================"
        oldest = get_oldest('agent')
        dateint = oldest.keys()[0]
        if oldest[dateint][2] and not oldest[dateint][2] == 'None':
            print "| %17s | %-49s|" % ("Oldest agent: ", oldest[dateint][2])
        else:
            print "| %17s | %-15s %30s|" % ("Oldest agent: ", oldest[dateint][1], " ")
        print "| %17s | %-25s %23s|" % ("First seen: ", time.strftime('%m/%d/%Y %H:%M:%S', time.localtime(dateint)), " ")
        last_updated = execute_atomic_int_query("SELECT last_update FROM agents WHERE id='{0}'".format(oldest[dateint][0]))
        print "| %17s | %-25s %23s|" % ("Last updated: ", time.strftime('%m/%d/%Y %H:%M:%S', time.localtime(last_updated)), " ")
        print '------------------------------------------------------------------------'
        newest = get_newest('agent')
        dateint = newest.keys()[0]
        if newest[dateint][2] and not newest[dateint][2] == 'None':
            print "| %17s | %-49s|" % ("Newest agent: ", newest[dateint][2])
        else:
            print "| %17s | %-49s|" % ("Newest agent: ", newest[dateint][1])
        print "| %17s | %-25s %23s|" % ("First seen: ", time.strftime('%m/%d/%Y %H:%M:%S', time.localtime(dateint)), "")
        last_updated = execute_atomic_int_query("SELECT last_update FROM agents WHERE id='{0}'".format(newest[dateint][0]))
        print "| %17s | %-25s %23s|" % ("Last updated: ", time.strftime('%m/%d/%Y %H:%M:%S', time.localtime(last_updated)), " ")
        print "========================================================================"
    else:
        raise Exception("Unrecognized report type! ({0})".format(args.report_type))

if __name__ == '__main__':
    main()
