#!/usr/bin/python

import time
import pprint
import sqlite3
import argparse
import datetime

import sqliteUtils

pp = pprint.PrettyPrinter(indent=4)

args_parse = argparse.ArgumentParser(description="Get those ARPs!")
args_parse.add_argument('-d', '--dbfile', dest='dbfile', default='/var/lib/arpwatch/collection.db', \
	help='The database file to read the collected data from.')
args_parse.add_argument('-r', '--report', dest='report_type', required=True, \
	help='The type of report to run.  Available options are: agent-summary.')
args = args_parse.parse_args()

def get_oldest(what):
    oldest_d = {}
    old_obj = {}
    oldest_i = 0
    oldest_date = 0
    sqlitedb = sqliteUtils.sqliteUtils(args.dbfile)
    if what == 'agent':
        agents = sqlitedb.exec_multi_row_query('SELECT DISTINCT id FROM agents')
        for agnt in agents:
            #pp.pprint(agnt)            # returns a list of tuples (1,)
            dateint = sqlitedb.exec_atomic_int_query("SELECT first_pull_date FROM \
                agents WHERE id='{0}'".format(agnt[0]))
            if oldest_date == 0 or oldest_date > dateint:
                oldest_date = dateint
                oldest_i = agnt[0]
        old_obj = sqlitedb.exec_single_row_query("SELECT id,ipaddr,fqdn FROM \
            agents WHERE id='{0}'".format(oldest_i))
    elif what == 'client':
        clients = sqlitedb.exec_multi_row_query('SELECT DISTINCT id FROM hosts')
        for c in clients:
            dateint = sqlitedb.exec_atomic_int_query("SELECT date_discovered FROM \
                hosts WHERE id='{0}'".format(c[0]))
            if oldest_date == 0 or oldest_date > dateint:
                oldest_date = dateint
                oldest_i = c[0]
        old_obj = sqlitedb.exec_single_row_query("SELECT h.id,m.mac_addr,ip.ipaddr,h.name FROM \
            hosts h INNER JOIN macs m ON h.mac_id=m.id INNER JOIN ipaddrs ip ON h.ipaddr_id=ip.id \
            WHERE h.id='{0}'".format(oldest_i))
    else:
        raise Exception("Unrecognized entity ({0})".format(what))
    oldest_d[oldest_date] = old_obj
    return oldest_d

def get_newest(what):
    newest_d = {}
    newest_i = 0
    newest_date = 0
    sqlitedb = sqliteUtils.sqliteUtils(args.dbfile)
    if what == 'agent':
        agents = sqlitedb.exec_multi_row_query('SELECT DISTINCT id FROM agents')
        for agnt in agents:
            dateint = sqlitedb.exec_atomic_int_query("SELECT first_pull_date FROM \
                agents WHERE id='{0}'".format(agnt[0]))
            if newest_date == 0 or newest_date < dateint:
                newest_date = dateint
                newest_i = agnt[0]
        new_obj = sqlitedb.exec_single_row_query("SELECT id,ipaddr,fqdn FROM \
            agents WHERE id='{0}'".format(newest_i))
    elif what == 'client':
        clients = sqlitedb.exec_multi_row_query("SELECT DISTINCT id FROM hosts")
        for c in clients:
            dateint = sqlitedb.exec_atomic_int_query("SELECT date_discovered FROM \
                hosts WHERE id='{0}'".format(c[0]))
            if newest_date == 0 or newest_date < dateint:
                newest_date = dateint
                newest_i = c[0]
        new_obj = sqlitedb.exec_single_row_query("SELECT h.id,m.mac_addr,ip.ipaddr,h.name FROM \
            hosts h INNER JOIN macs m ON h.mac_id=m.id INNER JOIN ipaddrs ip ON h.ipaddr_id=ip.id \
            WHERE h.id='{0}'".format(newest_i))
    else:
        raise Exception("Unrecognized entity ({0})".format(what))
    newest_d[newest_date] = new_obj
    return newest_d

def main():
    agents_mac_count = {}
    sqlitedb = sqliteUtils.sqliteUtils(args.dbfile)
    if args.report_type == 'agent-summary':
        agent_ids = sqlitedb.exec_multi_row_query('SELECT id FROM agents')
        for id in agent_ids:
            count = sqlitedb.exec_atomic_int_query("SELECT COUNT(DISTINCT mac_id) FROM \
                agents_macs WHERE agent_id='{0}'".format(id[0]))
            agnt = sqlitedb.exec_single_row_query("SELECT ipaddr,fqdn FROM agents \
                WHERE id='{0}'".format(id[0]))
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
        last_updated = sqlitedb.exec_atomic_int_query("SELECT last_update FROM \
            agents WHERE id='{0}'".format(oldest[dateint][0]))
        print "| %17s | %-25s %23s|" % ("Last updated: ", time.strftime('%m/%d/%Y %H:%M:%S', time.localtime(last_updated)), " ")
        print '-' * 72
        newest = get_newest('agent')
        dateint = newest.keys()[0]
        if newest[dateint][2] and not newest[dateint][2] == 'None':
            print "| %17s | %-49s|" % ("Newest agent: ", newest[dateint][2])
        else:
            print "| %17s | %-49s|" % ("Newest agent: ", newest[dateint][1])
        print "| %17s | %-25s %23s|" % ("First seen: ", time.strftime('%m/%d/%Y %H:%M:%S', time.localtime(dateint)), "")
        last_updated = sqlitedb.exec_atomic_int_query("SELECT last_update FROM agents \
            WHERE id='{0}'".format(newest[dateint][0]))
        print "| %17s | %-25s %23s|" % ("Last updated: ", time.strftime('%m/%d/%Y %H:%M:%S', time.localtime(last_updated)), " ")
        print "=" * 72
    elif args.report_type == 'client-summary':
        total_macs = sqlitedb.exec_atomic_int_query("SELECT COUNT(DISTINCT mac_addr) FROM macs")
        total_ips = sqlitedb.exec_atomic_int_query("SELECT COUNT(DISTINCT ipaddr) FROM ipaddrs")
        print "=" * 72
        print "| Total unique MAC Addresses: %-41s|" % (total_macs)
        print '-' * 72
        print "| Total unique IP addresses: %-42s|" % (total_ips) 
        print "=" * 72
        oldest = get_oldest('client')
        dateint = oldest.keys()[0]
        print "| Oldest Client: %-54s|" % oldest[dateint][3]
        print "| %-10s %17s %-10s %-29s|" % ("MAC Addr:", oldest[dateint][1], "IP Addr:", oldest[dateint][2])
        print "| %-17s %-50s |" % ("Date Discovered: ", time.strftime('%m/%d/%Y %H:%M:%S', time.localtime(dateint)))
        print '-' * 72
        newest = get_newest('client')
        dateint = newest.keys()[0]
        print "| Newest Client: %-54s|" % newest[dateint][3]
        print "| %-10s %17s %-10s %-29s|" % ("MAC Addr:", newest[dateint][1], "IP Addr:", newest[dateint][2])
        print "| %-17s %-50s |" % ("Date Discovered: ", time.strftime('%m/%d/%y %H:%M:%S', time.localtime(dateint)))
        print "=" * 72
    elif args.report_type == 'client-detail':
        clients = sqlitedb.exec_multi_row_query("SELECT id FROM hosts ORDER BY name")
        print "=" * 105
        print "| %-17s | %-17s | %-17s | %-19s | %-19s |" % ("Name","MAC Addr","IP Addr","Date Discovered","Last Updated")
        print "-" * 105
        for hid in clients:
            #pp.pprint(hid)
            ctup = sqlitedb.exec_single_row_query("SELECT h.name,m.mac_addr,ip.ipaddr,h.date_discovered,h.last_updated FROM hosts h INNER JOIN macs m ON h.mac_id=m.id INNER JOIN ipaddrs ip ON h.ipaddr_id=ip.id WHERE h.id='{0}'".format(hid[0]))
            #pp.pprint(ctup)
            print("| %-17s | %-17s | %-17s | %-19s | %-19s |" % (ctup[0],ctup[1],ctup[2],time.strftime('%m/%d/%Y %H:%M:%S', time.localtime(ctup[3])),time.strftime('%m/%d/%Y %H:%M:%S', time.localtime(ctup[4]))))
        print "=" * 105
    else:
        raise Exception("Unrecognized report type! ({0})".format(args.report_type))

if __name__ == '__main__':
    main()
