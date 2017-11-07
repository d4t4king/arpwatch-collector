#!/usr/bin/python

import re
import pprint
import socket
import os.path
import sqlite3
import argparse
import datetime
import subprocess
import dns.resolver
### custom py libs
import arpwatch
import sqliteUtils

args_parse = argparse.ArgumentParser(description="Get those ARPs!")
args_parse.add_argument('-d', '--dbfile', dest='dbfile', default='/var/lib/arpwatch/collection.db', help="The database file to store the collected data in.")
args_parse.add_argument('-a', '--agents', dest='agents_file', help="The list of end points to colect from")
args = args_parse.parse_args()

pp = pprint.PrettyPrinter(indent=4)
sqlitedb = sqliteUtils.sqliteUtils(args.dbfile)

def main():
    create_tables_sql = {
        'macs': 'CREATE TABLE IF NOT EXISTS macs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_addr TEXT, date_discovered INTEGER, last_updated INTEGER)',
        'ipaddrs': 'CREATE TABLE IF NOT EXISTS ipaddrs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_id INTEGER, ipaddr TEXT, date_discovered INTEGER, last_updated INTEGER)',
        'agents': 'CREATE TABLE IF NOT EXISTS agents (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, ipaddr TEXT, fqdn TEXT, file_interface TEXT, first_pull_date INTEGER, last_update INTEGER, iface TEXT)',
        'hosts': 'CREATE TABLE IF NOT EXISTS hosts (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_id INTEGER, ipaddr_id INTEGER, name TEXT, date_discovered INTEGER, last_updated INTEGER)',
        'agents_macs': 'CREATE TABLE IF NOT EXISTS agents_macs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, agent_id INTEGER, mac_id INTEGER)',
		'interfaces': 'CREATE TABLE IF NOT EXISTS interfaces (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, agent_id INTEGER, name TEXT, ipv4_addr TEXT, ipv4_bdct TEXT, ipv4_netmask TEXT, hwaddr TEXT)',
    }
    # create the tables in the sqlite3 database
    if args.dbfile:
        conn = sqlite3.connect(args.dbfile)
        cur = conn.cursor()
        print "Creating tables if they don't exist in the target database....",
        for sql in create_tables_sql:
            cur.execute(create_tables_sql[sql])
        print("done.")
        conn.commit()
        conn.close()
    else:
        raise Exception("Need a database file!")

    ### if agents list
    if args.agents_file:
        ### open the list of agents
        with open(args.agents_file, 'r') as f:
            ### loop through the agents
            for line in f:
                # skip empty lines (?)
                if not line:
                    continue
                ### skip commented lines
                if re.match(r'^#', line):
                    continue
                agnt = line.strip()
                print("Agent: " + agnt)
                agent = arpwatch.Agent(agnt)
                #agent.set_agentid(args.dbfile)
                agent.save(args.dbfile)
                files = agent.getFiles()
                pp.pprint(files)
                if files and 'list' in str(type(files)):
                    for f in files:
                        print("Processing file: {0} from agent {1}".format(f, agnt))
                        blob = subprocess.check_output(['/usr/bin/ssh', agnt, 'cat /var/lib/arpwatch/' + f])
                        if blob:
                            agent.processDat(blob, args.dbfile)
                        else:
                            print("Got no data from {0}:/var/lib/arpwatch/{1}".format(agnt, f))
                else:
                    print("There was a problem getting the files from agent ({0})!".format(agnt))
    else:
        ipaddr = socket.gethostbyname(socket.gethostname())
        agent = arpwatch.Agent(ipaddr)
        agent.save(args.dbfile)
        files = agent.getFiles()
        if files and 'list' in str(type(files)):
            for f in files:
                print("Processinf file {0} from agent {1}".format(f, agent.fqdn))
                blob = subprocess.check_output(['/bin/cat', "/var/lib/arpwatch/{0}".format(f)])
                if blob:
	                agent.processDat(blob, args.dbfile)
                else:
                    print("Got no data from {0}:/var/lib/arpwatch/{1}".format(agent.fqdn, f))
        else:
            print("There was a problem getting the files from agent ({0})!".format(agent.fqdn))

if __name__ == "__main__":
    main()
