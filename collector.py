#!/usr/bin/python

import pprint
import os.path
import sqlite3
import argparse
import datetime
#import paramiko

pp = pprint.PrettyPrinter(indent=4)

args_parse = argparse.ArgumentParser(description="Get those ARPs!")
args_parse.add_argument('-d', '--dbfile', dest='dbfile', help="The database file to store the collected data in.")
args_parse.add_argument('-a', '--agents', dest='agents_file', help="The list of end points to colect from")
args = args_parse.parse_args()

def get_data():
    f = open("/var/lib/arpwatch/arp.dat", 'r')
    blob = f.read()
    f.close()
    return blob
        
def process_dat(data_blob, agent_id=0):
    sql = ''
    conn = sqlite3.connect(args.dbfile)
    cur = conn.cursor()
    for line in data_blob.splitlines():
        # mac_add, ip_addr, epoch, name, iface
        fields = line.split('\t')
        print(fields[0])
        #cur.execute("SELECT id FROM macs WHERE mac_addr=?" % (fields[0]))
        cur.execute("SELECT id FROM macs WHERE mac_addr='" + fields[0] + "'")
        conn.commit()
        record_id = cur.fetchone()
        if record_id:
            print("Got record ID for mac " + fields[0] + ".  ID is " + str(record_id) + ".")
        else:
            print("No record id for mac " + fields[0] + ".")
    conn.close()

def main():
    create_tables_sql = {
        'macs': 'CREATE TABLE IF NOT EXISTS macs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_addr TEXT, date_discovered INTEGER, last_updated INTEGER)',
        'ipaddrs': 'CREATE TABLE IF NOT EXISTS ipaddrs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_id INTEGER, ipaddr TEXT, date_discovered INTEGER, last_updated INTEGER)',
        'agents': 'CREATE TABLE IF NOT EXISTS agents (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, ipaddr TEXT, fqdn TEXT, first_pull_date INTEGER, last_update INTEGER, iface TEXT)',
        'hosts': 'CREATE TABLE IF NOT EXISTS hosts (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_id INTEGER, ipaddr_id INTEGER, date_discovered INTEGER, last_updated INTEGER)',
        'agents_macs': 'CREATE TABLE IF NOT EXISTS agents_macs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, agent_id INTEGER, mac_id INTEGER)'
    }
    epoch_now = datetime.datetime.now()
    if args.agents_file:
        print("We're not handling multiple agents yet, just the local host.")
    else:
        if args.dbfile:
            conn = sqlite3.connect(args.dbfile)
            cur = conn.cursor()
            print "Creating tables if they don't exist in the target database....",
            for sql in create_tables_sql:
                cur.execute(create_tables_sql[sql])
            print("done.")

            blob = get_data()
            process_dat(blob)
        else:
            print("Need a database file.")

#if __name__ == "main":
main()
