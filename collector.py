#!/usr/bin/python

import re
import pprint
import os.path
import sqlite3
import argparse
import datetime
import dns.resolver
#import paramiko
from subprocess import check_output

pp = pprint.PrettyPrinter(indent=4)

epoch_now = datetime.datetime.now()

args_parse = argparse.ArgumentParser(description="Get those ARPs!")
args_parse.add_argument('-d', '--dbfile', dest='dbfile', help="The database file to store the collected data in.")
args_parse.add_argument('-a', '--agents', dest='agents_file', help="The list of end points to colect from")
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

def execute_non_query(sql):
    conn = sqlite3.connect(args.dbfile)
    with conn:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

def get_data():
    f = open("/var/lib/arpwatch/arp.dat", 'r')
    blob = f.read()
    f.close()
    return blob

def get_files(host):
    files = []
    output = check_output(['/usr/bin/ssh', host, 'ls /var/lib/arpwatch/'])
    for f in output.splitlines():
        # skip files ending in .new or -.  These are likely cache files and we don't want to process them
        if re.search(r'(\.new|\-)$', f):
            continue
        # skip the ethercodes.dat files.  These are vendor translations for mac addresses.
        # we may want to include this in the future, but it will probably be easier to
        # just get the data from IEEE
        if re.search(r'ethercodes\.(dat|db)', f):
            continue
        files.append(f)
    #pp.pprint(files)
    return files
        
def process_dat(data_blob, agent_id=0):
    sql = ''
    for line in data_blob.splitlines():
        # mac_add, ip_addr, epoch, name, iface
        fields = line.split('\t')
        record_id = execute_atomic_int_query("SELECT id FROM macs WHERE mac_addr='" + fields[0] + "'")
        if record_id:
            #print("Got record ID for mac " + fields[0] + ".  ID is " + str(record_id) + ".")
            execute_non_query("UPDATE macs SET last_updated='" + str(epoch_now.strftime('%s')) + "'")
        else:
            print("No record id for mac " + fields[0] + ".")
            execute_non_query("INSERT INTO macs (mac_addr,date_discovered,last_updated) VALUES ('" + fields[0] + "','" + str(epoch_now.strftime('%s')) + "','" + str(epoch_now.strftime('%s')) + "')")
        mac_id = execute_atomic_int_query("SELECT id FROM macs WHERE mac_addr='" + fields[0] + "'")
        record_id = execute_atomic_int_query("SELECT id FROM ipaddrs WHERE ipaddr='" + fields[1] + "'")
        if record_id:
            #print("Got record ID for ip address " + fields[1] + ".  ID is " + str(record_id) + ".")
            execute_non_query("UPDATE ipaddrs SET last_updated='" + str(epoch_now.strftime('%s')) + "'")
        else:
            print("No record id for ip address " + fields[1] + " ")
            execute_non_query("INSERT INTO ipaddrs (mac_id,ipaddr,date_discovered,last_updated) VALUES ('" + str(mac_id) + "','" + fields[1] + "','" + str(epoch_now.strftime('%s')) + "','" + str(epoch_now.strftime('%s')) + "')")
        ipid = execute_atomic_int_query("SELECT id FROM ipaddrs WHERE ipaddr='" + fields[1] + "'")
        record_id = execute_atomic_int_query("SELECT id FROM hosts WHERE mac_id='" + str(mac_id) + "' AND ipaddr_id='" + str(ipid) + "'")
        if record_id:
            #print("Got record ID for host with mac_id (" + str(mac_id) + ") and ip id (" + str(ipid) + ")")
            execute_non_query("UPDATE hosts SET last_updated='" + str(epoch_now.strftime('%s')) + "' WHERE mac_id='" + str(mac_id) + "' AND ipaddr_id='" + str(ipid) + "'")
        else:
            print("No record id for host with mac id (" + str(mac_id) + ") and ip id (" + str(ipid) + ")")
            execute_non_query("INSERT INTO hosts (mac_id,ipaddr_id,date_discovered,last_updated) VALUES ('" + str(mac_id) + "','" + str(ipid) + "','" + str(epoch_now.strftime('%s')) + "','" + str(epoch_now.strftime('%s')) + "')")
        host_id = execute_atomic_int_query("SELECT id FROM hosts WHERE mac_id='" + str(mac_id) + "' AND ipaddr_id='" + str(ipid) + "'")
        if args.agents_file:
            record_id = execute_atomic_int_query("SELECT id FROM agents_macs WHERE agent_id='" + str(agent_id) + "' AND mac_id='" + str(mac_id) + "'")

def main():
    create_tables_sql = {
        'macs': 'CREATE TABLE IF NOT EXISTS macs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_addr TEXT, date_discovered INTEGER, last_updated INTEGER)',
        'ipaddrs': 'CREATE TABLE IF NOT EXISTS ipaddrs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_id INTEGER, ipaddr TEXT, date_discovered INTEGER, last_updated INTEGER)',
        'agents': 'CREATE TABLE IF NOT EXISTS agents (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, ipaddr TEXT, fqdn TEXT, first_pull_date INTEGER, last_update INTEGER, iface TEXT)',
        'hosts': 'CREATE TABLE IF NOT EXISTS hosts (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_id INTEGER, ipaddr_id INTEGER, date_discovered INTEGER, last_updated INTEGER)',
        'agents_macs': 'CREATE TABLE IF NOT EXISTS agents_macs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, agent_id INTEGER, mac_id INTEGER)'
    }
    if args.dbfile:
        conn = sqlite3.connect(args.dbfile)
        cur = conn.cursor()
        print "Creating tables if they don't exist in the target database....",
        for sql in create_tables_sql:
            cur.execute(create_tables_sql[sql])
        print("done.")
        conn.commit()
        conn.close()

        if args.agents_file:
            #print("We're not handling multiple agents yet, just the local host.")
            with open(args.agents_file, 'r') as f:
                for line in f:
                    # skip empty lines (?)
                    if not line:
                        continue
                    # skip commented lines
                    if re.match(r'^#', line):
                        continue
                    agnt = line.strip()
                    print("Agent: " + agnt)
                    agent_id = execute_atomic_int_query("SELECT id FROM agents WHERE ipaddr='" + agnt + "' OR fqdn='" + agnt + "'")
                    if agent_id:
                        print("Got agent ID: " + str(agent_id) + ".")
                        execute_non_query("UPDATE agents SET last_update='" + str(epoch_now.strftime('%s')) + "' WHERE id='" + str(agent_id) + "'")
                    else:
                        if re.search(r'^(\d+\.){3}(\d+)$', agnt):               # looks like IP address
                            a = ''
                            try:
                                a = dns.resolver.query(agnt, 'A')
                            except dns.resolver.NXDOMAIN, nx:
                                a = 'UNRESOLVED'
                            if a:
                                execute_non_query("INSERT INTO agents (ipaddr,fqdn,first_pull_date,last_update) VALUES ('" + agnt + "','" + str(a) + "','" + str(epoch_now.strftime('%s')) + "','" + str(epoch_now.strftime('%s')) + "')")
                            else:
                                execute_non_query("INSERT INTO agents (ipaddr,first_pull_date,last_update) VALUES ('" + agnt + "','" + str(epoch_now.strftime('%s')) + "','" + str(epoch_now.strftime('%s')) + "')")
                        else:                                                   # assume it looks like an FQDN
                            ptr = dns.resolver.query(agnt, 'PTR')
                            if ptr:
                                execute_non_query("INSERT INTO agents (ipaddr,fqdn,first_pull_date,last_update) VALUES ('" + ptr + "','" + agnt + "','" + str(epoch_now.strftime('%s')) + "','" + str(epoch_now.strftime('%s')) + "')")
                            else:
                                execute_non_query("INSERT INTO agents (fqdn,first_pull_date,last_update) VALUES ('" + agnt + "','" + str(epoch_now.strftime('%s')) + "','" + str(epoch_now.strftime('%s')) + "')")
                        agent_id = execute_atomic_int_query("SELECT id FROM agents WHERE ipaddr='" + agnt + "' OR fqdn='" + agnt + "'")
                    
                    files = get_files(agnt)
                    for f in files:
                        print("Processing file: " + f + " from agent " + agnt)
                        blob = check_output(['/usr/bin/ssh', agnt, 'cat /var/lib/arpwatch/' + f])
                        if blob:
                            process_dat(blob, agent_id)
                        else:
                            print("Got no data from " + agnt + ":/var/lib/arpwatch/" + f)
                            
        else:
            blob = get_data()
            process_dat(blob)
    else:
        print("Need a database file.")

if __name__ == "__main__":
    main()
