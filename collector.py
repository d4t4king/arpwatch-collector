#!/usr/bin/python

import re
import pprint
import os.path
import sqlite3
import argparse
import datetime
import subprocess
import dns.resolver
#import paramiko
#from subprocess import check_output, CalledProcessError

pp = pprint.PrettyPrinter(indent=4)

epoch_now = datetime.datetime.now()

args_parse = argparse.ArgumentParser(description="Get those ARPs!")
args_parse.add_argument('-d', '--dbfile', dest='dbfile', default='/var/lib/arpwatch/collection.db', help="The database file to store the collected data in.")
args_parse.add_argument('-a', '--agents', dest='agents_file', help="The list of end points to colect from")
args = args_parse.parse_args()

def execute_atomic_int_query(sql):
    conn = sqlite3.connect(args.dbfile)
    with conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
        except ValueError, err:
            return -1
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
            raise TypeError("Unexpected SQL query result type: {0}.".format(type(result)))
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
    try:
        output = subprocess.check_output(['/usr/bin/ssh', host, 'ls /var/lib/arpwatch/'], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError, err:
        if err.returncode == 255:
            # likely ssh timed out becuse system offline or otherwise unavailable
            return err.returncode
        else:
            raise err
        
    for f in output.splitlines():
        # skip files ending in .new or -.  These are likely cache files and we don't want to process them
        if re.search(r'(\.new|\-)$', f):
            continue
        # skip the ethercodes.dat files.  These are vendor translations for mac addresses.
        # we may want to include this in the future, but it will probably be easier to
        # just get the data from IEEE
        if re.search(r'ethercodes\.(dat|db)', f):
            continue
        # skip the default collection database file
        if re.search(r'collection\.db', f):
            continue
        files.append(f)
    return files

def process_dat(data_blob, agent_id=0):
    sql = ''
    for line in data_blob.splitlines():
        # mac_add, ip_addr, epoch, name, iface
        fields = line.split('\t')
        pp.pprint(fields)
        # check if the record for the mac address is already in the database
        record_id = execute_atomic_int_query("SELECT id FROM macs WHERE mac_addr='{0}'".format(fields[0]))
        if record_id:
            # we found an id for that mac, so now check if the date matches that in the file
            lastUpdated = execute_atomic_int_query("SELECT last_updated FROM macs WHERE id='{0}'".format(record_id))
            if lastUpdated < fields[2]:
                # update the db date with that from the file
                execute_non_query("UPDATE macs SET last_updated='{0}' WHERE id='{1}'".format(fields[2], record_id))
            # otherwise don't do anything:
            # if the db date is greater than the file date, do nothing
            # if the db date is the same as the file date do nothing
        else:
            # no record found, so create a new one
            print("No record id for mac {0}.".format(fields[0]))
            # date_discovered = now, last_updated = file date
            execute_non_query("INSERT INTO macs (mac_addr,date_discovered,last_updated) VALUES ('{0}','{1}','{2}')".format(fields[0], epoch_now.strftime('%s'), fields[2]))
        # reacquire the mac id and
        mac_id = execute_atomic_int_query("SELECT id FROM macs WHERE mac_addr='{0}'".format(fields[0]))
        # check for a record for the IP address
        record_id = execute_atomic_int_query("SELECT id FROM ipaddrs WHERE ipaddr='{0}'".format(fields[1]))
        # if we find a record, update the last update date from the file
        if record_id:
            lastUpdated = execute_atomic_int_query("SELECT last_updated FROM ipaddrs WHERE id='{0}'".format(record_id))
            if lastUpdated < fields[2]:
                execute_non_query("UPDATE ipaddrs SET last_updated='{0}' WHERE id='{1}'".format(fields[2], record_id))
        else:
            print("No record id for ip address {0}".format(fields[1]))
            execute_non_query("INSERT INTO ipaddrs (mac_id,ipaddr,date_discovered,last_updated) VALUES ('{0}','{1}','{2}','{3}')".format(mac_id, fields[1], epoch_now.strftime('%s'), fields[2]))
        ipid = execute_atomic_int_query("SELECT id FROM ipaddrs WHERE ipaddr='{0}'".format(fields[1]))
        record_id = execute_atomic_int_query("SELECT id FROM hosts WHERE mac_id='{0}' AND ipaddr_id='{1}'".format(mac_id, ipid))
        if record_id:
            lastUpdated = execute_atomic_int_query("SELECT last_updated FROM hosts WHERE mac_id='{0}' and ipaddr_id='{1}'".format(mac_id, ipid))
            if lastUpdated < fields[2]:
                execute_non_query("UPDATE hosts SET last_updated='{0}' WHERE mac_id='{1}' AND ipaddr_id='{2}'".format(fields[2], mac_id, ipid))
        else:
            print("No record id for host with mac id ({0}) and ip id ({1})".format(mac_id, ipid))
            execute_non_query("INSERT INTO hosts (mac_id,ipaddr_id,date_discovered,last_updated) VALUES ('{0}','{1}','{2}','{3}')".format(mac_id, ipid, epoch_now.strftime('%s'), fields[2]))
        host_id = execute_atomic_int_query("SELECT id FROM hosts WHERE mac_id='{0}' AND ipaddr_id='{1}'".format(mac_id, ipid))
        if args.agents_file:
            record_id = execute_atomic_int_query("SELECT id FROM agents_macs WHERE agent_id='{0}' AND mac_id='{1}'".format(agent_id, mac_id))
            # There are not date stamps in the agents_Macs table.  It just links macs to agents.
            # so if we GET a record here, we don't need to do anything.
            if not record_id:
                # otherwise, we need to link the new mac to the agent
                execute_non_query("INSERT INTO agents_macs (agent_id, mac_id) VALUES ('{0}', '{1}')".format(agent_id, mac_id))
            

def main():
    create_tables_sql = {
        'macs': 'CREATE TABLE IF NOT EXISTS macs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_addr TEXT, date_discovered INTEGER, last_updated INTEGER)',
        'ipaddrs': 'CREATE TABLE IF NOT EXISTS ipaddrs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_id INTEGER, ipaddr TEXT, date_discovered INTEGER, last_updated INTEGER)',
        'agents': 'CREATE TABLE IF NOT EXISTS agents (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, ipaddr TEXT, fqdn TEXT, first_pull_date INTEGER, last_update INTEGER, iface TEXT)',
        'hosts': 'CREATE TABLE IF NOT EXISTS hosts (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_id INTEGER, ipaddr_id INTEGER, date_discovered INTEGER, last_updated INTEGER)',
        'agents_macs': 'CREATE TABLE IF NOT EXISTS agents_macs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, agent_id INTEGER, mac_id INTEGER)'
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
                ### get agent id from database
                agent_id = execute_atomic_int_query("SELECT id FROM agents WHERE ipaddr='{0}' OR fqdn='{0}'".format(agnt))
                ### if it exists
                if agent_id:
                    print("Got agent ID: {0}.".format(agent_id))
                    ### update the last_updated date/time
                    execute_non_query("UPDATE agents SET last_update='{0}' WHERE id='{1}'".format(epoch_now.strftime('%s'), agent_id))
                else:
                    ### try to get the name/IP
                    ### create the agent record
                    if re.search(r'^(\d+\.){3}(\d+)$', agnt):               # looks like IP address
                        a = ''
                        try:
                            a = dns.resolver.query(agnt, 'A')
                        except dns.resolver.NXDOMAIN, nx:
                            a = 'UNRESOLVED'
                        if a and not 'UNRESOLVED' in a:
                            pp.pprint(a.rrset)
                            print dir(a.rrset)
                            print a.rrset.name
                            #exit(1)
                            execute_non_query("INSERT INTO agents (ipaddr,fqdn,first_pull_date,last_update) VALUES ('{0}','{1}','{2}','{2}')".format(agnt, a.rrset.name, epoch_now.strftime('%s')))
                        else:
                            execute_non_query("INSERT INTO agents (ipaddr,first_pull_date,last_update) VALUES ('{0}','{1}','{1}')".format(agnt, epoch_now.strftime('%s')))
                    else:                                                   # assume it looks like an FQDN
                        ptr = ''
                        try:
                            ptr = dns.resolver.query(agnt, 'PTR')
                        except dns.resolver.NoAnswer, err:
                            ptr = 'NOPTR'
                        if ptr and not ptr == 'NOPTR':
                            execute_non_query("INSERT INTO agents (ipaddr,fqdn,first_pull_date,last_update) VALUES ('{0}','{1}','{2}','{2}')".format(ptr, agnt, epoch_now.strftime('%s')))
                        else:
                            execute_non_query("INSERT INTO agents (fqdn,first_pull_date,last_update) VALUES ('{0}','{1}','{1}')".format(agnt, epoch_now.strftime('%s')))
                    agent_id = execute_atomic_int_query("SELECT id FROM agents WHERE ipaddr='{0}' OR fqdn='{0}'".format(agnt))
                files = get_files(agnt)
                if files and 'list' in str(type(files)):
                    for f in files:
                        print("Processing file: {0} from agent {1}".format(f, agnt))
                        blob = subprocess.check_output(['/usr/bin/ssh', agnt, 'cat /var/lib/arpwatch/' + f])
                        if blob:
                            process_dat(blob, agent_id)
                        else:
                            print("Got no data from {0}:/var/lib/arpwatch/{1}".format(agnt, f))
                else:
                    print("There was a poroblem getting the files from agent ({0})!".format(agnt))
    else:
        blob = get_data()
        process_dat(blob)


if __name__ == "__main__":
    main()
