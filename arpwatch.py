#!/usr/bin/python
# -*- coding: utf-8 -*-

# arpwatch objects
# mainly containers

import re
import time
import pprint
import datetime
import subprocess
from dns.resolver import query,NXDOMAIN,NoAnswer

import sqliteUtils

__name__ = 'arpwatch'
__version__ = '0.1'

class ArpwatchUtils(object):

    ################################
    ### isValidIP - 
    @staticmethod
    def isValidIP(ipv4addr):
        match = re.search(r'\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b', ipv4addr)
        if match:
            return True
        else:
            return False

    ################################
    ### parseNslookup - 
    @staticmethod
    def parseNslookup(ipName):
        rname = ''
        result = subprocess.check_output(['nslookup', ipName])
        match = re.search(r'name\s+=\s+(.+)', result)
        if match:
            rname = match.group(1).strip()
        else:
            raise Exception("Unable to resolve ip.name: {0}".format(ipName))
        return rname

    ################################
    ### reallyResolve - 
    @staticmethod
    def reallyResolve(ipName):
        rname = ''
        if ArpwatchUtils.isValidIP(ipName):
            # got an IP address, so resolve the PTR
            try:
                rname = query(ipName, 'PTR')
            except NXDOMAIN, err:
                rname = ArpwatchUtils.parseNslookup(ipName)
        else:
            # assume it's a host/domain name
            rname = query(ipName, 'A')
        return rname

    ################################
    ### getDbId - 
    @staticmethod
    def getDbId(what, identifier, dbfile):
        #__sqlitedb = sqliteUtils.sqliteUtils(dbfile)
        if what == 'mac':
            sql = "SELECT id FROM macs WHERE mac_addr='{0}'".format(identifier)
        elif what == 'ip':
            sql = "SELECT id FROM ipaddrs WHERE ipaddr='{0}'".format(identifier)
        elif what == 'host':
            sql = "SELECT id FROM hosts WHERE mac_id='{0}' \
                AND ipaddr_id='{1}'".format(identifier[0], identifier[1])
        elif what == 'agents_macs':
            sql = "SELECT id FROM agents_macs WHERE agent_id='{0}' \
                AND mac_id='{1}'".format(identifier[0], identifier[1])
        else:
            raise Exception("Unknown entity to get database id for ({0})".format(what))

        import sqlite3
        result = ''
        record_id = -1
        conn = sqlite3.connect(dbfile)
        with conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            result = cursor.fetchone()
        if 'tuple' in str(type(result)):
            record_id = result[0]
        elif 'int' in str(type(result)):
            record_id = result
        elif 'NoneType' in str(type(result)):
            record_id = False
        else:
            raise TypeError("Unexpected object type from query: {0}".format(type(result)))
        #record_id = __sqlitedb.exec_atomic_int_query(sql)
        return record_id


#######################################################################
### EndPoint Abstract class.
### Plaseholder for Agent and CLient classes
class EndPoint(object):
    """
        This is an abstract class that should define
        many of the base attributes and methods for 
        super classes like Agents and Clients.
    """

    def __init__(self):
        """
            Returns an instance of the object.
        """

#######################################################################
### Agent class
### This represents the end points that are running arpwatch 
### and doing the monitoring.
class Agent(object):
    """
        Extends teh EndPoint object with relevant attributes
        and methods for arpwatch Agents
    """

    ################################
    ### __init__ - Constructor
    def __init__(self, ipv4addr, firstPullDate=time.strftime('%s', time.localtime()), 
        lastUpdate=datetime.datetime.now()):
        """
            Returns an instance of the Agent object.
        """
        self.ipv4addr = ipv4addr
        self.firstPullDate = float(firstPullDate)
        self.lastUpdate = lastUpdate
        self.fqdn = ArpwatchUtils.reallyResolve(self.ipv4addr)

    ################################
    ### save - 
    def save(self, dbfile):
        pp = pprint.PrettyPrinter(indent=4)
        """
            Saves the stored data to the database.
        """
        sqlitedb = sqliteUtils.sqliteUtils(dbfile)
        # If we get an IP id, even better chance the agent exists.  Continue...
        sql = "SELECT MIN(id) FROM agents WHERE ipaddr='{0}'".format(self.ipv4addr)
        agent_id = sqlitedb.exec_atomic_int_query(sql)
        if agent_id:
            # agent already exists, so just update the date
            enow = time.strftime('%s', time.localtime())
            sql = "UPDATE agents SET last_update='{0}' WHERE id='{1}'".format(
                enow, agent_id)
            sqlitedb.exec_non_query(sql)
        else:
            # agent does not exist
            # resolve the IP, if we can
            rname = ArpwatchUtils.reallyResolve(self.ipv4addr)
            # TODO: get the interface info
            enow = time.strftime('%s', time.localtime())
            if rname and not 'UNRESOLVED' in rname:
                sql = "INSERT INTO agents (ipaddr,fqdn,first_pull_date,last_update) \
                    VALUES ('{0}','{1}','{2}','{3}')".format(self.ipv4addr, rname, \
                    time.strftime('%s', time.localtime(self.firstPullDate)), enow)
            else:
                sql = "INSERT INTO agents (ipaddr,first_pull_date,last_update) \
                    VALUES ('{0}','{1}','{1}')".format(self.ipv4addr, enow)
            sqlitedb.exec_non_query(sql)
        # get the db id for the record
        sql = "SELECT id FROM agents WHERE ipaddr='{0}'".format(self.ipv4addr)
        self.agentid = sqlitedb.exec_atomic_int_query(sql)

    ################################
    ### id - 
    def set_agentid(self,dbfile):
        sqlitedb = sqliteUtils.sqliteUtils(dbfile)
        sql = "SELECT MIN(id) FROM agents WHERE ipaddr='{0}'".format(self.ipv4addr)
        self.agentid = sqlitedb.exec_atomic_int_query(sql)

    ################################
    ### getFiles - 
    def getFiles(self):
        """
            Gets the arpwatch related data files from teh agent.
            For now, assumes that access is allowed and SSH keys
            are set up between the collector and the Agent.
        """
        files = []
        try:
            output = subprocess.check_output(['/usr/bin/ssh', self.ipv4addr, 'ls /var/lib/arpwatch/'], stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError, err:
            if err.returncode == 255:
                # likely ssh timed out because the system if offline or othersise unavailable
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
            if re.search(r'ethercodes\.(date|db)', f):
                continue
            # skip the default collection database file
            if re.search(r'collection\.db', f):
                continue
            files.append(f.strip())
        return files

    ################################
    ### processDate - 
    def processDat(self, dataBlob, dbfile):
        pp = pprint.PrettyPrinter(indent=4)
        sql = ''
        for line in dataBlob.splitlines():
            # mac_addr, ip_addr, record_date, name, iface
            fields = line.split('\t')
            #pp.pprint(fields)
            if fields[0] == '0.0.0' and not fields[2]:
                # skip likely invalid MAC addresses with no record date
                continue
            if not self.agentid:
                raise Exception("No agent id for client!")
            print("Agent ID: {0}".format(self.agentid))
            client = Client(fields[0], fields[1], fields[2], fields[3], fields[4], self.agentid)
            client.save(dbfile)
            

#######################################################################
### Client class
### This represents the discovered end points.  Presumably these
### are DHCP clients that are on the same subnet as the Agent.
class Client(EndPoint):
    """
        Returns a Client object which represents the arp endpoint
        discovered by arpwatch.
    """

    ################################
    ### __init__
    def __init__(self, macaddr, ipv4addr, recordDate, name='None', \
        interface='None', agentid=0):
        """
            Constructor for the Client class
        """
        self.macaddr = macaddr
        self.ipv4addr = ipv4addr
        self.discoveredDate = recordDate
        if name == 'None':
            self.name = ArpwatchUtils.reallyResolve(ipv4addr)
        else:
            self.name = name
        self.interface = interface
        self.agentid = agentid
        print("MAC ADDR: {0}".format(self.macaddr))

    ################################
    ### save
    ### Saves the client data to the database
    def save(self, dbfile):
        sql = ''
        sqlitedb = sqliteUtils.sqliteUtils(dbfile)
        enow = time.strftime('%s', time.localtime())
        mac_id = ArpwatchUtils.getDbId('mac', self.macaddr, dbfile)
        if not mac_id:
            sql = "INSERT INTO macs (mac_addr, date_discovered, last_updated) \
                VALUES ('{0}','{1}','{2}')".format(self.macaddr,self.discoveredDate,enow)
        else:
            sql = "UPDATE macs SET last_updated='{0}' WHERE id='{1}'".format(enow,mac_id)
        #print(sql)
        sqlitedb.exec_non_query(sql)
        mac_id = ArpwatchUtils.getDbId('mac', self.macaddr, dbfile)
        ipaddr_id = ArpwatchUtils.getDbId('ip', self.ipv4addr, dbfile)
        if not ipaddr_id:
            sql = "INSERT INTO ipaddrs (mac_id,ipaddr,date_discovered,last_updated) \
                VALUES ('{0}','{1}','{2}','{3}')".format(mac_id,self.ipv4addr,self.discoveredDate,enow)
        else:
            sql = "UPDATE ipaddrs SET last_updated='{0}' WHERE id='{1}'".format(enow,ipaddr_id)
        #print(sql)
        sqlitedb.exec_non_query(sql)
        ipaddr_id = ArpwatchUtils.getDbId('ip', self.ipv4addr, dbfile)
        #print "mac_id: {0} ipaddr_id: {1}".format(mac_id, ipaddr_id)
        host_id = ArpwatchUtils.getDbId('host', (mac_id, ipaddr_id), dbfile)
        if not host_id:
            sql = "INSERT INTO hosts (mac_id,ipaddr_id,name,date_discovered,last_updated) \
                VALUES ('{0}','{1}','{2}','{3}','{4}')".format(mac_id,ipaddr_id,self.name, \
                self.discoveredDate,enow)
        else:
            sql = "UPDATE hosts SET last_updated='{0}' WHERE id='{1}'".format(enow,host_id)
        #print(sql)
        sqlitedb.exec_non_query(sql)
        host_id = ArpwatchUtils.getDbId('host', (mac_id,ipaddr_id), dbfile)
        ag_mac_id = ArpwatchUtils.getDbId('agents_macs', (self.agentid,mac_id), dbfile)
        if not ag_mac_id:
            sql = "INSERT INTO agents_macs (agent_id, mac_id) \
                VALUES ('{0}','{1}')".format(self.agentid,mac_id)
        #print(sql)
        if not self.agentid:
            raise Exception("No id for agent!")
        sqlitedb.exec_non_query(sql)

