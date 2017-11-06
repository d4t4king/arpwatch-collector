#!/usr/bin/env python
# -*- coding: utf-8 -*-

# arpwatch objects
# mainly containers

import re
from datetime import now
from time import strftime,localtime
from subprocess import check_output

import sqliteUtils

class EndPoint(object):
    """
        This is an abstract class that should define
        many of the base attributes and methods for 
        super classes like Agents and Clients.
    """

	def __init__(self, macaddr):
        """
            Returns an instance of the object.
        """
        self.mac_addr = macaddr

class Agent(EndPoint):
    """
        Extends teh EndPoint object with relevant attributes
        and methods for arpwatch Agents
    """

    def __init__(self, macaddr, ipv4addr='', firstPullDate, lastUpdate):
        """
            Returns an instance of the Agent object.
        """
        self.mac_addr = macaddr
		self.ipv4addr = ipv4addr
        self.firstPullDate = firstPullDate
        self.lastUpdate = lastUpdate

    def _isValidIP(ipv4addr):
        match = re.search(r'\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b'm ipv4addr)
        if match:
            return True
        else:
            return False

    def _parseNslookup(ipName):
        rname = ''
        result = check_output(['nslookup', ipName])
        match = re.search(r'name\s+=\s+(.+)', result)
        if match:
            rname = match.group(1).strip()
        else:
            raise Exception("Unable to resolve ip.name: {0}".format(ipName))
        return rname

    def _reallyResolve(ipName):
        rname = ''
        if _isValidIP(ipName):
            # got an IP address, so resolve the PTR
            try:
                rname = query(ipName, 'PTR')
            except NXDOMAIN, err:
                rname = _parseNslookup(ipName)
        else:
            # assume it's a host/domain name
            rname = query(ipName, 'A')
        return rname

    def save(self, dbfile):
        """
            Saves the stored data to the database.
        """
        sqlitedb = sqliteUtils.sqliteUtils(dbfile)
        # If we get an IP id, even better chance the agent exists.  Continue...
        sql = "SELECT MIN(id) FROM agents WHERE ipaddr_id='{0}'".format(self.ipv4addr)
        agent_id = sqlitedb.exec_atomic_int_query(sql)
        if agent_id:
            # agent already exists, so just update the date
            now = now()
            sql = "UPDATE agents SET last_updated='{0}' WHERE id='{1}'".format(strftime('%s', localtime(now)), agent_id)
            sqlitedb.exec_non_query(sql)
        else:
            # agent does not exist
            # resolve the IP, if we can
            rname = _reallyResolve(self.ipv4addr)
            # TODO: get the interface info
            now = now()
            if rname and not 'UNRESOLVED' in rname:
                sql = "INSERT INTO agents (ipaddr,fqdn,first_pull_date,last_update) VALUES ('{0}','{1}','{2}','{2}')".format(self.ipv4addr, rname, strftime('%s', localtime(now)))
            else:
                sql = "INSERT INTO agents (ipaddr,first_pull_date,last_update) VALUES ('{0}','{1}','{1}')".format(self.ipv4addr, strftime('%s', localtime(now)))
            sqlitedb.exec_non_query(sql)

