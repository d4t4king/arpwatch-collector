#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper;

use lib '../SQL-Utils/lib/';
use SQL::Utils;

my %create_tables_sql = (
	'macs' => 'CREATE TABLE IF NOT EXISTS macs (id INTEGER NOT NULL AUTOINCREMENT, date_discovered INTEGER, last_updated INTEGER)',
	'ipaddrs' => 'CREATE TABLE IF NOT EXISTS ipaddrs (id INTEGER NOT NULL AUTOINCREMENT, mac_id INTEGER, ipaddr TEXT, date_discovered INTEGER, last_updated INTEGER)',
	'agents' => 'CREATE TABLE IF NOT EXISTS agents (id INTEGER NOT NULL AUTOINCREMENT, ipaddr TEXT, fqdn TEXT, first_pull_date INTEGER, last_update INTEGER)',
	'hosts' => 'CREATE TABLE IF NOT EXISTS agents (id INTEGER NOT NULL AUTOINCREMENT, mac_id INTEGER, ipaddr_id INTEGER, date_discovered INTEGER, last_update INTEGER)',
	'agents_macs' => 'CREATE TABLE IF NOT EXISTS agents_macs (id INTEGER NOT NULL AUTOINCREMENT, agent_id INTEGER, mac_id INTEGER)',
);

sub process_dat {
	my $blob = shift;
	my @lines = split(/\r?\n/, $blob);
	foreach my $l ( @lines ) {
		my ($mac, $ip, $epoch, $name, $iface) = split(/\t/, $l);
		print "$mac, $ip, $epoch, $name, $iface\n";
	}
}

sub get_data {
	local $/ = undef;
	open IN, "</var/lib/arpwatch/eth1.dat" or die "Couldn't open arpwatch data file: $!";
	my $blob = <IN>;
	close IN or die "There was a problem closing the arpwatch data file: $!";
	return $blob;
}


