#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper;
use Getopt::Long;

use lib '../SQL-Utils/lib/';
use SQL::Utils;

my ($help, $verbose, $dbfile);
GetOptions(
	'h|help'		=>	\$help,
	'v|verbose+'	=>	\$verbose,
	'd|dbfile=s'	=>	\$dbfile,
);

my %create_tables_sql = (
	'macs' => 'CREATE TABLE IF NOT EXISTS macs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_addr TEXT, date_discovered INTEGER, last_updated INTEGER)',
	'ipaddrs' => 'CREATE TABLE IF NOT EXISTS ipaddrs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_id INTEGER, ipaddr TEXT, date_discovered INTEGER, last_updated INTEGER)',
	'agents' => 'CREATE TABLE IF NOT EXISTS agents (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, ipaddr TEXT, fqdn TEXT, first_pull_date INTEGER, last_update INTEGER, iface TEXT)',
	'hosts' => 'CREATE TABLE IF NOT EXISTS hosts (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_id INTEGER, ipaddr_id INTEGER, date_discovered INTEGER, last_updated INTEGER)',
	'agents_macs' => 'CREATE TABLE IF NOT EXISTS agents_macs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, agent_id INTEGER, mac_id INTEGER)',
);

if ((!defined($dbfile)) or ($dbfile eq '')) { die "Need a database file!"; }

# sqlite3 only does dates as epoch integers or strings,
my $epoch_now = time();
print "Epoch now: $epoch_now \n" if ($verbose);

# create the sql-utils object
my $sqlutils = SQL::Utils->new('sqlite3', { 'db_filename' => $dbfile });

# create the tables
foreach my $t ( keys %create_tables_sql ) {
	$sqlutils->execute_non_query($create_tables_sql{$t});
}

my $blob = &get_data();

&process_dat($blob);

### Subs ###
sub get_data {
	local $/ = undef;
	open IN, "</var/lib/arpwatch/arp.dat" or die "Couldn't open arpwatch data file: $!";
	my $blob = <IN>;
	close IN or die "There was a problem closing the arpwatch data file: $!";
	return $blob;
}

sub process_dat {
	my $blob = shift;
	my @lines = split(/\r?\n/, $blob);
	my $sql = '';
	foreach my $l ( @lines ) {
		my ($mac, $ip, $epoch, $name, $iface) = split(/\t/, $l);
		print "$mac, $ip, $epoch, $name, $iface\n" if ($verbose);
		my $record_id = $sqlutils->execute_atomic_int_query("SELECT id FROM macs WHERE mac_addr='$mac';");
		my $rtv = -1;
		if ($record_id) {
			$rtv = $sqlutils->execute_non_query("UPDATE macs SET last_updated='$epoch_now' WHERE mac_addr='$mac';");
		} else {
			$rtv = $sqlutils->execute_non_query("INSERT INTO macs (mac_addr, date_discovered, last_updated) VALUES ('$mac', '$epoch_now', '$epoch_now');");
		}
		print "MACS RTV: $rtv\n";
		my $mac_id = $sqlutils->execute_atomic_int_query("SELECT id FROM macs WHERE mac_addr='$mac'");
		$record_id = $sqlutils->execute_atomic_int_query("SELECT id FROM ipaddrs WHERE ipaddr='$ip';");
		if ($record_id) {
			$rtv = $sqlutils->execute_non_query("UPDATE ipaddrs SET last_updated='$epoch_now' WHERE ipaddr='$ip';");
		} else {
			$rtv = $sqlutils->execute_non_query("INSERT INTO ipaddrs (mac_id, ipaddr, date_discovered, last_updated) VALUES ('$mac_id','$ip','$epoch_now','$epoch_now');");
		}
		print "IPADDRS RTV: $rtv\n";
		my $ipaddr_id = $sqlutils->execute_atomic_int_query("SELECT id FROM ipaddrs WHERE ipaddr='$ip';");
		$record_id = $sqlutils->execute_atomic_int_query("SELECT id FROM hosts WHERE mac_id='$mac_id' AND ipaddr_id='$ipaddr_id';");
		if ($record_id) {
			$rtv = $sqlutils->execute_non_query("UPDATE hosts SET last_discovered='$epoch_now' WHERE mac_id='$mac_id' AND ipaddr_id='$ipaddr_id';");
		} else {
			$rtv = $sqlutils->execute_non_query("INSERT INTO hosts (mac_id,ipaddr_id,date_discovered,last_updated) VALUES ('$mac_id','$ipaddr_id','$epoch_now','$epoch_now');");
		}
		print "HOSTS RTV: $rtv\n";
	}
}
