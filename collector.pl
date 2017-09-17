#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper;
use Getopt::Long;
use Net::SSH::Perl;
use Net::Nslookup;

use lib 'SQL-Utils/lib/';
use SQL::Utils;

my ($help, $verbose, $dbfile, $agents);
GetOptions(
	'h|help'		=>	\$help,
	'v|verbose+'	=>	\$verbose,
	'd|dbfile=s'	=>	\$dbfile,
	'a|agents=s'	=>	\$agents,
);

my %create_tables_sql = (
	'macs' => 'CREATE TABLE IF NOT EXISTS macs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_addr TEXT, date_discovered INTEGER, last_updated INTEGER)',
	'ipaddrs' => 'CREATE TABLE IF NOT EXISTS ipaddrs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_id INTEGER, ipaddr TEXT, date_discovered INTEGER, last_updated INTEGER)',
	'agents' => 'CREATE TABLE IF NOT EXISTS agents (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, ipaddr TEXT, fqdn TEXT, first_pull_date INTEGER, last_update INTEGER, iface TEXT)',
	'hosts' => 'CREATE TABLE IF NOT EXISTS hosts (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mac_id INTEGER, ipaddr_id INTEGER, date_discovered INTEGER, last_updated INTEGER)',
	'agents_macs' => 'CREATE TABLE IF NOT EXISTS agents_macs (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, agent_id INTEGER, mac_id INTEGER)',
);

if ((!defined($dbfile)) or ($dbfile eq '')) { die "Need a database file!"; }
if ((!defined($agents)) or ($agents eq '')) { print "No agents file specified.  Using local host.\n"; }

# sqlite3 only does dates as epoch integers or strings,
my $epoch_now = time();
print "Epoch now: $epoch_now \n" if ($verbose);

# create the sql-utils object
my $sqlutils = SQL::Utils->new('sqlite3', { 'db_filename' => $dbfile });

my $sshbin = `which ssh`;
chomp($sshbin);

# sel autofluch to true
local $| = 1;

# create the tables
print "Creating tables if they don't exist in the target database....";
foreach my $t ( keys %create_tables_sql ) {
	$sqlutils->execute_non_query($create_tables_sql{$t});
}
print "done.\n";

# process a list of agents to pull arp data from
if ($agents) {
	print "Processing agents file....\n";
	open AGT, "<$agents" or die "There was a problem reading the agents file: $!";
	while (my $agnt = <AGT>) {
		chomp($agnt);
		next if ($agnt =~ /^#/);		# skip commented lines
		my $rtv = -1;
		my $agent_id = $sqlutils->execute_atomic_int_query("SELECT id FROM agents WHERE ipaddr='$agnt' OR fqdn='$agnt'");
		if ($agent_id) {
			print "Got agent ID: $agent_id\n";
			$rtv = $sqlutils->execute_non_query("UPDATE agents SET last_update='$epoch_now' WHERE id='$agent_id'");
		} else {
			if ($agnt =~ /^(?:\d+\.){3}(\d+)$/) {		# looks like IP address
				my $a = nslookup('host' => $agnt, 'type' => 'A');
				if ((defined($a)) and ($a ne '')) {
					$rtv = $sqlutils->execute_non_query("INSERT INTO agents (ipaddr,fqdn,first_pull_date,last_update) VALUES ('$agnt','$a','$epoch_now','$epoch_now');");
				} else {
					$rtv = $sqlutils->execute_non_query("INSERT INTO agents (ipaddr,first_pull_date,last_update) VALUES ('$agnt','$epoch_now','$epoch_now');");
				}
				print "AGT ip RTV: $rtv\n" if ($verbose);
			} else {
				my $ptr = nslookup('host' => $agnt, 'type' => 'PTR');
				if ((defined($ptr)) and ($ptr ne '')) {
					$rtv = $sqlutils->execute_non_query("INSERT INTO agents (ipaddr,fqdn,first_pull_date,last_update) VALUES ('$ptr','$agnt','$epoch_now','$epoch_now');");
				} else {
					$rtv = $sqlutils->execute_non_query("INSERT INTO agents (fqdn,first_pull_date,last_update) VALUES ('$agnt','$epoch_now','$epoch_now');");
				}
				print "AGT agnt RTV: $rtv\n" if ($verbose);
			}
			$agent_id = $sqlutils->execute_atomic_int_query("SELECT id FROM agents WHERE ipaddr='$agnt' OR fqdn='$agnt';");
		}
		my @files = &get_files($agnt);
		foreach my $f ( sort @files ) {
			my $blob = `$sshbin $agnt 'cat /var/lib/arpwatch/$f'`;
			if ((!defined($blob)) or ($blob eq '')) {
				print "Got no data from $agnt:/var/lib/arpwatch/$f\n" if ($verbose);
			} else {
				my $rtv  = &process_dat($blob, $agent_id);
				print "$agnt: process_dat RTV: $rtv\n";
			}
		}
	}
	close AGT or die "There was a problem closing the agents file: $!";
} else {
	my $blob = &get_data();
	&process_dat($blob);
}

############
### Subs ###
# Summary: Gets the data in the local arpwatch dat file
# Param Name: None
# Returns: the contents of the dat file in string form.
sub get_data {
	local $/ = undef;
	open IN, "</var/lib/arpwatch/arp.dat" or die "Couldn't open arpwatch data file: $!";
	my $blob = <IN>;
	close IN or die "There was a problem closing the arpwatch data file: $!";
	return $blob;
}

# Summary: gets the list of files from an agent
# Param Name: host => The host to grab the list of files from
# Returns: A list of arpwatch dat files from the remote host
sub get_files {
	my $host = shift;
	my $blob = `$sshbin $host 'ls /var/lib/arpwatch'`;
	chomp($blob);
	$blob =~ s/\r?\n/ /g;
	my @list = split(/ /, $blob);
	@list = grep(!/ethercodes\./, @list);
	@list = grep(!/\.new$/, @list);
	my @files = grep(!/\-$/, @list);
	return @files;
}

# Summary: populates the sqlite tables with arp data from files
# Param Name: blob => dat file contents as string
# Returns: None (Void)
sub process_dat {
	my $blob = shift;
	my $agent_id = shift;
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
		print "MACS RTV: $rtv\n" if (($verbose) and ($verbose > 1));
		my $mac_id = $sqlutils->execute_atomic_int_query("SELECT id FROM macs WHERE mac_addr='$mac'");
		$record_id = -1;
		$record_id = $sqlutils->execute_atomic_int_query("SELECT id FROM ipaddrs WHERE ipaddr='$ip';");
		if ($record_id) {
			$rtv = $sqlutils->execute_non_query("UPDATE ipaddrs SET last_updated='$epoch_now' WHERE ipaddr='$ip';");
		} else {
			$rtv = $sqlutils->execute_non_query("INSERT INTO ipaddrs (mac_id, ipaddr, date_discovered, last_updated) VALUES ('$mac_id','$ip','$epoch_now','$epoch_now');");
		}
		print "IPADDRS RTV: $rtv\n" if (($verbose) and ($verbose > 1));
		my $ipaddr_id = $sqlutils->execute_atomic_int_query("SELECT id FROM ipaddrs WHERE ipaddr='$ip';");
		$record_id = -1;
		$record_id = $sqlutils->execute_atomic_int_query("SELECT id FROM hosts WHERE mac_id='$mac_id' AND ipaddr_id='$ipaddr_id';");
		if ($record_id) {
			$rtv = $sqlutils->execute_non_query("UPDATE hosts SET last_updated='$epoch_now' WHERE mac_id='$mac_id' AND ipaddr_id='$ipaddr_id';");
		} else {
			$rtv = $sqlutils->execute_non_query("INSERT INTO hosts (mac_id,ipaddr_id,date_discovered,last_updated) VALUES ('$mac_id','$ipaddr_id','$epoch_now','$epoch_now');");
		}
		print "HOSTS RTV: $rtv\n" if (($verbose) and ($verbose > 1));
		print "DEBUG: SELECT id FROM agents_macs WHERE agent_id='$agent_id' AND mac_id='$mac_id';\n" if (($verbose) and ($verbose > 2));
		$record_id = -1;
		$record_id = $sqlutils->execute_atomic_int_query("SELECT id FROM agents_macs WHERE agent_id='$agent_id' AND mac_id='$mac_id';");
		if ($record_id) {
			print "AGENTS_MACS record exists.  Record_ID = $record_id \n" if (($verbose) and ($verbose > 1)); 
		} else {
			$rtv = $sqlutils->execute_non_query("INSERT INTO agents_macs (agent_id,mac_id) VALUES ('$agent_id', '$mac_id');");
		}
		print "AGENTS_MACS RTV: $rtv\n" if (($verbose) and ($verbose > 1));
	}
}
