#!/usr/bin/perl

use strict;
use warnings;
use feature qw( switch );
no if $] ge '5.018', warnings => "experimental::smartmatch";

use Data::Dumper;
use Getopt::Long;

use lib "SQL-Utils/lib";
use SQL::Utils;

my ($help, $verbose, $dbfile, $report);
GetOptions(
	'h|help'		=>	\$help,
	'v|verbose+'	=>	\$verbose,
	'd|dbfile=s'	=>	\$dbfile,
	'r|report=s'	=>	\$report,
);

my $sqlutils = SQL::Utils->new('sqlite3', {db_filename => $dbfile});

my %agents_mac_count;
given ($report) {
	when ('agent-summary') {
		# get the list of agents in the db
		my $agent_ids = $sqlutils->execute_multi_row_query('SELECT id FROM agents;');
		#print Dumper($agent_ids);
		foreach my $id_ref ( @{$agent_ids} ) {
			print "Should be a hash: ".ref($id_ref)."\n" if (($verbose) and ($verbose > 1));
			print "Should be a number: ".$id_ref->{'id'}."\n" if (($verbose) and ($verbose > 1));
			my $count = $sqlutils->execute_atomic_int_query("SELECT COUNT(DISTINCT mac_id) FROM agents_macs WHERE agent_id='$id_ref->{'id'}';");
			print "SELECT ipaddr,fqdn FROM agents WHERE id='$id_ref->{'id'}'\n" if (($verbose) and ($verbose > 1));
			my $agnt = $sqlutils->execute_single_row_query("SELECT ipaddr,fqdn FROM agents WHERE id='$id_ref->{'id'}'");
			print Dumper($agnt) if (($verbose) and ($verbose < 1));
			if ((!defined($agnt->{'fqdn'})) or ($agnt->{'fqdn'} eq '')) {
				$agents_mac_count{$agnt->{'ipaddr'}} = $count;
			} else {
				$agents_mac_count{$agnt->{'fqdn'}} = $count;
			}	
		}
		#print Dumper(\%agents_mac_count);
		print '=' x 72; print "\n";
		printf "| %5d agents in database. %43s|\n", scalar(@{$agent_ids}), " ";
		print '=' x 72; print "\n";
		printf "| %17s | %14s %33s |\n", "Agent IP/FQDN", "MAC Addr Count", " ";
		print '=' x 72; print "\n";
		foreach my $ag ( sort { $agents_mac_count{$b} <=> $agents_mac_count{$a} } keys %agents_mac_count ) {
			printf "| %17s | %5d %43s|\n", $ag, $agents_mac_count{$ag}, " ";
		}
		print '=' x 72; print "\n";
		my $oldest = &get_oldest('agent');
		#print Dumper($oldest);
		printf "| %13s | \n", "Oldest agent: ";
		print '=' x 72; print "\n";
	}
	default {
		warn "Unrecognized report type! ($report)";
	}
}

###############################################################################
### SUBS
###############################################################################
sub usage {
	print <<EOS;

Prints various reports from the collected arpwatch data.

Usage $0 -h -v -d <db file> -r <report>

Where:
-h|--help			Display this useful message then exit.
-v|--verbose		Display output in increasing verbosity.  Primarily used for debugging.
-d|--dbfile			Path to the database to report on.
-r|--report			The type of report to display.  Available options are:
						agent-summary
EOS
	exit 0;
}

sub get_oldest {
	my $what = shift;
	my %oldest;
	my $oldest = 0;
	my $oldest_id = 0;
	given ($what) {
		when ('agent') {
			my $agents = $sqlutils->execute_multi_row_query('SELECT DISTINCT id FROM agents');
			foreach my $agnt ( @{$agents} ) {
				my $dateint = $sqlutils->execute_atomic_int_query("SELECT first_pull_date FROM agents WHERE id='$agnt->{'id'}'");
				if (($oldest == 0) or ($oldest < $dateint)) {
					$oldest = $dateint;
					$oldest_id = $agnt->{'id'};
				}
			}
		}
		default { die "Unrecognized enity ($what)!"; }
	}
	my $old_obj = $sqlutils->execute_single_row_query("SELECT id,ipaddr,fqdn FROM agents WHERE id='$oldest_id'");
	$oldest{$oldest} = $old_obj;
	return \%oldest;
}
