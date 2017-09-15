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

given ($report) {
	when ('agent-summary') {
		# get the list of agents in the db
		my $agent_ids = $sqlutils->execute_multi_row_query('SELECT id FROM agents;');
		#print Dumper($agent_ids);
		print scalar(@{$agent_ids})." agents in database.\n";
		foreach my $id ( @{$agent_ids} ) {
			print "Should be a hashref: ".ref($id)."\n";
		}
	}
}
