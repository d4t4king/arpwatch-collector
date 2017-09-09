#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper;

sub process_dat {
	my $blob = shift;
	my @lines = split(/\r?\n/, $blob);
	foreach my $l ( @lines ) {
		my ($mac, $ip, $epoch, $name, $iface) = split(/\t/, $l);
		print "$mac, $ip, $epoch, $name, $iface\n";
	}
}

{
	local $/ = undef;
	open IN, "</var/lib/arpwatch/eth1.dat" or die "Couldn't open arpwatch data file: $!";
	my $blob = <IN>;
	close IN or die "There was a problem closing the arpwatch data file: $!";
	&process_dat($blob);
}

